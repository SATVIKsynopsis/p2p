use crate::peer::PeerConnection;
use crate::piece::{Piece, PieceManager};
use crate::protocol::Message;
use std::collections::HashSet;

use std::sync::Arc;
use tokio::sync::{Mutex, watch};

pub struct Downloader {
    pub piece_manager: Arc<Mutex<PieceManager>>,
    pub downloading: Arc<Mutex<HashSet<u32>>>,
    completion: watch::Sender<bool>,
}

impl Downloader {
    pub fn new(piece_manager: PieceManager) -> Downloader {
        let (completion, _) = watch::channel(false);
        Downloader {
            piece_manager: Arc::new(Mutex::new(piece_manager)),
            downloading: Arc::new(Mutex::new(HashSet::new())),
            completion,
        }
    }

    pub async fn request_piece(
        &self,
        connection: &mut PeerConnection,
        piece_index: u32,
    ) -> Result<(), std::io::Error> {
        if !connection.remote_unchoked {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Remote peer is choking requests",
            ));
        }
        let message = Message::Request { piece_index };

        connection.send_message(&message).await
    }

    pub async fn receive_piece(
        &self,
        piece_index: u32,
        data: Vec<u8>,
        hash: [u8; 32],
    ) -> Result<(), std::io::Error> {
        if !crate::integrity::verify_hash(&data, &hash) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Piece {} failed integrity verification", piece_index),
            ));
        }

        let piece = Piece {
            index: piece_index,
            data,
            hash,
        };

        let mut piece_manager = self.piece_manager.lock().await;

        piece_manager.add_piece(piece)?;
        Ok(())
    }

    async fn update_remote_have(
        &self,
        connection: &mut PeerConnection,
        piece_index: u32,
    ) -> Result<(), std::io::Error> {
        let total = self.piece_manager.lock().await.total_pieces();
        if piece_index as usize >= total {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Have piece index out of range",
            ));
        }
        connection.update_remote_piece(piece_index);
        Ok(())
    }

    pub async fn download_from_peer(
        &self,
        connection: &mut PeerConnection,
        requested_piece_index: u32,
    ) -> Result<(), std::io::Error> {
        println!(
            "Downloader requesting piece {} from peer {}",
            requested_piece_index, connection.peer_id
        );

        self.request_piece(connection, requested_piece_index)
            .await?;

        loop {
            match connection.receive_message().await? {
                Message::Piece {
                    piece_index,
                    data,
                    hash,
                } => {
                    if piece_index != requested_piece_index {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Received a different piece than requested",
                        ));
                    }
                    println!(
                        "Downloader received piece {} from peer {}",
                        piece_index, connection.peer_id
                    );

                    self.receive_piece(piece_index, data, hash).await?;
                    connection.send_have(piece_index).await?;
                    connection.downloaded_pieces += 1;
                    connection.successful_requests += 1;
                    if self.is_complete().await {
                        self.completion.send_replace(true);
                    }

                    return Ok(());
                }
                Message::Have { piece_index } => {
                    self.update_remote_have(connection, piece_index).await?
                }
                Message::Choke => {
                    connection.remote_unchoked = false;
                    let total = self.piece_manager.lock().await.total_pieces();
                    connection.wait_until_unchoked(total).await?;
                    self.request_piece(connection, requested_piece_index)
                        .await?;
                }
                Message::Unchoke => {
                    connection.remote_unchoked = true;
                    self.request_piece(connection, requested_piece_index)
                        .await?;
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Expected Piece, Have, Choke, or Unchoke message",
                    ));
                }
            }
        }
    }

    pub async fn download_next_piece(
        &self,
        connection: &mut PeerConnection,
    ) -> Result<bool, std::io::Error> {
        let piece_index = {
            let piece_manager = self.piece_manager.lock().await;
            let mut downloading = self.downloading.lock().await;

            let mut selected_piece = None;

            for index in 0..piece_manager.total_pieces() {
                let piece_index = index as u32;

                if !piece_manager.has_piece(piece_index) && !downloading.contains(&piece_index) {
                    println!(
                        "Piece {} available locally? {} | downloading? {} | remote has piece? {} | peer={}",
                        piece_index,
                        piece_manager.has_piece(piece_index),
                        downloading.contains(&piece_index),
                        connection.has_remote_piece(piece_index),
                        connection.peer_id
                    );

                    if connection.has_remote_piece(piece_index) {
                        selected_piece = Some(piece_index);
                        break;
                    }
                }
            }

            match selected_piece {
                Some(index) => {
                    downloading.insert(index);
                    println!(
                        "Downloader selected piece {} from peer {}",
                        index, connection.peer_id
                    );
                    index
                }

                None => return Ok(false),
            }
        };

        let result = self.download_from_peer(connection, piece_index).await;

        {
            let mut downloading = self.downloading.lock().await;
            downloading.remove(&piece_index);
        }

        result.map(|_| true)
    }

    pub async fn download_from_peers(
        &self,
        connections: &mut [PeerConnection],
    ) -> Result<(), std::io::Error> {
        loop {
            let mut downloaded = false;

            for connection in connections.iter_mut() {
                if self.download_next_piece(connection).await? {
                    downloaded = true;
                }
            }

            if !downloaded {
                break;
            }
        }

        Ok(())
    }

    pub async fn download_from_connection(
        &self,
        mut connection: PeerConnection,
    ) -> Result<(), std::io::Error> {
        loop {
            let downloaded = self.download_next_piece(&mut connection).await?;

            if !downloaded {
                break;
            }
        }

        Ok(())
    }

    pub async fn download_concurrently(
        self: std::sync::Arc<Self>,
        connections: Vec<PeerConnection>,
    ) -> Result<(), std::io::Error> {
        let mut completed = self.completion.subscribe();
        let mut tasks = tokio::task::JoinSet::new();

        for connection in connections {
            let downloader = std::sync::Arc::clone(&self);

            tasks.spawn(async move {
                let mut connection = connection;

                loop {
                    if downloader.is_complete().await {
                        break;
                    }

                    if !connection.remote_unchoked {
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(30),
                            connection.receive_message(),
                        )
                        .await
                        {
                            Ok(Ok(Message::Unchoke)) => connection.remote_unchoked = true,
                            Ok(Ok(Message::Choke)) => connection.remote_unchoked = false,
                            Ok(Ok(Message::Have { piece_index })) => {
                                if downloader
                                    .update_remote_have(&mut connection, piece_index)
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Ok(Ok(Message::Ping)) => {
                                if connection.send_message(&Message::Pong).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => break,
                        }
                        continue;
                    }

                    match downloader.try_download_from_peer(&mut connection).await {
                        Ok(true) => {}

                        Ok(false) => {
                            if downloader.is_complete().await {
                                break;
                            }
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(5),
                                connection.receive_message(),
                            )
                            .await
                            {
                                Ok(Ok(Message::Have { piece_index })) => {
                                    if downloader
                                        .update_remote_have(&mut connection, piece_index)
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                Ok(Ok(Message::Choke)) => connection.remote_unchoked = false,
                                Ok(Ok(Message::Unchoke)) => connection.remote_unchoked = true,
                                Ok(Ok(Message::Ping)) => {
                                    if connection.send_message(&Message::Pong).await.is_err() {
                                        break;
                                    }
                                }
                                Ok(Ok(_)) => break,
                                Ok(Err(_)) | Err(_) => break,
                            }
                        }

                        Err(error) => {
                            eprintln!("Peer {} stopped: {}", connection.peer_id, error);
                            break;
                        }
                    }
                }
            });
        }

        loop {
            if *completed.borrow() {
                tasks.abort_all();
                break;
            }
            if tasks.is_empty() {
                break;
            }
            tokio::select! {
                joined = tasks.join_next() => {
                    if let Some(Err(error)) = joined {
                        if !error.is_cancelled() {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Download task failed: {error}")));
                        }
                    }
                }
                changed = completed.changed() => {
                    if changed.is_err() { break; }
                }
            }
        }
        while tasks.join_next().await.is_some() {}

        if !self.is_complete().await {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Download incomplete: some pieces are still missing",
            ));
        }

        Ok(())
    }

    pub async fn try_download_from_peer(
        &self,
        connection: &mut PeerConnection,
    ) -> Result<bool, std::io::Error> {
        match self.download_next_piece(connection).await {
            Ok(downloaded) => Ok(downloaded),

            Err(error) => {
                eprintln!(
                    "Peer {} failed during download: {}",
                    connection.peer_id, error
                );

                Ok(false)
            }
        }
    }

    pub async fn is_complete(&self) -> bool {
        let piece_manager = self.piece_manager.lock().await;

        for index in 0..piece_manager.total_pieces() {
            if !piece_manager.has_piece(index as u32) {
                return false;
            }
        }

        true
    }

    pub async fn reassemble(&self, output_path: &str) -> Result<(), std::io::Error> {
        let piece_manager = self.piece_manager.lock().await;

        piece_manager.reassemble(output_path)
    }
}
