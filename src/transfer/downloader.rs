use crate::peer::PeerConnection;
use crate::piece::{Piece, PieceManager};
use crate::protocol::Message;
use std::collections::HashSet;

use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Downloader {
    pub piece_manager: Arc<Mutex<PieceManager>>,
    pub downloading: Arc<Mutex<HashSet<u32>>>,
}

impl Downloader {
    pub fn new(piece_manager: PieceManager) -> Downloader {
    Downloader {
        piece_manager: Arc::new(Mutex::new(piece_manager)),
        downloading: Arc::new(Mutex::new(HashSet::new())),
    }
}

    pub async fn request_piece(
        &self,
        connection: &mut PeerConnection,
        piece_index: u32,
    ) -> Result<(), std::io::Error> {
        let message = Message::Request {
            piece_index,
        };

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

        piece_manager.add_piece(piece)
    }

    pub async fn download_from_peer(
        &self,
        connection: &mut PeerConnection,
        piece_index: u32,
    ) -> Result<(), std::io::Error> {
        self.request_piece(connection, piece_index).await?;

        let message = connection.receive_message().await?;

        match message {
            Message::Piece {
                piece_index,
                data,
                hash,
            } => {
                self.receive_piece(piece_index, data, hash).await?;
                connection.send_have(piece_index).await?;
                Ok(())
            }

            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Expected Piece message",
            )),
        }
    }

    pub async fn download_next_piece(
    &self,
    connection: &mut PeerConnection,
) -> Result<bool, std::io::Error> {
    let piece_index = {
        let piece_manager = self.piece_manager.lock().await;
        let downloading = self.downloading.lock().await;

        let mut selected_piece = None;

        for index in 0..piece_manager.total_pieces() {
            let piece_index = index as u32;

            if !piece_manager.has_piece(piece_index)
                && !downloading.contains(&piece_index)
                && connection.has_remote_piece(piece_index)
            {
                selected_piece = Some(piece_index);
                break;
            }
        }

        match selected_piece {
            Some(index) => index,
            None => return Ok(false),
        }
    };

    {
        let mut downloading = self.downloading.lock().await;
        downloading.insert(piece_index);
    }

    let result = self
        .download_from_peer(connection, piece_index)
        .await;

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
    let mut tasks = Vec::new();

    for connection in connections {
        let downloader = std::sync::Arc::clone(&self);

        let task = tokio::spawn(async move {
            let mut connection = connection;

            loop {
                if downloader.is_complete().await {
                    break;
                }

                match downloader
                    .try_download_from_peer(&mut connection)
                    .await
                {
                    Ok(true) => {}

                    Ok(false) => {
                        break;
                    }

                    Err(error) => {
                        eprintln!(
                            "Peer {} stopped: {}",
                            connection.peer_id,
                            error
                        );
                        break;
                    }
                }
            }
        });

        tasks.push(task);
    }

    for task in tasks {
        task.await.map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Download task failed: {}", error),
            )
        })?;
    }

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
                connection.peer_id,
                error
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

pub async fn reassemble(
    &self,
    output_path: &str,
) -> Result<(), std::io::Error> {
    let piece_manager = self.piece_manager.lock().await;

    piece_manager.reassemble(output_path)
}

}