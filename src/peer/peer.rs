use crate::peer::connection::PeerConnection;
use crate::piece::PieceManager;
use crate::protocol::Message;
use crate::transfer::Uploader;
use std::collections::HashMap;

pub struct Peer {
    pub peer_id: String,
    pub connections: HashMap<String, PeerConnection>,
    pub uploader: Uploader,
}

impl Peer {
    pub fn new(peer_id: String, piece_manager: PieceManager) -> Peer {
        Peer {
            peer_id,
            connections: HashMap::new(),
            uploader: Uploader::new(piece_manager),
        }
    }

    
    pub fn select_unchoked(&mut self, limit: usize) -> Vec<String> {
        self.select_unchoked_with_threshold(limit, 1)
    }

    pub fn select_unchoked_with_threshold(
        &mut self,
        limit: usize,
        min_downloaded_pieces: u64,
    ) -> Vec<String> {
        let mut ranked: Vec<_> = self
            .connections
            .iter()
            .map(|(id, c)| {
                (
                    id.clone(),
                    c.downloaded_pieces,
                    c.successful_requests,
                    c.uploaded_pieces,
                )
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then(b.2.cmp(&a.2))
                .then(b.3.cmp(&a.3))
                .then(a.0.cmp(&b.0))
        });
        let contributors: Vec<_> = ranked
            .iter()
            .filter(|peer| peer.1 >= min_downloaded_pieces.max(1))
            .collect();
        let selected: std::collections::HashSet<_> = if contributors.is_empty() {
            ranked.into_iter().take(limit).map(|peer| peer.0).collect()
        } else {
            contributors
                .into_iter()
                .take(limit)
                .map(|peer| peer.0.clone())
                .collect()
        };
        for (id, connection) in self.connections.iter_mut() {
            connection.locally_unchoked = selected.contains(id);
        }
        let mut result: Vec<_> = selected.into_iter().collect();
        result.sort();
        result
    }

    
    pub async fn reconsider_unchoked(
        &mut self,
        limit: usize,
    ) -> Result<Vec<String>, std::io::Error> {
        let was: HashMap<_, _> = self
            .connections
            .iter()
            .map(|(id, c)| (id.clone(), c.locally_unchoked))
            .collect();
        let selected = self.select_unchoked(limit);
        for (id, connection) in &mut self.connections {
            if was.get(id).copied().unwrap_or(false) != connection.locally_unchoked {
                if connection.locally_unchoked {
                    connection.send_unchoke().await?;
                } else {
                    connection.send_choke().await?;
                }
            }
        }
        Ok(selected)
    }

    pub async fn announce_have(
        &mut self,
        piece_index: u32,
        piece_manager: &PieceManager,
    ) -> Result<(), std::io::Error> {
        if !piece_manager.has_piece(piece_index) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot announce a piece this peer does not own",
            ));
        }
        for connection in self.connections.values_mut() {
            connection.send_have(piece_index).await?;
        }
        Ok(())
    }

    pub fn add_connection(&mut self, connection: PeerConnection) {
        let peer_id = connection.peer_id.clone();

        self.connections.insert(peer_id, connection);
    }

    pub fn remove_connection(&mut self, peer_id: &str) -> Option<PeerConnection> {
        self.connections.remove(peer_id)
    }

    pub fn get_connection(&self, peer_id: &str) -> Option<&PeerConnection> {
        self.connections.get(peer_id)
    }

    pub async fn send_to_peer(
        &mut self,
        peer_id: &str,
        message: &Message,
    ) -> Result<(), std::io::Error> {
        if let Some(connection) = self.connections.get_mut(peer_id) {
            connection.send_message(message).await
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Peer not found",
            ))
        }
    }

    pub async fn receive_from_peer(&mut self, peer_id: &str) -> Result<Message, std::io::Error> {
        if let Some(connection) = self.connections.get_mut(peer_id) {
            connection.receive_message().await
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Peer not found",
            ))
        }
    }

    pub async fn connect_to_peer(
        &mut self,
        peer_id: String,
        address: &str,
    ) -> Result<(), std::io::Error> {
        let stream = crate::network::tcp_client::connect_to_peer(address).await?;

        let mut connection = PeerConnection::new(peer_id, stream);
        connection.handshake(&self.peer_id).await?;
        connection
            .exchange_bitfield(self.uploader.piece_manager.bitfield())
            .await?;
        connection.receive_choke_state().await?;

        self.add_connection(connection);

        Ok(())
    }

    pub fn disconnect_peer(&mut self, peer_id: &str) -> Option<PeerConnection> {
        self.remove_connection(peer_id)
    }

    pub async fn send_bitfield(
        &mut self,
        peer_id: &str,
        piece_manager: &crate::piece::PieceManager,
    ) -> Result<(), std::io::Error> {
        let bitfield = piece_manager.bitfield();

        let message = Message::Bitfield { bitfield };

        self.send_to_peer(peer_id, &message).await
    }

    pub async fn handle_message(&mut self, peer_id: &str) -> Result<(), std::io::Error> {
        let message = {
            let connection = self.connections.get_mut(peer_id).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "Peer not found")
            })?;

            connection.receive_message().await?
        };

        match message {
            Message::Bitfield { bitfield } => {
                if let Some(connection) = self.connections.get_mut(peer_id) {
                    connection.bitfield = Some(bitfield);
                }
            }

            Message::Have { piece_index } => {
                if piece_index as usize >= self.uploader.piece_manager.total_pieces() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Have piece index out of range",
                    ));
                }
                if let Some(connection) = self.connections.get_mut(peer_id) {
                    if let Some(bits) = &connection.bitfield {
                        if piece_index as usize >= bits.len() * 8 {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "Have piece index out of range",
                            ));
                        }
                    }
                    connection.update_remote_piece(piece_index);
                }
            }

            Message::Choke => {
                if let Some(connection) = self.connections.get_mut(peer_id) {
                    connection.remote_unchoked = false;
                }
            }
            Message::Unchoke => {
                if let Some(connection) = self.connections.get_mut(peer_id) {
                    connection.remote_unchoked = true;
                }
            }

            Message::Ping => {
                if let Some(connection) = self.connections.get_mut(peer_id) {
                    connection.send_message(&Message::Pong).await?;
                }
            }

            Message::Request { piece_index } => {
                if let Some(connection) = self.connections.get_mut(peer_id) {
                    if !connection.locally_unchoked {
                        return Ok(());
                    }
                    self.uploader.serve_request(connection, piece_index).await?;
                    connection.successful_requests += 1;
                }
            }

            Message::Handshake { peer_id: remote_id } => {
                if remote_id.trim().is_empty() || remote_id == self.peer_id {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid handshake peer ID",
                    ));
                }
                if let Some(connection) = self.connections.get_mut(peer_id) {
                    connection.peer_id = remote_id;
                    connection.handshake_complete = true;
                }
            }

            Message::Pong => {}

            _ => {}
        }

        Ok(())
    }

    pub async fn connect_to_discovered_peers(
        &self,
        dht_address: &str,
        piece_index: u32,
    ) -> Result<Vec<PeerConnection>, std::io::Error> {
        let peers = crate::dht::discover_peers_for_piece(dht_address, piece_index).await?;

        let mut connections = Vec::new();

        for (peer_id, address) in peers {
            if peer_id == self.peer_id {
                continue;
            }

            println!(
                "Attempting TCP connection to discovered peer {} at {}",
                peer_id, address
            );

            match crate::network::tcp_client::connect_to_peer(&address).await {
                Ok(stream) => {
                    let mut connection = PeerConnection::new(peer_id, stream);

                    if let Err(error) = connection.handshake(&self.peer_id).await {
                        eprintln!("Failed handshake with {}: {}", connection.peer_id, error);
                        continue;
                    }
                    if let Err(error) = connection
                        .exchange_bitfield(self.uploader.piece_manager.bitfield())
                        .await
                    {
                        eprintln!(
                            "Failed to receive bitfield from {}: {}",
                            connection.peer_id, error
                        );

                        continue;
                    }
                    if let Err(error) = connection.receive_choke_state().await {
                        eprintln!(
                            "Failed to receive uploader state from {}: {}",
                            connection.peer_id, error
                        );
                        continue;
                    }

                    println!(
                        "Discovered peer {} has bitfield: {:?}",
                        connection.peer_id, connection.bitfield
                    );

                    connections.push(connection);
                }

                Err(error) => {
                    eprintln!("Failed to connect to {}: {}", address, error);
                }
            }
        }

        Ok(connections)
    }

    pub async fn discover_connections_for_missing_pieces(
        &self,
        dht_address: &str,
        piece_manager: &PieceManager,
    ) -> Result<Vec<PeerConnection>, std::io::Error> {
        let mut connections = Vec::new();
        let mut discovered_peer_ids = std::collections::HashSet::new();

        for index in 0..piece_manager.total_pieces() {
            let piece_index = index as u32;

            if piece_manager.has_piece(piece_index) {
                continue;
            }

            let peers = crate::dht::discover_peers_for_piece(dht_address, piece_index).await?;

            println!(
                "DHT discovered peers for piece {}: {:?}",
                piece_index, peers
            );

            for (peer_id, address) in peers {
                if peer_id == self.peer_id {
                    continue;
                }

                if !discovered_peer_ids.insert(peer_id.clone()) {
                    continue;
                }

                match crate::network::tcp_client::connect_to_peer(&address).await {
                    Ok(stream) => {
                        println!("Connected to discovered peer {} at {}", peer_id, address);

                        let mut connection = PeerConnection::new(peer_id, stream);

                        if let Err(error) = connection.handshake(&self.peer_id).await {
                            eprintln!("Failed handshake with {}: {}", connection.peer_id, error);
                            continue;
                        }
                        if let Err(error) =
                            connection.exchange_bitfield(piece_manager.bitfield()).await
                        {
                            eprintln!(
                                "Failed to receive bitfield from {}: {}",
                                connection.peer_id, error
                            );
                            continue;
                        }
                        if let Err(error) = connection.receive_choke_state().await {
                            eprintln!(
                                "Failed to receive uploader state from {}: {}",
                                connection.peer_id, error
                            );
                            continue;
                        }

                        println!(
                            "Discovered peer {} has bitfield: {:?}",
                            connection.peer_id, connection.bitfield
                        );

                        connections.push(connection);
                    }

                    Err(error) => {
                        eprintln!("Failed to connect to {}: {}", address, error);
                    }
                }
            }
        }

        Ok(connections)
    }

    pub async fn download_from_dht(
        &self,
        dht_address: &str,
        piece_manager: PieceManager,
    ) -> Result<(), std::io::Error> {
        let connections = self
            .discover_connections_for_missing_pieces(dht_address, &piece_manager)
            .await?;

        println!(
            "DHT downloader discovered {} TCP connections",
            connections.len()
        );

        if connections.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "DHT found no peers for missing pieces",
            ));
        }

        let downloader = std::sync::Arc::new(crate::transfer::Downloader::new(piece_manager));

        downloader.download_concurrently(connections).await
    }
}
