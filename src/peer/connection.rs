use crate::piece::PieceManager;

pub struct PeerConnection {
    pub stream: tokio::net::TcpStream,
    pub peer_id: String,
    pub bitfield: Option<Vec<u8>>,
    pub handshake_complete: bool,
    
    pub remote_unchoked: bool,
    
    pub uploaded_pieces: u64,
    pub downloaded_pieces: u64,
    pub successful_requests: u64,
}

impl PeerConnection {
    pub fn new(peer_id: String, stream: tokio::net::TcpStream) -> PeerConnection {
        PeerConnection {
            peer_id,
            stream,
            bitfield: None,
            handshake_complete: false,
            remote_unchoked: true,
            locally_unchoked: true,
            uploaded_pieces: 0,
            downloaded_pieces: 0,
            successful_requests: 0,
        }
    }

    
    pub async fn handshake(&mut self, local_peer_id: &str) -> Result<(), std::io::Error> {
        if local_peer_id.trim().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Local peer ID cannot be empty",
            ));
        }
        self.send_message(&crate::protocol::Message::Handshake {
            peer_id: local_peer_id.to_owned(),
        })
        .await?;
        match self.receive_message().await? {
            crate::protocol::Message::Handshake { peer_id }
                if !peer_id.trim().is_empty() && peer_id != local_peer_id =>
            {
                self.peer_id = peer_id;
                self.handshake_complete = true;
                Ok(())
            }
            crate::protocol::Message::Handshake { .. } => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid or self peer ID in handshake",
            )),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Expected Handshake message",
            )),
        }
    }

    pub async fn exchange_bitfield(
        &mut self,
        local_bitfield: Vec<u8>,
    ) -> Result<(), std::io::Error> {
        if !self.handshake_complete {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Handshake must complete before bitfield exchange",
            ));
        }
        self.send_message(&crate::protocol::Message::Bitfield {
            bitfield: local_bitfield,
        })
        .await?;
        self.receive_bitfield().await
    }

    /// Reads the initial uploader decision sent after bitfield exchange.
    pub async fn receive_choke_state(&mut self) -> Result<(), std::io::Error> {
        match self.receive_message().await? {
            crate::protocol::Message::Choke => {
                self.remote_unchoked = false;
                Ok(())
            }
            crate::protocol::Message::Unchoke => {
                self.remote_unchoked = true;
                Ok(())
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Expected initial Choke or Unchoke",
            )),
        }
    }

    pub async fn wait_until_unchoked(&mut self, total_pieces: usize) -> Result<(), std::io::Error> {
        while !self.remote_unchoked {
            match self.receive_message().await? {
                crate::protocol::Message::Unchoke => self.remote_unchoked = true,
                crate::protocol::Message::Choke => self.remote_unchoked = false,
                crate::protocol::Message::Have { piece_index } => {
                    if piece_index as usize >= total_pieces {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Have piece index out of range",
                        ));
                    }
                    self.update_remote_piece(piece_index)
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Unexpected message while waiting for Unchoke",
                    ));
                }
            }
        }
        Ok(())
    }

    pub async fn send_choke(&mut self) -> Result<(), std::io::Error> {
        self.locally_unchoked = false;
        self.send_message(&crate::protocol::Message::Choke).await
    }

    pub async fn send_unchoke(&mut self) -> Result<(), std::io::Error> {
        self.locally_unchoked = true;
        self.send_message(&crate::protocol::Message::Unchoke).await
    }

    pub fn request_allowed(&self) -> Result<(), std::io::Error> {
        if self.remote_unchoked {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Remote peer is choking requests",
            ))
        }
    }

    pub async fn send_message(
        &mut self,
        message: &crate::protocol::Message,
    ) -> Result<(), std::io::Error> {
        crate::protocol::codec::write_message(&mut self.stream, message).await
    }

    pub async fn receive_message(&mut self) -> Result<crate::protocol::Message, std::io::Error> {
        crate::protocol::codec::read_message(&mut self.stream).await
    }

    pub async fn receive_bitfield(&mut self) -> Result<(), std::io::Error> {
        let message = self.receive_message().await?;

        match message {
            crate::protocol::Message::Bitfield { bitfield } => {
                self.bitfield = Some(bitfield);
                Ok(())
            }

            other => {
                println!(
                    "Expected Bitfield from {}, received: {:?}",
                    self.peer_id, other
                );

                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected Bitfield message",
                ))
            }
        }
    }

    pub fn has_remote_piece(&self, piece_index: u32) -> bool {
        if let Some(bitfield) = &self.bitfield {
            crate::piece::PieceManager::has_piece_from_bitfield(bitfield, piece_index)
        } else {
            false
        }
    }

    pub fn next_available_piece(&self, piece_manager: &PieceManager) -> Option<u32> {
        for index in 0..piece_manager.total_pieces() {
            let piece_index = index as u32;

            if !piece_manager.has_piece(piece_index) && self.has_remote_piece(piece_index) {
                return Some(piece_index);
            }
        }

        None
    }

    pub async fn send_have(&mut self, piece_index: u32) -> Result<(), std::io::Error> {
        let message = crate::protocol::Message::Have { piece_index };

        self.send_message(&message).await
    }

    pub async fn receive_have(&mut self) -> Result<(), std::io::Error> {
        let message = self.receive_message().await?;

        match message {
            crate::protocol::Message::Have { piece_index } => {
                if let Some(bits) = &self.bitfield {
                    if piece_index as usize >= bits.len() * 8 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Have piece index is outside known bitfield",
                        ));
                    }
                }
                let byte_index = piece_index as usize / 8;
                let bit_index = piece_index as usize % 8;

                let bitfield = self.bitfield.get_or_insert_with(Vec::new);

                if bitfield.len() <= byte_index {
                    bitfield.resize(byte_index + 1, 0);
                }

                bitfield[byte_index] |= 1 << bit_index;

                Ok(())
            }

            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Expected Have message",
            )),
        }
    }

    pub fn update_remote_piece(&mut self, piece_index: u32) {
        let byte_index = piece_index as usize / 8;
        let bit_index = piece_index as usize % 8;

        let bitfield = self.bitfield.get_or_insert_with(Vec::new);

        if bitfield.len() <= byte_index {
            bitfield.resize(byte_index + 1, 0);
        }

        bitfield[byte_index] |= 1 << bit_index;
    }
}
