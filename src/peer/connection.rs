use crate::piece::PieceManager;

pub struct PeerConnection {
    pub stream: tokio::net::TcpStream,
    pub peer_id: String,
    pub bitfield: Option<Vec<u8>>,
}

impl PeerConnection {
    pub fn new(peer_id: String, stream: tokio::net::TcpStream) -> PeerConnection {
        PeerConnection {
            peer_id,
            stream,
            bitfield: None,
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
