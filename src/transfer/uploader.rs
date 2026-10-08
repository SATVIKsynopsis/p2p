use crate::integrity::hash::verify_hash;
use crate::peer::PeerConnection;
use crate::piece::PieceManager;
use crate::protocol::Message;

pub struct Uploader {
    pub piece_manager: PieceManager,
}

impl Uploader {
    pub fn new(piece_manager: PieceManager) -> Uploader {
        Uploader { piece_manager }
    }

    pub async fn serve_request(
        &self,
        connection: &mut PeerConnection,
        piece_index: u32,
    ) -> Result<(), std::io::Error> {
        if let Some(piece) = self.piece_manager.get_piece(piece_index) {
            let message = Message::Piece {
                piece_index: piece.index,
                data: piece.data.clone(),
                hash: piece.hash,
            };

            connection.send_message(&message).await?;
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Piece {} not found", piece_index),
            ))
        }
    }

    pub async fn handle_request(
        &self,
        connection: &mut PeerConnection,
    ) -> Result<(), std::io::Error> {
        let message = connection.receive_message().await?;

        match message {
            Message::Request { piece_index } => self.serve_request(connection, piece_index).await,

            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Expected Request message",
            )),
        }
    }
}
