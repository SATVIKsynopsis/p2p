use crate::integrity::hash::verify_hash;
use crate::piece::{Piece, new_piece};
use std::io::Write;

pub struct PieceManager {
    pub pieces: Vec<Option<Piece>>,
    pub piece_size: usize,
}

impl PieceManager {
    pub fn try_from_file(path: &str, piece_size: usize) -> Result<PieceManager, std::io::Error> {
        if piece_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Piece size must be greater than zero",
            ));
        }

        let read_file = std::fs::read(path)?;

        if read_file.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "File is empty",
            ));
        }

        let mut pieces = Vec::new();

        for (index, data) in read_file.chunks(piece_size).enumerate() {
            let piece = new_piece(index as u32, data.to_vec());
            pieces.push(Some(piece));
        }

        Ok(PieceManager { pieces, piece_size })
    }

    pub fn new_empty(
        total_pieces: usize,
        piece_size: usize,
    ) -> Result<PieceManager, std::io::Error> {
        if piece_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Piece size must be greater than zero",
            ));
        }

        if total_pieces == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Total pieces must be greater than zero",
            ));
        }

        let mut pieces = Vec::with_capacity(total_pieces);

        for _ in 0..total_pieces {
            pieces.push(None);
        }

        Ok(PieceManager { pieces, piece_size })
    }

    pub fn get_piece(&self, index: u32) -> Option<&Piece> {
        self.pieces
            .get(index as usize)
            .and_then(|piece| piece.as_ref())
    }

    pub fn has_piece(&self, index: u32) -> bool {
        self.get_piece(index).is_some()
    }

    pub fn add_piece(&mut self, piece: Piece) -> Result<(), std::io::Error> {
        let index = piece.index as usize;

        if index >= self.pieces.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Piece index {} is out of range", piece.index),
            ));
        }

        if self.pieces[index].is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Piece {} already exists", piece.index),
            ));
        }

        if !verify_hash(&piece.data, &piece.hash) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Piece {} failed integrity verification", piece.index),
            ));
        }

        self.pieces[index] = Some(piece);

        Ok(())
    }

    pub fn total_pieces(&self) -> usize {
        self.pieces.len()
    }

    pub fn reassemble(&self, output_path: &str) -> Result<(), std::io::Error> {
        let mut output_file = std::fs::File::create(output_path)?;

        for (index, piece) in self.pieces.iter().enumerate() {
            let piece = piece.as_ref().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Piece {} is missing", index),
                )
            })?;

            output_file.write_all(&piece.data)?;
        }

        Ok(())
    }

    pub fn bitfield(&self) -> Vec<u8> {
        let total_pieces = self.pieces.len();
        let byte_count = (total_pieces + 7) / 8;

        let mut bitfield = vec![0u8; byte_count];

        for index in 0..total_pieces {
            if self.pieces[index].is_some() {
                let byte_index = index / 8;
                let bit_index = index % 8;

                bitfield[byte_index] |= 1 << bit_index;
            }
        }

        bitfield
    }

    pub fn has_piece_from_bitfield(bitfield: &[u8], piece_index: u32) -> bool {
        let index = piece_index as usize;

        let byte_index = index / 8;
        let bit_index = index % 8;

        if byte_index >= bitfield.len() {
            return false;
        }

        (bitfield[byte_index] & (1 << bit_index)) != 0
    }
}
