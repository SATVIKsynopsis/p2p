use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct Piece {
    pub index: u32,
    pub data: Vec<u8>,
    pub hash: [u8; 32],
}

pub fn new_piece(index: u32, data: Vec<u8>) -> Piece {

    let mut hasher = Sha256::new();
    hasher.update(&data);

    let hash: [u8; 32] = hasher.finalize().into();

    Piece {
        index,
        data,
        hash,
    }
}

pub fn piece_verify(piece: &Piece) -> bool {

    let mut verify_hash = Sha256::new();
    verify_hash.update(&piece.data);

    let calculated_hash: [u8; 32] = verify_hash.finalize().into();

    piece.hash == calculated_hash
}