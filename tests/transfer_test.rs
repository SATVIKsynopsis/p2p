use std::fs;

use p2p::piece::{PieceManager, new_piece};

#[test]
fn test_piece_split_integrity_and_reassembly() {
    let original_data = b"Hello from the P2P file sharing network!";

    let piece_size = 10;

    let pieces: Vec<_> = original_data
        .chunks(piece_size)
        .enumerate()
        .map(|(index, data)| new_piece(index as u32, data.to_vec()))
        .collect();

    let mut manager =
        PieceManager::new_empty(pieces.len(), piece_size).expect("Failed to create PieceManager");

    for index in 0..pieces.len() {
        assert!(!manager.has_piece(index as u32));
    }

    for piece in pieces {
        manager.add_piece(piece).expect("Failed to add piece");
    }

    for index in 0..manager.total_pieces() {
        assert!(manager.has_piece(index as u32));
    }

    let output_path = "test_output.txt";

    manager
        .reassemble(output_path)
        .expect("Failed to reassemble file");

    let reconstructed_data = fs::read(output_path).expect("Failed to read reconstructed file");

    assert_eq!(reconstructed_data, original_data);

    fs::remove_file(output_path).expect("Failed to remove test output");
}
