use std::fs;

use p2p::piece::{new_piece, PieceManager};

#[test]
fn test_piece_split_integrity_and_reassembly() {
    let original_data = b"Hello from the P2P file sharing network!";

    let piece_size = 10;

    // Split the original data into pieces.
    let pieces: Vec<_> = original_data
        .chunks(piece_size)
        .enumerate()
        .map(|(index, data)| new_piece(index as u32, data.to_vec()))
        .collect();

    // Create an empty manager representing the downloader.
    let mut manager =
        PieceManager::new_empty(pieces.len(), piece_size)
            .expect("Failed to create PieceManager");

    // Initially no pieces should exist.
    for index in 0..pieces.len() {
        assert!(!manager.has_piece(index as u32));
    }

    // Add every piece.
    for piece in pieces {
        manager
            .add_piece(piece)
            .expect("Failed to add piece");
    }

    // Every piece should now exist.
    for index in 0..manager.total_pieces() {
        assert!(manager.has_piece(index as u32));
    }

    // Reassemble the file.
    let output_path = "test_output.txt";

    manager
        .reassemble(output_path)
        .expect("Failed to reassemble file");

    // Read reconstructed file.
    let reconstructed_data =
        fs::read(output_path).expect("Failed to read reconstructed file");

    assert_eq!(reconstructed_data, original_data);

    // Cleanup.
    fs::remove_file(output_path)
        .expect("Failed to remove test output");
}