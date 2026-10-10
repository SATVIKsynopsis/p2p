use std::fs;
use std::sync::Arc;

use p2p::peer::PeerConnection;
use p2p::piece::{PieceManager, new_piece};
use p2p::transfer::{Downloader, Uploader};
use tokio::net::TcpListener;

async fn start_seeder(piece_manager: PieceManager) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind seeder");

    let address = listener
        .local_addr()
        .expect("Failed to get seeder address")
        .to_string();

    let handle = tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .expect("Failed to accept connection");

        let mut connection = PeerConnection::new("seeder".to_string(), stream);

        let uploader = Uploader::new(piece_manager);

        loop {
            match connection.receive_message().await {
                Ok(p2p::protocol::Message::Request { piece_index }) => {
                    uploader
                        .serve_request(&mut connection, piece_index)
                        .await
                        .expect("Seeder failed to serve request");
                }

                Ok(_) => {}

                Err(_) => {
                    break;
                }
            }
        }
    });

    (address, handle)
}

fn create_piece_manager(
    pieces: &[p2p::piece::Piece],
    indexes: &[usize],
    total_pieces: usize,
    piece_size: usize,
) -> PieceManager {
    let mut manager =
        PieceManager::new_empty(total_pieces, piece_size).expect("Failed to create PieceManager");

    for &index in indexes {
        manager
            .add_piece(pieces[index].clone())
            .expect("Failed to add piece");
    }

    manager
}

#[tokio::test]
async fn test_three_peer_concurrent_download() {
    let original_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    let piece_size = 10;

    let pieces: Vec<_> = original_data
        .chunks(piece_size)
        .enumerate()
        .map(|(index, data)| new_piece(index as u32, data.to_vec()))
        .collect();

    let total_pieces = pieces.len();

    assert_eq!(total_pieces, 7);

    

    let peer_a_manager = create_piece_manager(&pieces, &[0, 1], total_pieces, piece_size);

    let peer_b_manager = create_piece_manager(&pieces, &[2, 3], total_pieces, piece_size);

    let peer_c_manager = create_piece_manager(&pieces, &[4, 5, 6], total_pieces, piece_size);

    let peer_a_bitfield = peer_a_manager.bitfield();
    let peer_b_bitfield = peer_b_manager.bitfield();
    let peer_c_bitfield = peer_c_manager.bitfield();

    let (address_a, task_a) = start_seeder(peer_a_manager).await;

    let (address_b, task_b) = start_seeder(peer_b_manager).await;

    let (address_c, task_c) = start_seeder(peer_c_manager).await;

    let stream_a = tokio::net::TcpStream::connect(&address_a)
        .await
        .expect("Failed to connect to Peer A");

    let stream_b = tokio::net::TcpStream::connect(&address_b)
        .await
        .expect("Failed to connect to Peer B");

    let stream_c = tokio::net::TcpStream::connect(&address_c)
        .await
        .expect("Failed to connect to Peer C");

    let mut connection_a = PeerConnection::new("peer_a".to_string(), stream_a);

    let mut connection_b = PeerConnection::new("peer_b".to_string(), stream_b);

    let mut connection_c = PeerConnection::new("peer_c".to_string(), stream_c);

    connection_a.bitfield = Some(peer_a_bitfield);
    connection_b.bitfield = Some(peer_b_bitfield);
    connection_c.bitfield = Some(peer_c_bitfield);

    let downloader_manager = PieceManager::new_empty(total_pieces, piece_size)
        .expect("Failed to create downloader PieceManager");

    let downloader = Arc::new(Downloader::new(downloader_manager));

    downloader
        .clone()
        .download_concurrently(vec![connection_a, connection_b, connection_c])
        .await
        .expect("Concurrent download failed");

    assert!(
        downloader.is_complete().await,
        "Downloader should have all pieces"
    );

    let output_path = "test_multi_peer_output.txt";

    downloader
        .reassemble(output_path)
        .await
        .expect("Failed to reassemble downloaded file");

    let reconstructed = fs::read(output_path).expect("Failed to read reconstructed file");

    assert_eq!(
        reconstructed, original_data,
        "Reconstructed file differs from original"
    );

    fs::remove_file(output_path).expect("Failed to remove test output");

    task_a.await.expect("Peer A task failed");

    task_b.await.expect("Peer B task failed");

    task_c.await.expect("Peer C task failed");
}
