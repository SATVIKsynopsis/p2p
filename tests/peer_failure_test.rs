use std::fs;
use std::sync::Arc;

use p2p::peer::PeerConnection;
use p2p::piece::{PieceManager, new_piece};
use p2p::protocol::Message;
use p2p::transfer::{Downloader, Uploader};
use tokio::net::TcpListener;

async fn start_seeder(
    peer_id: &'static str,
    piece_manager: PieceManager,
    request_count: usize,
    drop_connection: bool,
) -> (String, tokio::task::JoinHandle<()>) {
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

        let mut connection = PeerConnection::new(peer_id.to_string(), stream);

        let uploader = Uploader::new(piece_manager);

        let mut served = 0;

        while served < request_count {
            match connection.receive_message().await {
                Ok(Message::Request { piece_index }) => {
                    if drop_connection {
                        return;
                    }

                    uploader
                        .serve_request(&mut connection, piece_index)
                        .await
                        .expect("Seeder failed to serve request");

                    served += 1;
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

fn create_manager(
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
async fn test_peer_dropout_recovery() {
    let original_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    let piece_size = 10;

    let pieces: Vec<_> = original_data
        .chunks(piece_size)
        .enumerate()
        .map(|(index, data)| new_piece(index as u32, data.to_vec()))
        .collect();

    let total_pieces = pieces.len();

    

    let failing_manager = create_manager(&pieces, &[0, 1], total_pieces, piece_size);

    let healthy_manager = create_manager(&pieces, &[0, 1, 2, 3, 4, 5, 6], total_pieces, piece_size);

    let (failing_address, failing_task) =
        start_seeder("failing_peer", failing_manager, 1, true).await;

    let (healthy_address, healthy_task) =
        start_seeder("healthy_peer", healthy_manager, total_pieces, false).await;

    let failing_stream = tokio::net::TcpStream::connect(&failing_address)
        .await
        .expect("Failed to connect to failing peer");

    let healthy_stream = tokio::net::TcpStream::connect(&healthy_address)
        .await
        .expect("Failed to connect to healthy peer");

    let mut failing_connection = PeerConnection::new("failing_peer".to_string(), failing_stream);

    let mut healthy_connection = PeerConnection::new("healthy_peer".to_string(), healthy_stream);

    failing_connection.bitfield = Some(failing_connection_bitfield(total_pieces, &[0, 1]));

    healthy_connection.bitfield = Some(failing_connection_bitfield(
        total_pieces,
        &[0, 1, 2, 3, 4, 5, 6],
    ));

    let downloader_manager = PieceManager::new_empty(total_pieces, piece_size)
        .expect("Failed to create downloader manager");

    let downloader = Arc::new(Downloader::new(downloader_manager));

    

    let _ = downloader
        .try_download_from_peer(&mut failing_connection)
        .await;

    

    loop {
        if downloader.is_complete().await {
            break;
        }

        let downloaded = downloader
            .try_download_from_peer(&mut healthy_connection)
            .await
            .expect("Healthy peer failed");

        if !downloaded {
            panic!(
                "Healthy peer had no more pieces, \
                 but download was incomplete"
            );
        }
    }

    assert!(
        downloader.is_complete().await,
        "Download should complete after peer failure"
    );

    let output_path = "test_peer_failure_output.txt";

    downloader
        .reassemble(output_path)
        .await
        .expect("Failed to reassemble file");

    let reconstructed = fs::read(output_path).expect("Failed to read reconstructed file");

    assert_eq!(
        reconstructed, original_data,
        "Recovered file differs from original"
    );

    fs::remove_file(output_path).expect("Failed to remove test output");

    failing_task.await.expect("Failing peer task failed");

    healthy_task.await.expect("Healthy peer task failed");
}

#[tokio::test]
async fn test_three_peer_dropout_recovery_during_concurrent_transfer() {
    let original =
        b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQR";
    let piece_size = 10;
    let pieces: Vec<_> = original
        .chunks(piece_size)
        .enumerate()
        .map(|(index, data)| new_piece(index as u32, data.to_vec()))
        .collect();
    let total = pieces.len();
    assert_eq!(total, 8);

    let (a_addr, a_task) = start_seeder(
        "drop_a",
        create_manager(&pieces, &[0, 1, 2], total, piece_size),
        1,
        true,
    )
    .await;
    let (b_addr, b_task) = start_seeder(
        "peer_b",
        create_manager(&pieces, &[0, 1, 2, 3, 4, 5], total, piece_size),
        6,
        false,
    )
    .await;
    let (c_addr, c_task) = start_seeder(
        "peer_c",
        create_manager(&pieces, &[6, 7], total, piece_size),
        2,
        false,
    )
    .await;

    let mut a = PeerConnection::new(
        "drop_a".into(),
        tokio::net::TcpStream::connect(a_addr).await.unwrap(),
    );
    let mut b = PeerConnection::new(
        "peer_b".into(),
        tokio::net::TcpStream::connect(b_addr).await.unwrap(),
    );
    let mut c = PeerConnection::new(
        "peer_c".into(),
        tokio::net::TcpStream::connect(c_addr).await.unwrap(),
    );
    a.bitfield = Some(failing_connection_bitfield(total, &[0, 1, 2]));
    b.bitfield = Some(failing_connection_bitfield(total, &[0, 1, 2, 3, 4, 5]));
    c.bitfield = Some(failing_connection_bitfield(total, &[6, 7]));

    let downloader = Arc::new(Downloader::new(
        PieceManager::new_empty(total, piece_size).unwrap(),
    ));
    downloader
        .clone()
        .download_concurrently(vec![a, b, c])
        .await
        .unwrap();
    let output_path = std::env::temp_dir().join(format!("p2p_failure_{}.bin", std::process::id()));
    downloader
        .reassemble(output_path.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(fs::read(&output_path).unwrap(), original);
    fs::remove_file(output_path).unwrap();
    a_task.await.unwrap();
    b_task.await.unwrap();
    c_task.await.unwrap();
}

fn failing_connection_bitfield(total_pieces: usize, indexes: &[usize]) -> Vec<u8> {
    let byte_count = (total_pieces + 7) / 8;

    let mut bitfield = vec![0u8; byte_count];

    for &index in indexes {
        let byte_index = index / 8;
        let bit_index = index % 8;

        bitfield[byte_index] |= 1 << bit_index;
    }

    bitfield
}
