use p2p::dht::server::start_dht_server;
use p2p::dht::{DhtTable, announce_pieces};
use p2p::peer::Peer;
use p2p::piece::PieceManager;
use p2p::protocol::Message;
use p2p::transfer::Uploader;

use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

async fn start_seeder(address: &str, uploader: Arc<Uploader>) {
    let listener = TcpListener::bind(address).await.unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();

            let uploader = Arc::clone(&uploader);

            tokio::spawn(async move {
                let mut connection = p2p::peer::PeerConnection::new("".to_string(), stream);

                let bitfield = uploader.piece_manager.bitfield();

                println!("Seeder sending bitfield: {:?}", bitfield);

                if connection.handshake("seeder").await.is_err()
                    || connection.exchange_bitfield(bitfield).await.is_err()
                {
                    return;
                }
                if connection.send_message(&Message::Unchoke).await.is_err() {
                    return;
                }

                loop {
                    match connection.receive_message().await {
                        Ok(Message::Request { piece_index }) => {
                            if uploader
                                .serve_request(&mut connection, piece_index)
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }

                        Ok(_) => {}

                        Err(_) => break,
                    }
                }
            });
        }
    });
}

#[tokio::test]
async fn test_dht_discovered_download() {
    let input_path = "dht_download_input.txt";
    let output_path = "dht_download_output.txt";

    let data = b"abcdefghijklmnopqrstuvwxyz0123456789";

    std::fs::write(input_path, data).unwrap();

    let piece_size = 5;

    let peer_a_manager = PieceManager::try_from_file(input_path, piece_size).unwrap();

    let peer_a_uploader = Arc::new(Uploader::new(peer_a_manager));

    let peer_b_manager = PieceManager::try_from_file(input_path, piece_size).unwrap();

    let peer_b_uploader = Arc::new(Uploader::new(peer_b_manager));

    let dht_table = Arc::new(Mutex::new(DhtTable::new()));

    let server_table = Arc::clone(&dht_table);

    tokio::spawn(async move {
        start_dht_server("127.0.0.1:7800", server_table)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    start_seeder("127.0.0.1:7801", Arc::clone(&peer_a_uploader)).await;

    start_seeder("127.0.0.1:7802", Arc::clone(&peer_b_uploader)).await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    announce_pieces(
        "127.0.0.1:7800",
        "peer_a",
        "127.0.0.1:7801",
        &peer_a_uploader.piece_manager,
    )
    .await
    .unwrap();

    announce_pieces(
        "127.0.0.1:7800",
        "peer_b",
        "127.0.0.1:7802",
        &peer_b_uploader.piece_manager,
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let empty_manager =
        PieceManager::new_empty(peer_a_uploader.piece_manager.total_pieces(), piece_size).unwrap();

    let peer = Peer::new(
        "downloader".to_string(),
        PieceManager::new_empty(peer_a_uploader.piece_manager.total_pieces(), piece_size).unwrap(),
    );

    peer.download_from_dht("127.0.0.1:7800", empty_manager)
        .await
        .unwrap();

    std::fs::remove_file(input_path).unwrap();

    if std::path::Path::new(output_path).exists() {
        std::fs::remove_file(output_path).unwrap();
    }
}
