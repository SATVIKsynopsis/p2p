use p2p::dht::server::start_dht_server;
use p2p::dht::{DhtTable, announce_piece};
use p2p::peer::Peer;
use p2p::piece::PieceManager;
use tokio::net::TcpListener;

use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_dht_discovers_connections_for_missing_pieces() {
    let dht_table = Arc::new(Mutex::new(DhtTable::new()));

    let server_table = Arc::clone(&dht_table);

    tokio::spawn(async move {
        start_dht_server("127.0.0.1:7700", server_table)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let peer_a_address = "127.0.0.1:7701";
    let peer_b_address = "127.0.0.1:7702";

    tokio::spawn(async move {
        let listener = TcpListener::bind(peer_a_address).await.unwrap();

        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let mut connection = p2p::peer::PeerConnection::new(String::new(), socket);
            connection.handshake("peer_a").await.unwrap();
            connection
                .exchange_bitfield(vec![0b0000_0010])
                .await
                .unwrap();
            connection
                .send_message(&p2p::protocol::Message::Unchoke)
                .await
                .unwrap();
        }
    });

    tokio::spawn(async move {
        let listener = TcpListener::bind(peer_b_address).await.unwrap();

        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let mut connection = p2p::peer::PeerConnection::new(String::new(), socket);
            connection.handshake("peer_b").await.unwrap();
            connection
                .exchange_bitfield(vec![0b0000_0100])
                .await
                .unwrap();
            connection
                .send_message(&p2p::protocol::Message::Unchoke)
                .await
                .unwrap();
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    announce_piece("127.0.0.1:7700", 1, "peer_a", peer_a_address)
        .await
        .unwrap();

    announce_piece("127.0.0.1:7700", 2, "peer_b", peer_b_address)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let piece_manager = PieceManager::new_empty(3, 1024).unwrap();

    let peer = Peer::new(
        "downloader".to_string(),
        PieceManager::new_empty(3, 1024).unwrap(),
    );

    let connections = peer
        .discover_connections_for_missing_pieces("127.0.0.1:7700", &piece_manager)
        .await
        .unwrap();

    assert_eq!(connections.len(), 2);

    assert!(
        connections
            .iter()
            .any(|connection| connection.peer_id == "peer_a")
    );

    assert!(
        connections
            .iter()
            .any(|connection| connection.peer_id == "peer_b")
    );
}
