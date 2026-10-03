use p2p::dht::{announce_piece, DhtTable};
use p2p::dht::server::start_dht_server;
use p2p::network::tcp_server::start_server;
use p2p::peer::Peer;
use p2p::piece::PieceManager;

use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_dht_discovers_connections_for_missing_pieces() {
    let dht_table = Arc::new(Mutex::new(DhtTable::new()));

    let server_table = Arc::clone(&dht_table);

    tokio::spawn(async move {
        start_dht_server(
            "127.0.0.1:7700",
            server_table,
        )
        .await
        .unwrap();
    });

    tokio::time::sleep(
        std::time::Duration::from_millis(100),
    )
    .await;

    // Start two actual TCP peers.
    let peer_a_address = "127.0.0.1:7701";
    let peer_b_address = "127.0.0.1:7702";

    tokio::spawn(async move {
        start_server(peer_a_address)
            .await
            .unwrap();
    });

    tokio::spawn(async move {
        start_server(peer_b_address)
            .await
            .unwrap();
    });

    tokio::time::sleep(
        std::time::Duration::from_millis(100),
    )
    .await;

    // Peer A owns piece 1.
    announce_piece(
        "127.0.0.1:7700",
        1,
        "peer_a",
        peer_a_address,
    )
    .await
    .unwrap();

    // Peer B owns piece 2.
    announce_piece(
        "127.0.0.1:7700",
        2,
        "peer_b",
        peer_b_address,
    )
    .await
    .unwrap();

    tokio::time::sleep(
        std::time::Duration::from_millis(100),
    )
    .await;

    // Downloader has 3 pieces but owns none.
    let piece_manager =
        PieceManager::new_empty(3, 1024)
            .unwrap();

    let peer = Peer::new(
        "downloader".to_string(),
        PieceManager::new_empty(3, 1024)
            .unwrap(),
    );

    let connections =
        peer.discover_connections_for_missing_pieces(
            "127.0.0.1:7700",
            &piece_manager,
        )
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