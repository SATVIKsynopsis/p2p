use p2p::dht::{
    announce_piece,
    find_dht_peers,
    DhtTable,
};
use p2p::dht::server::start_dht_server;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_piece_announcement_over_network() {
    let table = Arc::new(Mutex::new(DhtTable::new()));

    let server_table = Arc::clone(&table);

    tokio::spawn(async move {
        start_dht_server(
            "127.0.0.1:7400",
            server_table,
        )
        .await
        .unwrap();
    });

    tokio::time::sleep(
        std::time::Duration::from_millis(100),
    )
    .await;

    announce_piece(
        "127.0.0.1:7400",
        3,
        "peer_a",
        "127.0.0.1:8001",
    )
    .await
    .unwrap();

    tokio::time::sleep(
        std::time::Duration::from_millis(100),
    )
    .await;

    // Ask the DHT who owns piece 3.
    let peers = find_dht_peers(
        "127.0.0.1:7400",
        "piece_3",
    )
    .await
    .unwrap();

    assert_eq!(peers.len(), 1);

    assert_eq!(
        peers[0],
        (
            "peer_a".to_string(),
            "127.0.0.1:8001".to_string(),
        )
    );
}