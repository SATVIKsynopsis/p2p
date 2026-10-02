use p2p::dht::{
    join_dht,
    DhtTable,
};
use p2p::dht::server::start_dht_server;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_dht_join_over_network() {
    let table = Arc::new(Mutex::new(DhtTable::new()));

    let server_table = Arc::clone(&table);

    tokio::spawn(async move {
        start_dht_server(
            "127.0.0.1:7300",
            server_table,
        )
        .await
        .unwrap();
    });

    // Give the server time to start.
    tokio::time::sleep(
        std::time::Duration::from_millis(100),
    )
    .await;

    join_dht(
        "127.0.0.1:7300",
        "peer_a",
        "127.0.0.1:8001",
    )
    .await
    .unwrap();

    tokio::time::sleep(
        std::time::Duration::from_millis(100),
    )
    .await;

    let table = table.lock().await;

    let peer = table
        .get_node("peer_a")
        .expect("Peer was not registered");

    assert_eq!(peer.node_id, "peer_a");
    assert_eq!(peer.address, "127.0.0.1:8001");
}