use p2p::dht::{
    find_dht_peers,
    send_dht_message,
    DhtMessage,
    DhtTable,
};
use p2p::dht::server::start_dht_server;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_dht_find_over_network() {
    let table = Arc::new(Mutex::new(DhtTable::new()));

    let server_table = Arc::clone(&table);

    tokio::spawn(async move {
        start_dht_server(
            "127.0.0.1:7200",
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

    // Store peer A.
    send_dht_message(
        "127.0.0.1:7200",
        &DhtMessage::Store {
            key: "piece_1".to_string(),
            node_id: "peer_a".to_string(),
            address: "127.0.0.1:8001".to_string(),
        },
    )
    .await
    .unwrap();

    // Store peer B.
    send_dht_message(
        "127.0.0.1:7200",
        &DhtMessage::Store {
            key: "piece_1".to_string(),
            node_id: "peer_b".to_string(),
            address: "127.0.0.1:8002".to_string(),
        },
    )
    .await
    .unwrap();

    tokio::time::sleep(
        std::time::Duration::from_millis(100),
    )
    .await;

    // Find peers owning piece_1.
    let peers = find_dht_peers(
        "127.0.0.1:7200",
        "piece_1",
    )
    .await
    .unwrap();

    assert_eq!(peers.len(), 2);

    assert!(peers.contains(&(
        "peer_a".to_string(),
        "127.0.0.1:8001".to_string(),
    )));

    assert!(peers.contains(&(
        "peer_b".to_string(),
        "127.0.0.1:8002".to_string(),
    )));
}   