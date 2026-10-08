use p2p::dht::server::start_dht_server;
use p2p::dht::{DhtMessage, DhtTable, send_dht_message};
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_dht_store_over_network() {
    let table = Arc::new(Mutex::new(DhtTable::new()));

    let server_table = Arc::clone(&table);

    tokio::spawn(async move {
        start_dht_server("127.0.0.1:7100", server_table)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let message = DhtMessage::Store {
        key: "piece_1".to_string(),
        node_id: "peer_a".to_string(),
        address: "127.0.0.1:8001".to_string(),
    };

    send_dht_message("127.0.0.1:7100", &message).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let table = table.lock().await;

    let peers = table.find("piece_1");

    assert_eq!(peers, vec!["peer_a".to_string()]);
}
