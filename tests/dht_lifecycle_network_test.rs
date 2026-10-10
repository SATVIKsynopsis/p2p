use p2p::dht::{
    DhtMessage, DhtTable, announce_piece, find_dht_peers, join_dht, remove_peer, send_dht_message,
};
use std::sync::Arc;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

const ADDRESS: &str = "127.0.0.1:7501";

#[tokio::test]
async fn lifecycle_concurrency_and_malformed_client_isolation() {
    let table = Arc::new(Mutex::new(DhtTable::new()));
    let shared = Arc::clone(&table);
    tokio::spawn(async move {
        let _ = p2p::dht::server::start_dht_server(ADDRESS, shared).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    join_dht(ADDRESS, "same_peer", "127.0.0.1:8001")
        .await
        .unwrap();
    join_dht(ADDRESS, "same_peer", "127.0.0.1:8002")
        .await
        .unwrap();
    announce_piece(ADDRESS, 0, "same_peer", "127.0.0.1:8002")
        .await
        .unwrap();
    announce_piece(ADDRESS, 0, "same_peer", "127.0.0.1:8002")
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for i in 0..12u32 {
        tasks.push(tokio::spawn(async move {
            let id = format!("peer_{i}");
            announce_piece(ADDRESS, 5, &id, &format!("127.0.0.1:{}", 8100 + i))
                .await
                .unwrap();
            assert!(!find_dht_peers(ADDRESS, "piece_5").await.unwrap().is_empty());
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(
        find_dht_peers(ADDRESS, "piece_0").await.unwrap(),
        vec![("same_peer".into(), "127.0.0.1:8002".into())]
    );
    assert!(
        find_dht_peers(ADDRESS, "piece_missing")
            .await
            .unwrap()
            .is_empty()
    );

    remove_peer(ADDRESS, "same_peer").await.unwrap();
    assert!(find_dht_peers(ADDRESS, "piece_0").await.unwrap().is_empty());
    assert!(!table.lock().await.contains_node("same_peer"));

    let mut stream = TcpStream::connect(ADDRESS).await.unwrap();
    stream.write_all(&3u32.to_be_bytes()).await.unwrap();
    stream.write_all(&[0xff, 0xff, 0xff]).await.unwrap();
    drop(stream);
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    join_dht(ADDRESS, "healthy", "127.0.0.1:8003")
        .await
        .unwrap();
    assert!(
        find_dht_peers(ADDRESS, "piece_never_announced")
            .await
            .unwrap()
            .is_empty()
    );
    send_dht_message(
        ADDRESS,
        &DhtMessage::Store {
            key: "piece_9".into(),
            node_id: "healthy".into(),
            address: "127.0.0.1:8003".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(find_dht_peers(ADDRESS, "piece_9").await.unwrap().len(), 1);
}
