use p2p::{
    dht::{DhtTable, announce_pieces, server::start_dht_server},
    network::tcp_server::serve_peer,
    peer::Peer,
    piece::{PieceManager, new_piece},
    transfer::Downloader,
};
use std::{sync::Arc, time::Instant};
use tokio::{net::TcpListener, sync::Mutex};

async fn unused_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

#[tokio::test]
async fn dht_handshake_three_peer_transfer_reconstructs_original() {
    let original =
        b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!@#$%^&*ABCDEFGH";
    let piece_size = 10;
    let pieces: Vec<_> = original
        .chunks(piece_size)
        .enumerate()
        .map(|(i, bytes)| new_piece(i as u32, bytes.to_vec()))
        .collect();
    let count = pieces.len();
    assert_eq!(count, 8);

    let dht_addr = unused_address().await;
    let table = Arc::new(Mutex::new(DhtTable::new()));
    let server_table = Arc::clone(&table);
    let server_addr = dht_addr.clone();
    let dht_task = tokio::spawn(async move {
        start_dht_server(&server_addr, server_table).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;

    let ranges = [0..2, 2..4, 4..8];
    let mut peer_tasks = Vec::new();
    let mut announcements = Vec::new();
    for (peer_no, range) in ranges.into_iter().enumerate() {
        let mut manager = PieceManager::new_empty(count, piece_size).unwrap();
        for idx in range {
            manager.add_piece(pieces[idx].clone()).unwrap();
        }
        let manager = Arc::new(manager);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let id = format!("seed{}", peer_no + 1);
        announcements.push((id.clone(), address.clone(), Arc::clone(&manager)));
        peer_tasks.push(tokio::spawn(serve_peer(listener, id, manager)));
    }
    for (id, address, manager) in &announcements {
        announce_pieces(&dht_addr, id, address, manager)
            .await
            .unwrap();
    }

    let empty = PieceManager::new_empty(count, piece_size).unwrap();
    let peer = Peer::new(
        "download-client".into(),
        PieceManager::new_empty(count, piece_size).unwrap(),
    );
    let connections = peer
        .discover_connections_for_missing_pieces(&dht_addr, &empty)
        .await
        .unwrap();
    assert_eq!(
        connections.len(),
        3,
        "DHT discovery should find all three seeders"
    );

    let downloader = Arc::new(Downloader::new(empty));
    let started = Instant::now();
    downloader
        .clone()
        .download_concurrently(connections)
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let output = std::env::temp_dir().join(format!("p2p_e2e_{}.bin", std::process::id()));
    downloader
        .reassemble(output.to_str().unwrap())
        .await
        .unwrap();
    let reconstructed = std::fs::read(&output).unwrap();
    std::fs::remove_file(output).unwrap();
    assert_eq!(original.as_slice(), reconstructed.as_slice());
    println!(
        "E2E measurement: peers=3 pieces={count} connections=3 elapsed_ms={}",
        elapsed.as_millis()
    );

    for task in peer_tasks {
        task.abort();
    }
    dht_task.abort();
}
