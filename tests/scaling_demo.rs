use p2p::{
    network::tcp_server::serve_peer,
    peer::PeerConnection,
    piece::{PieceManager, new_piece},
    transfer::Downloader,
};
use std::{sync::Arc, time::Instant};
use tokio::net::TcpListener;

async fn measure(peer_count: usize) -> u128 {
    let original =
        b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQR";
    let piece_size = 10;
    let pieces: Vec<_> = original
        .chunks(piece_size)
        .enumerate()
        .map(|(index, data)| new_piece(index as u32, data.to_vec()))
        .collect();
    let mut tasks = Vec::new();
    let mut connections = Vec::new();
    for peer_index in 0..peer_count {
        let mut manager = PieceManager::new_empty(pieces.len(), piece_size).unwrap();
        for (piece_index, piece) in pieces.iter().enumerate() {
            if piece_index % peer_count == peer_index {
                manager.add_piece(piece.clone()).unwrap();
            }
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tasks.push(tokio::spawn(serve_peer(
            listener,
            format!("scale-{peer_index}"),
            Arc::new(manager.clone()),
        )));
        connections.push((format!("scale-{peer_index}"), address, manager));
    }
    let mut established = Vec::new();
    for (id, address, _) in &connections {
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut connection = PeerConnection::new(id.clone(), stream);
        connection.handshake("measurement-client").await.unwrap();
        connection
            .exchange_bitfield(vec![0; (pieces.len() + 7) / 8])
            .await
            .unwrap();
        connection.receive_choke_state().await.unwrap();
        established.push(connection);
    }
    let downloader = Arc::new(Downloader::new(
        PieceManager::new_empty(pieces.len(), piece_size).unwrap(),
    ));
    let started = Instant::now();
    downloader
        .clone()
        .download_concurrently(established)
        .await
        .unwrap();
    let elapsed = started.elapsed().as_millis();
    assert!(downloader.is_complete().await);
    for task in tasks {
        task.abort();
    }
    elapsed
}

#[tokio::test]
async fn report_one_two_three_peer_measurements() {
    for peers in 1..=3 {
        let elapsed_ms = measure(peers).await;
        println!(
            "scale demo: peers={peers} pieces=8 concurrent_connections={peers} elapsed_ms={elapsed_ms}"
        );
    }
}
