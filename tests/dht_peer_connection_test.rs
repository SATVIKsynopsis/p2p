use p2p::dht::server::start_dht_server;
use p2p::dht::{DhtTable, announce_piece};
use p2p::network::tcp_server::start_server;
use p2p::peer::Peer;
use p2p::piece::PieceManager;
use p2p::protocol::{Message, codec::write_message};
use tokio::net::TcpListener;

use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_dht_discovery_and_peer_connection() {
    let dht_table = Arc::new(Mutex::new(DhtTable::new()));

    let dht_server_table = Arc::clone(&dht_table);

    tokio::spawn(async move {
        start_dht_server("127.0.0.1:7600", dht_server_table)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let peer_a_address = "127.0.0.1:7601";

    tokio::spawn(async move {
    let listener = TcpListener::bind(peer_a_address).await.unwrap();

    loop {
        let (mut socket, _) = listener.accept().await.unwrap();

        write_message(
            &mut socket,
            &Message::Bitfield {
                bitfield: vec![0b0000_1000],
            },
        )
        .await
        .unwrap();
    }
});
    let peer_b_address = "127.0.0.1:7602";

   tokio::spawn(async move {
    let listener = TcpListener::bind(peer_b_address).await.unwrap();

    loop {
        let (mut socket, _) = listener.accept().await.unwrap();

        write_message(
            &mut socket,
            &Message::Bitfield {
                bitfield: vec![0b0000_1000],
            },
        )
        .await
        .unwrap();
    }
});

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    announce_piece("127.0.0.1:7600", 3, "peer_a", peer_a_address)
        .await
        .unwrap();

    announce_piece("127.0.0.1:7600", 3, "peer_b", peer_b_address)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let piece_manager = PieceManager::new_empty(10, 1024).unwrap();

    let peer = Peer::new("downloader".to_string(), piece_manager);

    let connections = peer
        .connect_to_discovered_peers("127.0.0.1:7600", 3)
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
