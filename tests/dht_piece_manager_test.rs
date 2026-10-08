use p2p::dht::server::start_dht_server;
use p2p::dht::{DhtTable, announce_pieces, find_dht_peers};
use p2p::piece::PieceManager;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_piece_manager_announcement() {
    let input_path = "test_dht_input.txt";

    std::fs::write(input_path, b"abcdefghijklmnopqrstuvwxyz").unwrap();

    let piece_manager = PieceManager::try_from_file(input_path, 5).unwrap();

    let table = Arc::new(Mutex::new(DhtTable::new()));

    let server_table = Arc::clone(&table);

    tokio::spawn(async move {
        start_dht_server("127.0.0.1:7500", server_table)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    announce_pieces("127.0.0.1:7500", "peer_a", "127.0.0.1:8001", &piece_manager)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    for piece_index in 0..6 {
        let peers = find_dht_peers("127.0.0.1:7500", &format!("piece_{}", piece_index))
            .await
            .unwrap();

        assert_eq!(peers.len(), 1);

        assert_eq!(
            peers[0],
            ("peer_a".to_string(), "127.0.0.1:8001".to_string(),)
        );
    }

    std::fs::remove_file(input_path).unwrap();
}
