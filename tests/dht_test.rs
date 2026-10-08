use p2p::dht::{DhtNode, DhtTable};

#[test]
fn test_dht_store_and_find() {
    let mut table = DhtTable::new();

    let peer_a = DhtNode::new("peer_a".to_string(), "127.0.0.1:7001".to_string());

    let peer_b = DhtNode::new("peer_b".to_string(), "127.0.0.1:7002".to_string());

    table.add_node(peer_a);
    table.add_node(peer_b);

    table.store("piece_1".to_string(), "peer_a".to_string());

    table.store("piece_1".to_string(), "peer_b".to_string());

    let peers = table.find("piece_1");

    assert_eq!(peers.len(), 2);
    assert!(peers.contains(&"peer_a".to_string()));
    assert!(peers.contains(&"peer_b".to_string()));
}

#[test]
fn test_dht_remove_record() {
    let mut table = DhtTable::new();

    table.store("piece_1".to_string(), "peer_a".to_string());

    table.store("piece_1".to_string(), "peer_b".to_string());

    table.remove_record("piece_1", "peer_a");

    let peers = table.find("piece_1");

    assert_eq!(peers, vec!["peer_b".to_string()]);
}
