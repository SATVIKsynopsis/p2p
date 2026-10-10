use p2p::dht::{DhtNode, DhtTable};
use std::time::Duration;

#[test]
fn lifecycle_and_piece_records_are_idempotent() {
    let mut table = DhtTable::new();
    let peer = DhtNode::new("peer".into(), "127.0.0.1:7001".into());
    table.add_node(peer.clone());
    table.add_node(peer);
    assert_eq!(table.all_nodes().len(), 1);

    table.store("piece_0".into(), "peer".into());
    table.store("piece_0".into(), "peer".into());
    table.store("piece_1".into(), "peer".into());
    assert_eq!(table.find("piece_0"), vec!["peer"]);
    assert_eq!(table.find("piece_1"), vec!["peer"]);

    table.remove_record("piece_0", "peer");
    assert!(table.find("piece_0").is_empty());
    assert_eq!(table.find("piece_1"), vec!["peer"]);
    table.remove_node("peer");
    assert!(table.find("piece_1").is_empty());
    assert!(!table.contains_node("peer"));
}

#[test]
fn missing_piece_is_empty_and_orphaned_records_are_hidden_by_registry() {
    let mut table = DhtTable::new();
    table.store("piece_8".into(), "gone".into());
    assert_eq!(table.find("piece_8"), vec!["gone"]); // raw table compatibility
    table.add_node(DhtNode::new("gone".into(), "127.0.0.1:9".into()));
    table.remove_node("gone");
    assert!(table.find("piece_8").is_empty());
    assert!(table.find("piece_missing").is_empty());
}

#[test]
fn stale_peer_cleanup_removes_its_ownership() {
    let mut table = DhtTable::new();
    table.add_node(DhtNode::new("stale".into(), "127.0.0.1:7010".into()));
    table.store("piece_2".into(), "stale".into());
    table.cleanup_stale(Duration::ZERO);
    assert!(!table.contains_node("stale"));
    assert!(table.find("piece_2").is_empty());
}
