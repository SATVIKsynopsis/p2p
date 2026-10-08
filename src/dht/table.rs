use std::collections::HashMap;

use super::node::DhtNode;

pub struct DhtTable {
    pub nodes: HashMap<String, DhtNode>,
    pub records: HashMap<String, Vec<String>>,
}

impl DhtTable {
    pub fn new() -> DhtTable {
        DhtTable {
            nodes: HashMap::new(),
            records: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: DhtNode) {
        self.nodes.insert(node.node_id.clone(), node);
    }

    pub fn remove_node(&mut self, node_id: &str) -> Option<DhtNode> {
        self.nodes.remove(node_id)
    }

    pub fn get_node(&self, node_id: &str) -> Option<&DhtNode> {
        self.nodes.get(node_id)
    }

    pub fn all_nodes(&self) -> Vec<&DhtNode> {
        self.nodes.values().collect()
    }

    pub fn contains_node(&self, node_id: &str) -> bool {
        self.nodes.contains_key(node_id)
    }

    pub fn store(&mut self, key: String, node_id: String) {
        let peers = self.records.entry(key).or_insert_with(Vec::new);

        if !peers.contains(&node_id) {
            peers.push(node_id);
        }
    }

    pub fn find(&self, key: &str) -> Vec<String> {
        self.records.get(key).cloned().unwrap_or_default()
    }

    pub fn remove_record(&mut self, key: &str, node_id: &str) {
        if let Some(peers) = self.records.get_mut(key) {
            peers.retain(|id| id != node_id);

            if peers.is_empty() {
                self.records.remove(key);
            }
        }
    }
}
