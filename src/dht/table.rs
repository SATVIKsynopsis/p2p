use super::node::DhtNode;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};


pub struct DhtTable {
    pub nodes: HashMap<String, DhtNode>,
    pub records: HashMap<String, Vec<String>>,
    last_seen: HashMap<String, Instant>,
}

impl DhtTable {
    pub fn new() -> DhtTable {
        DhtTable {
            nodes: HashMap::new(),
            records: HashMap::new(),
            last_seen: HashMap::new(),
        }
    }

    
    pub fn add_node(&mut self, node: DhtNode) {
        self.last_seen.insert(node.node_id.clone(), Instant::now());
        self.nodes.insert(node.node_id.clone(), node);
    }

    
    pub fn remove_node(&mut self, node_id: &str) -> Option<DhtNode> {
        for peers in self.records.values_mut() {
            peers.retain(|id| id != node_id);
        }
        self.records.retain(|_, peers| !peers.is_empty());
        self.last_seen.remove(node_id);
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
        let peers = self.records.entry(key).or_default();
        if !peers.contains(&node_id) {
            peers.push(node_id);
        }
    }

   
    pub fn find(&self, key: &str) -> Vec<String> {
        self.records.get(key).cloned().unwrap_or_default()
    }

 
    pub fn cleanup_stale(&mut self, max_age: Duration) {
        let now = Instant::now();
        let stale: Vec<String> = self
            .last_seen
            .iter()
            .filter(|(_, seen)| now.duration_since(**seen) >= max_age)
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            self.remove_node(&id);
        }
    }
    pub fn remove_record(&mut self, key: &str, node_id: &str) {
        if let Some(peers) = self.records.get_mut(key) {
            peers.retain(|id| id != node_id);
        }
        self.records.retain(|_, peers| !peers.is_empty());
    }
}
