#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhtNode {
    pub node_id: String,
    pub address: String,
}

impl DhtNode {
    pub fn new(node_id: String, address: String) -> DhtNode {
        DhtNode { node_id, address }
    }
}