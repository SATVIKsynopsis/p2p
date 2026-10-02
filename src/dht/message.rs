use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum DhtMessage {
    Join {
        node_id: String,
        address: String,
    },

    Store {
        key: String,
        node_id: String,
        address: String,
    },

    Find {
        key: String,
    },

    Found {
        key: String,
        nodes: Vec<(String, String)>,
    },

    Remove {
        key: String,
        node_id: String,
    },
}