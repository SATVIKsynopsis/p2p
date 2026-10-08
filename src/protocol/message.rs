use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Handshake {
        peer_id: String,
    },

    Bitfield {
        bitfield: Vec<u8>,
    },

    Have {
        piece_index: u32,
    },

    Request {
        piece_index: u32,
    },

    Piece {
        piece_index: u32,
        data: Vec<u8>,
        hash: [u8; 32],
    },

    Choke,

    Unchoke,

    Ping,

    Pong,
}
