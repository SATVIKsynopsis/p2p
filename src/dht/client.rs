use crate::dht::DhtMessage;
use std::io;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const MAX_FRAME_SIZE: usize = 1024 * 1024;


pub async fn discover_peers_for_piece(
    dht_address: &str,
    piece_index: u32,
) -> Result<Vec<(String, String)>, io::Error> {
    find_dht_peers(dht_address, &format!("piece_{piece_index}")).await
}

pub async fn send_dht_message(address: &str, message: &DhtMessage) -> Result<(), io::Error> {
    let mut stream = connect(address).await?;
    write_message(&mut stream, message).await
}

pub async fn find_dht_peers(address: &str, key: &str) -> Result<Vec<(String, String)>, io::Error> {
    let mut stream = connect(address).await?;
    write_message(
        &mut stream,
        &DhtMessage::Find {
            key: key.to_owned(),
        },
    )
    .await?;
    let response = read_message(&mut stream).await?;
    match response {
        DhtMessage::Found {
            key: found_key,
            nodes,
        } if found_key == key => {
            if nodes
                .iter()
                .any(|(id, addr)| id.trim().is_empty() || addr.trim().is_empty())
            {
                return Err(invalid(
                    "DHT Found response contains an empty peer ID or address",
                ));
            }
            Ok(nodes)
        }
        DhtMessage::Found { .. } => Err(invalid("DHT Found response key does not match request")),
        _ => Err(invalid("expected Found response to DHT Find request")),
    }
}

pub async fn join_dht(address: &str, node_id: &str, node_address: &str) -> Result<(), io::Error> {
    send_dht_message(
        address,
        &DhtMessage::Join {
            node_id: node_id.to_owned(),
            address: node_address.to_owned(),
        },
    )
    .await
}

pub async fn announce_piece(
    dht_address: &str,
    piece_index: u32,
    node_id: &str,
    node_address: &str,
) -> Result<(), io::Error> {
    send_dht_message(
        dht_address,
        &DhtMessage::Store {
            key: format!("piece_{piece_index}"),
            node_id: node_id.to_owned(),
            address: node_address.to_owned(),
        },
    )
    .await
}

pub async fn announce_pieces(
    dht_address: &str,
    node_id: &str,
    node_address: &str,
    piece_manager: &crate::piece::PieceManager,
) -> Result<(), io::Error> {
    for index in 0..piece_manager.total_pieces() {
        let piece_index = index as u32;
        if piece_manager.has_piece(piece_index) {
            announce_piece(dht_address, piece_index, node_id, node_address).await?;
        }
    }
    Ok(())
}


pub async fn remove_piece(
    dht_address: &str,
    piece_index: u32,
    node_id: &str,
) -> Result<(), io::Error> {
    send_dht_message(
        dht_address,
        &DhtMessage::Remove {
            key: format!("piece_{piece_index}"),
            node_id: node_id.to_owned(),
        },
    )
    .await
}


pub async fn remove_peer(dht_address: &str, node_id: &str) -> Result<(), io::Error> {
    send_dht_message(
        dht_address,
        &DhtMessage::Remove {
            key: String::new(),
            node_id: node_id.to_owned(),
        },
    )
    .await
}

async fn connect(address: &str) -> Result<TcpStream, io::Error> {
    TcpStream::connect(address).await.map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("could not connect to DHT server {address}: {e}"),
        )
    })
}
async fn write_message(stream: &mut TcpStream, message: &DhtMessage) -> Result<(), io::Error> {
    let payload = bincode::serde::encode_to_vec(message, bincode::config::standard())
        .map_err(|e| invalid(format!("could not encode DHT request: {e}")))?;
    if payload.is_empty() || payload.len() > MAX_FRAME_SIZE {
        return Err(invalid("DHT request exceeds frame limit"));
    }
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("could not write DHT frame header: {e}")))?;
    stream
        .write_all(&payload)
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("could not write DHT request body: {e}")))
}
async fn read_message(stream: &mut TcpStream) -> Result<DhtMessage, io::Error> {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await.map_err(|e| {
        io::Error::new(e.kind(), format!("could not read DHT response header: {e}"))
    })?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > MAX_FRAME_SIZE {
        return Err(invalid(
            "DHT response frame length is outside the allowed range",
        ));
    }
    let mut payload = vec![0; length];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("could not read DHT response body: {e}")))?;
    let (message, used): (DhtMessage, usize) =
        bincode::serde::decode_from_slice(&payload, bincode::config::standard())
            .map_err(|e| invalid(format!("malformed DHT response: {e}")))?;
    if used != payload.len() {
        return Err(invalid("DHT response contains trailing bytes"));
    }
    Ok(message)
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
