use crate::dht::{DhtMessage, DhtTable};
use std::{io, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

const MAX_FRAME_SIZE: usize = 1024 * 1024;


pub async fn start_dht_server(address: &str, table: Arc<Mutex<DhtTable>>) -> Result<(), io::Error> {
    let listener = TcpListener::bind(address).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let shared = Arc::clone(&table);
        tokio::spawn(async move {
            if let Err(error) = handle_client(stream, shared).await {
                if error.kind() != io::ErrorKind::UnexpectedEof
                    && error.kind() != io::ErrorKind::ConnectionReset
                {
                    eprintln!("DHT client request failed: {error}");
                }
            }
        });
    }
}

async fn handle_client(
    mut stream: TcpStream,
    table: Arc<Mutex<DhtTable>>,
) -> Result<(), io::Error> {
    let message = read_message(&mut stream).await?;
    match message {
        DhtMessage::Join { node_id, address } => {
            validate_peer(&node_id, &address)?;
            table
                .lock()
                .await
                .add_node(crate::dht::DhtNode::new(node_id, address));
        }
        DhtMessage::Store {
            key,
            node_id,
            address,
        } => {
            validate_peer(&node_id, &address)?;
            validate_key(&key)?;
            let mut table = table.lock().await;
            table.add_node(crate::dht::DhtNode::new(node_id.clone(), address));
            table.store(key, node_id);
        }
        DhtMessage::Find { key } => {
            validate_key(&key)?;
            let nodes = {
                let mut table = table.lock().await;
                table.cleanup_stale(std::time::Duration::from_secs(300));
                table
                    .find(&key)
                    .into_iter()
                    .filter_map(|id| {
                        table
                            .get_node(&id)
                            .map(|node| (node.node_id.clone(), node.address.clone()))
                    })
                    .collect()
            };
            write_message(&mut stream, &DhtMessage::Found { key, nodes }).await?;
        }
        DhtMessage::Remove { key, node_id } => {
            if node_id.trim().is_empty() {
                return Err(invalid("node_id cannot be empty"));
            }
            let mut table = table.lock().await;
            if key.is_empty() {
                table.remove_node(&node_id);
            } else {
                validate_key(&key)?;
                table.remove_record(&key, &node_id);
            }
        }
        DhtMessage::Found { .. } => {
            return Err(invalid(
                "Found is a response and cannot be sent to the server",
            ));
        }
    }
    Ok(())
}

async fn read_message(stream: &mut TcpStream) -> Result<DhtMessage, io::Error> {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > MAX_FRAME_SIZE {
        return Err(invalid("DHT frame length is outside the allowed range"));
    }
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).await?;
    let (message, used): (DhtMessage, usize) =
        bincode::serde::decode_from_slice(&payload, bincode::config::standard())
            .map_err(|e| invalid(format!("invalid DHT message: {e}")))?;
    if used != payload.len() {
        return Err(invalid("DHT frame contains trailing bytes"));
    }
    Ok(message)
}

async fn write_message(stream: &mut TcpStream, message: &DhtMessage) -> Result<(), io::Error> {
    let payload = bincode::serde::encode_to_vec(message, bincode::config::standard())
        .map_err(|e| invalid(format!("could not encode DHT response: {e}")))?;
    if payload.is_empty() || payload.len() > MAX_FRAME_SIZE {
        return Err(invalid("DHT response exceeds frame limit"));
    }
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&payload).await
}

fn validate_peer(id: &str, address: &str) -> Result<(), io::Error> {
    if id.trim().is_empty() || id.len() > 256 {
        return Err(invalid("node_id must be 1..=256 bytes"));
    }
    if address.trim().is_empty() || address.len() > 512 {
        return Err(invalid("address must be 1..=512 bytes"));
    }
    Ok(())
}
fn validate_key(key: &str) -> Result<(), io::Error> {
    if key.trim().is_empty() || key.len() > 256 {
        return Err(invalid("key must be 1..=256 bytes"));
    }
    Ok(())
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
