use crate::protocol::codec::{decode_message, encode_message};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::dht::{DhtMessage, DhtTable};
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn start_dht_server(
    address: &str,
    table: Arc<Mutex<DhtTable>>,
) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(address).await?;

    println!("DHT server listening on {}", address);

    loop {
        let (mut stream, peer_address) = listener.accept().await?;

        println!("DHT connection from {}", peer_address);

        let table = Arc::clone(&table);

        tokio::spawn(async move {
            let mut length_prefix = [0u8; 4];

            if stream.read_exact(&mut length_prefix).await.is_err() {
                return;
            }

            let length = u32::from_be_bytes(length_prefix) as usize;

            let mut payload = vec![0u8; length];

            if stream.read_exact(&mut payload).await.is_err() {
                return;
            }

            let mut frame = Vec::with_capacity(4 + length);
            frame.extend_from_slice(&length_prefix);
            frame.extend_from_slice(&payload);

            let message = match decode_dht_message(&frame) {
                Ok(message) => message,
                Err(error) => {
                    eprintln!("Failed to decode DHT message: {}", error);
                    return;
                }
            };

            let mut table = table.lock().await;

            match message {
                DhtMessage::Join { node_id, address } => {
                    table.add_node(crate::dht::DhtNode::new(node_id, address));
                }

                DhtMessage::Store {
                    key,
                    node_id,
                    address,
                } => {
                    table.add_node(crate::dht::DhtNode::new(node_id.clone(), address));

                    table.store(key, node_id);
                }

                DhtMessage::Find { key } => {
                    let peer_ids = table.find(&key);

                    let mut nodes = Vec::new();

                    for peer_id in peer_ids {
                        if let Some(node) = table.get_node(&peer_id) {
                            nodes.push((node.node_id.clone(), node.address.clone()));
                        }
                    }

                    let response = DhtMessage::Found { key, nodes };

                    let encoded =
                        match bincode::serde::encode_to_vec(&response, bincode::config::standard())
                        {
                            Ok(encoded) => encoded,
                            Err(error) => {
                                eprintln!("Failed to encode DHT response: {}", error);
                                return;
                            }
                        };

                    let length = (encoded.len() as u32).to_be_bytes();

                    if stream.write_all(&length).await.is_err() {
                        return;
                    }

                    if stream.write_all(&encoded).await.is_err() {
                        return;
                    }
                }

                DhtMessage::Remove { key, node_id } => {
                    table.remove_record(&key, &node_id);
                }

                DhtMessage::Found { .. } => {}
            }
        });
    }
}

fn encode_dht_message(message: &DhtMessage) -> Result<Vec<u8>, std::io::Error> {
    let encoded = bincode::serde::encode_to_vec(message, bincode::config::standard())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;

    let length = (encoded.len() as u32).to_be_bytes();

    let mut frame = Vec::with_capacity(4 + encoded.len());

    frame.extend_from_slice(&length);
    frame.extend_from_slice(&encoded);

    Ok(frame)
}

fn decode_dht_message(bytes: &[u8]) -> Result<DhtMessage, std::io::Error> {
    if bytes.len() < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid DHT frame",
        ));
    }

    let length = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;

    if bytes.len() - 4 != length {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "DHT frame length mismatch",
        ));
    }

    let (message, _) = bincode::serde::decode_from_slice(&bytes[4..], bincode::config::standard())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;

    Ok(message)
}
