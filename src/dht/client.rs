use crate::dht::DhtMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn discover_peers_for_piece(
    dht_address: &str,
    piece_index: u32,
) -> Result<Vec<(String, String)>, std::io::Error> {
    let key = format!("piece_{}", piece_index);

    find_dht_peers(
        dht_address,
        &key,
    )
    .await
}

pub async fn send_dht_message(
    address: &str,
    message: &DhtMessage,
) -> Result<(), std::io::Error> {
    let mut stream = TcpStream::connect(address).await?;

    let encoded_message =
        bincode::serde::encode_to_vec(
            message,
            bincode::config::standard(),
        )
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error,
            )
        })?;

    let length_prefix =
        (encoded_message.len() as u32).to_be_bytes();

    stream.write_all(&length_prefix).await?;
    stream.write_all(&encoded_message).await?;

    Ok(())
}

pub async fn find_dht_peers(
    address: &str,
    key: &str,
) -> Result<Vec<(String, String)>, std::io::Error> {
    let mut stream = TcpStream::connect(address).await?;

    let message = DhtMessage::Find {
        key: key.to_string(),
    };

    let encoded_message =
        bincode::serde::encode_to_vec(
            &message,
            bincode::config::standard(),
        )
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error,
            )
        })?;

    let length_prefix =
        (encoded_message.len() as u32).to_be_bytes();

    stream.write_all(&length_prefix).await?;
    stream.write_all(&encoded_message).await?;

    let mut response_length = [0u8; 4];

    stream.read_exact(&mut response_length).await?;

    let length =
        u32::from_be_bytes(response_length) as usize;

    let mut payload = vec![0u8; length];

    stream.read_exact(&mut payload).await?;

    let (response, _) =
        bincode::serde::decode_from_slice(
            &payload,
            bincode::config::standard(),
        )
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error,
            )
        })?;

    match response {
        DhtMessage::Found { nodes, .. } => Ok(nodes),

        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Expected Found response",
        )),
    }
}

pub async fn join_dht(
    address: &str,
    node_id: &str,
    node_address: &str,
) -> Result<(), std::io::Error> {
    let message = DhtMessage::Join {
        node_id: node_id.to_string(),
        address: node_address.to_string(),
    };

    send_dht_message(address, &message).await
}

pub async fn announce_piece(
    dht_address: &str,
    piece_index: u32,
    node_id: &str,
    node_address: &str,
) -> Result<(), std::io::Error> {
    let message = DhtMessage::Store {
        key: format!("piece_{}", piece_index),
        node_id: node_id.to_string(),
        address: node_address.to_string(),
    };

    send_dht_message(dht_address, &message).await
}

pub async fn announce_pieces(
    dht_address: &str,
    node_id: &str,
    node_address: &str,
    piece_manager: &crate::piece::PieceManager,
) -> Result<(), std::io::Error> {
    for index in 0..piece_manager.total_pieces() {
        let piece_index = index as u32;

        if piece_manager.has_piece(piece_index) {
            announce_piece(
                dht_address,
                piece_index,
                node_id,
                node_address,
            )
            .await?;
        }
    }

    Ok(())
}