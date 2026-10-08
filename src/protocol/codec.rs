use crate::protocol::Message;
use bincode::config;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub fn encode_message(message: &Message) -> Result<Vec<u8>, std::io::Error> {
    // serialize the message using bincode
    let encoded = bincode::serde::encode_to_vec(message, config::standard())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // 4 byte big endian length prefix
    let length_prefix = (encoded.len() as u32).to_be_bytes();

    let mut buffer = Vec::with_capacity(4 + encoded.len());

    // append length prefix
    buffer.extend_from_slice(&length_prefix);

    // append serialized message
    buffer.extend_from_slice(&encoded);

    Ok(buffer)
}

pub fn decode_message(bytes: &[u8]) -> Result<Message, std::io::Error> {
    if bytes.len() < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Not enough bytes to read length prefix",
        ));
    }

    let extract_bytes = &bytes[0..4];

    let payload_length = u32::from_be_bytes(extract_bytes.try_into().unwrap()) as usize;

    let actual_length = bytes.len() - 4;
    if actual_length != payload_length {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Length prefix does not match actual message length",
        ));
    }

    let paylod_extract = &bytes[4..4 + payload_length];

    //deserializing the message now
    let (message, _): (Message, usize) =
        bincode::serde::decode_from_slice(paylod_extract, config::standard())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    Ok(message)
}

pub async fn write_message(
    stream: &mut TcpStream,
    message: &Message,
) -> Result<(), std::io::Error> {
    let encoded_message = encode_message(message)?;

    stream.write_all(&encoded_message).await?;

    Ok(())
}

pub async fn read_message(stream: &mut TcpStream) -> Result<Message, std::io::Error> {
    let mut length_prefix = [0u8; 4];

    if let Err(e) = stream.read_exact(&mut length_prefix).await {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e));
    }

    let payload_length = u32::from_be_bytes(length_prefix) as usize;

    let mut buffer = vec![0u8; payload_length];

    if let Err(e) = stream.read_exact(&mut buffer).await {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e));
    }

    let frame = length_prefix
        .iter()
        .chain(buffer.iter())
        .cloned()
        .collect::<Vec<u8>>();

    let message = decode_message(&frame)?;

    Ok(message)
}
