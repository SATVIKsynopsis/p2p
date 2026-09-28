use crate::peer::connection::PeerConnection;
use std::collections::HashMap;
use crate::protocol::Message;

pub struct Peer {
    pub peer_id: String,
    pub connections: HashMap<String, PeerConnection>,
}

impl Peer {

pub fn new(peer_id: String) -> Peer {
    Peer {
        peer_id,
        connections: HashMap::new(),
    }
}

pub fn add_connection(
    &mut self,
    connection: PeerConnection,
) {
    let peer_id = connection.peer_id.clone();

    self.connections.insert(peer_id, connection);
}

pub fn remove_connection(
    &mut self,
    peer_id: &str,
) -> Option<PeerConnection> {
    self.connections.remove(peer_id)
}

pub fn get_connection(
    &self,
    peer_id: &str,
) -> Option<&PeerConnection> {
    self.connections.get(peer_id)
}

pub async fn send_to_peer(
    &mut self,
    peer_id: &str,
    message: &Message,
) -> Result<(), std::io::Error> {

    if let Some(connection)  = self.connections.get_mut(peer_id) {
        
        connection.send_message(message).await
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Peer not found",
        ))
    }

}

pub async fn receive_from_peer(
    &mut self,
    peer_id: &str,
) -> Result<Message, std::io::Error> {

    if let Some(connection)  = self.connections.get_mut(peer_id) {
        
        connection.receive_message().await
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Peer not found",
        ))
    }
}

pub async fn connect_to_peer(
    &mut self,
    peer_id: String,
    address: &str,
) -> Result<(), std::io::Error> {

    let stream = crate::network::tcp_client::connect_to_peer(address).await?;

    let connection = PeerConnection::new(peer_id.clone(), stream);

    self.add_connection(connection);

    Ok(())
}

pub fn disconnect_peer(
    &mut self,
    peer_id: &str,
) -> Option<PeerConnection> {

    self.remove_connection(peer_id)
}

pub async fn send_bitfield(
    &mut self,
    peer_id: &str,
    piece_manager: &crate::piece::PieceManager,
) -> Result<(), std::io::Error> {
    let bitfield = piece_manager.bitfield();

    let message = Message::Bitfield {
        bitfield,
    };

    self.send_to_peer(peer_id, &message).await
}

}