use crate::{peer::PeerConnection, piece::PieceManager, protocol::Message, transfer::Uploader};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};

#[derive(Clone, Copy, Debug)]
pub struct FairnessConfig {
    pub max_unchoked_peers: usize,
    
    pub min_shared_pieces: usize,
    pub reconsider_every: std::time::Duration,
}

impl Default for FairnessConfig {
    fn default() -> Self {
        Self {
            max_unchoked_peers: 4,
            min_shared_pieces: 1,
            reconsider_every: std::time::Duration::from_secs(5),
        }
    }
}

struct PeerSession {
    shared_pieces: HashSet<u32>,
    unchoked: bool,
    announced: bool,
    control: mpsc::UnboundedSender<bool>,
}

type PeerSessions = Arc<Mutex<HashMap<String, PeerSession>>>;

async fn refresh_unchoked(sessions: &PeerSessions, config: FairnessConfig, rotation: usize) {
    let mut sessions = sessions.lock().await;
    let mut ranked: Vec<_> = sessions
        .iter()
        .map(|(id, state)| (id.clone(), state.shared_pieces.len()))
        .collect();
    let mut tie_order: Vec<_> = ranked.iter().map(|(id, _)| id.clone()).collect();
    tie_order.sort();
    if !tie_order.is_empty() {
        let offset = rotation % tie_order.len();
        tie_order.rotate_left(offset);
    }
    let tie_rank: HashMap<_, _> = tie_order
        .into_iter()
        .enumerate()
        .map(|(rank, id)| (id, rank))
        .collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then(tie_rank[&a.0].cmp(&tie_rank[&b.0])));
    let minimum = config.min_shared_pieces.max(1);
    let contributors: Vec<_> = ranked
        .iter()
        .filter(|(_, score)| *score >= minimum)
        .collect();
    let selected: HashSet<_> = if contributors.is_empty() {
        ranked
            .into_iter()
            .take(config.max_unchoked_peers)
            .map(|(id, _)| id)
            .collect()
    } else {
        contributors
            .into_iter()
            .take(config.max_unchoked_peers)
            .map(|(id, _)| id.clone())
            .collect()
    };
    for (id, state) in sessions.iter_mut() {
        let should_unchoke = selected.contains(id);
        if !state.announced || should_unchoke != state.unchoked {
            state.unchoked = should_unchoke;
            state.announced = true;
            let _ = state.control.send(should_unchoke);
        }
    }
}

pub async fn start_server(address: &str) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(address).await?;

    println!("Server listening on {}", address);

    loop {
        let (socket, addr) = listener.accept().await?;

        println!("New connection from {}", addr);

        tokio::spawn(async move {
            
            let _socket = socket;
        });
    }
}


pub async fn serve_peer(
    listener: TcpListener,
    peer_id: String,
    piece_manager: Arc<PieceManager>,
) -> Result<(), std::io::Error> {
    serve_peer_with_fairness(listener, peer_id, piece_manager, FairnessConfig::default()).await
}

pub async fn serve_peer_with_fairness(
    listener: TcpListener,
    peer_id: String,
    piece_manager: Arc<PieceManager>,
    fairness: FairnessConfig,
) -> Result<(), std::io::Error> {
    let sessions: PeerSessions = Arc::new(Mutex::new(HashMap::new()));
    let scheduler_sessions = Arc::clone(&sessions);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(fairness.reconsider_every);
        let mut rotation = 0usize;
        loop {
            interval.tick().await;
            refresh_unchoked(&scheduler_sessions, fairness, rotation).await;
            rotation = rotation.wrapping_add(1);
        }
    });
    loop {
        let (stream, _) = listener.accept().await?;
        let peer_id = peer_id.clone();
        let manager = Arc::clone(&piece_manager);
        let sessions = Arc::clone(&sessions);
        tokio::spawn(async move {
            let uploader = Uploader::new((*manager).clone());
            let mut connection = PeerConnection::new(String::new(), stream);
            if connection.handshake(&peer_id).await.is_err()
                || connection
                    .exchange_bitfield(uploader.piece_manager.bitfield())
                    .await
                    .is_err()
            {
                return;
            }
            let remote_id = connection.peer_id.clone();
            let (control_tx, mut control_rx) = mpsc::unbounded_channel();
            sessions.lock().await.insert(
                remote_id.clone(),
                PeerSession {
                    shared_pieces: HashSet::new(),
                    unchoked: false,
                    announced: false,
                    control: control_tx,
                },
            );
            refresh_unchoked(&sessions, fairness, 0).await;
            let Some(initial_unchoke) = control_rx.recv().await else {
                return;
            };
            let initial_message = if initial_unchoke {
                Message::Unchoke
            } else {
                Message::Choke
            };
            if connection.send_message(&initial_message).await.is_err() {
                return;
            }
            let mut locally_unchoked = initial_unchoke;
            loop {
                tokio::select! {
                    command = control_rx.recv() => {
                        let Some(unchoke) = command else { break; };
                        let result = if unchoke { connection.send_unchoke().await } else { connection.send_choke().await };
                        if result.is_err() { break; }
                        locally_unchoked = unchoke;
                    }
                    incoming = connection.receive_message() => match incoming {
                        Ok(Message::Request { piece_index }) => {
                            if locally_unchoked {
                                if uploader.serve_request(&mut connection, piece_index).await.is_err() { break; }
                                connection.successful_requests += 1;
                            }
                        }
                        Ok(Message::Have { piece_index }) => {
                            if piece_index as usize >= uploader.piece_manager.total_pieces() { break; }
                            connection.update_remote_piece(piece_index);
                            if let Some(session) = sessions.lock().await.get_mut(&remote_id) {
                                session.shared_pieces.insert(piece_index);
                            }
                        }
                        Ok(Message::Choke) => connection.remote_unchoked = false,
                        Ok(Message::Unchoke) => connection.remote_unchoked = true,
                        Ok(Message::Ping) => { if connection.send_message(&Message::Pong).await.is_err() { break; } }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            }
            sessions.lock().await.remove(&remote_id);
            refresh_unchoked(&sessions, fairness, 0).await;
        });
    }
}
