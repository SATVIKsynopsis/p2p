use p2p::{
    peer::{Peer, PeerConnection},
    piece::PieceManager,
    protocol::Message,
};
use tokio::net::TcpListener;

#[tokio::test]
async fn handshake_and_bitfield_exchange() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let remote = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut c = PeerConnection::new(String::new(), stream);
        c.handshake("remote").await.unwrap();
        c.exchange_bitfield(vec![0b10]).await.unwrap();
        assert_eq!(c.peer_id, "local");
    });
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut c = PeerConnection::new("expected".into(), stream);
    c.handshake("local").await.unwrap();
    assert_eq!(c.peer_id, "remote");
    c.exchange_bitfield(vec![1]).await.unwrap();
    assert!(c.has_remote_piece(1));
    remote.await.unwrap();
}

#[tokio::test]
async fn invalid_handshake_is_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let remote = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = p2p::protocol::codec::read_message(&mut stream)
            .await
            .unwrap();
        p2p::protocol::codec::write_message(&mut stream, &Message::Ping)
            .await
            .unwrap();
    });
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut c = PeerConnection::new("unknown".into(), stream);
    assert!(c.handshake("local").await.is_err());
    remote.await.unwrap();
}

#[tokio::test]
async fn empty_remote_peer_id_is_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let remote = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = p2p::protocol::codec::read_message(&mut stream)
            .await
            .unwrap();
        p2p::protocol::codec::write_message(
            &mut stream,
            &Message::Handshake {
                peer_id: "   ".into(),
            },
        )
        .await
        .unwrap();
    });
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut connection = PeerConnection::new("unverified".into(), stream);
    assert!(connection.handshake("local").await.is_err());
    remote.await.unwrap();
}

#[tokio::test]
async fn contribution_policy_prefers_contributors_and_choke_blocks_upload() {
    let manager = PieceManager::new_empty(1, 1).unwrap();
    let mut peer = Peer::new("local".into(), manager);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
    let (stream, _) = listener.accept().await.unwrap();
    let a = PeerConnection::new("a".into(), stream);
    let bstream = client.await.unwrap();
    let b = PeerConnection::new("b".into(), bstream);
    peer.add_connection(a);
    peer.add_connection(b);
    peer.connections.get_mut("a").unwrap().downloaded_pieces = 1;
    assert_eq!(peer.select_unchoked(1), vec!["a"]);
    let conn = peer.connections.get("b").unwrap();
    assert!(!conn.locally_unchoked);
}

#[tokio::test]
async fn invalid_have_index_is_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let remote = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        p2p::protocol::codec::write_message(&mut stream, &Message::Have { piece_index: 9 })
            .await
            .unwrap();
    });
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut c = PeerConnection::new("peer".into(), stream);
    c.bitfield = Some(vec![0]);
    assert!(c.receive_have().await.is_err());
    remote.await.unwrap();
}

#[tokio::test]
async fn have_propagates_and_announced_piece_can_be_requested() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let piece = p2p::piece::new_piece(3, b"piece-three".to_vec());
    let mut owned = PieceManager::new_empty(4, 20).unwrap();
    owned.add_piece(piece.clone()).unwrap();
    let manager_for_server = owned.clone();
    let remote = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let a = PeerConnection::new("b".into(), stream);
        let mut peer_a = Peer::new("a".into(), manager_for_server);
        peer_a.add_connection(a);
        peer_a.announce_have(3, &owned).await.unwrap();
        let mut connection = peer_a.connections.remove("b").unwrap();
        assert!(matches!(
            connection.receive_message().await.unwrap(),
            Message::Request { piece_index: 3 }
        ));
        peer_a
            .uploader
            .serve_request(&mut connection, 3)
            .await
            .unwrap();
    });
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut b = PeerConnection::new("a".into(), stream);
    b.bitfield = Some(vec![0]);
    
    b.receive_have().await.unwrap();
    assert!(b.has_remote_piece(3));
    let downloader = p2p::transfer::Downloader::new(PieceManager::new_empty(4, 20).unwrap());
    downloader.request_piece(&mut b, 3).await.unwrap();
    let response = b.receive_message().await.unwrap();
    if let Message::Piece {
        piece_index,
        data,
        hash,
    } = response
    {
        downloader
            .receive_piece(piece_index, data, hash)
            .await
            .unwrap();
        assert!(downloader.piece_manager.lock().await.has_piece(3));
    } else {
        panic!("expected requested piece");
    }
    remote.await.unwrap();
}

#[tokio::test]
async fn downloader_uses_have_for_future_piece_selection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut seeder = PieceManager::new_empty(1, 1).unwrap();
    seeder
        .add_piece(p2p::piece::new_piece(0, vec![42]))
        .unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = PeerConnection::new(String::new(), stream);
        connection.handshake("late-seeder").await.unwrap();
        connection.exchange_bitfield(vec![0]).await.unwrap();
        connection.send_message(&Message::Unchoke).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        connection
            .send_message(&Message::Have { piece_index: 0 })
            .await
            .unwrap();
        assert!(matches!(
            connection.receive_message().await.unwrap(),
            Message::Request { piece_index: 0 }
        ));
        p2p::transfer::Uploader::new(seeder)
            .serve_request(&mut connection, 0)
            .await
            .unwrap();
    });

    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut connection = PeerConnection::new(String::new(), stream);
    connection.handshake("late-downloader").await.unwrap();
    connection.exchange_bitfield(vec![0]).await.unwrap();
    connection.receive_choke_state().await.unwrap();
    let downloader = std::sync::Arc::new(p2p::transfer::Downloader::new(
        PieceManager::new_empty(1, 1).unwrap(),
    ));
    downloader
        .clone()
        .download_concurrently(vec![connection])
        .await
        .unwrap();
    assert!(downloader.piece_manager.lock().await.has_piece(0));
    server.await.unwrap();
}

#[tokio::test]
async fn have_is_broadcast_to_every_connected_peer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let observer = tokio::spawn(async move {
        let mut saw = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            match p2p::protocol::codec::read_message(&mut stream)
                .await
                .unwrap()
            {
                Message::Have { piece_index: 3 } => saw.push(true),
                other => panic!("expected Have(3), got {other:?}"),
            }
        }
        saw
    });
    let manager_streams = [
        tokio::net::TcpStream::connect(address).await.unwrap(),
        tokio::net::TcpStream::connect(address).await.unwrap(),
    ];
    let piece = p2p::piece::new_piece(3, b"owned".to_vec());
    let mut manager = PieceManager::new_empty(4, 8).unwrap();
    manager.add_piece(piece).unwrap();
    let mut peer = Peer::new("owner".into(), manager.clone());
    for (index, stream) in manager_streams.into_iter().enumerate() {
        peer.add_connection(PeerConnection::new(format!("peer-{index}"), stream));
    }
    peer.announce_have(3, &manager).await.unwrap();
    assert_eq!(observer.await.unwrap(), vec![true, true]);
    assert!(peer.announce_have(2, &manager).await.is_err());
}

#[tokio::test]
async fn choke_and_unchoke_are_sent_and_enforced() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut c = PeerConnection::new("client".into(), stream);
        assert!(matches!(c.receive_message().await.unwrap(), Message::Choke));
        c.send_message(&Message::Choke).await.unwrap();
        c.send_message(&Message::Unchoke).await.unwrap();
    });
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut c = PeerConnection::new("server".into(), stream);
    c.send_choke().await.unwrap();
    assert!(!c.locally_unchoked);
    assert!(
        p2p::transfer::Uploader::new(PieceManager::new_empty(1, 1).unwrap())
            .serve_request(&mut c, 0)
            .await
            .is_err()
    );
    assert!(matches!(c.receive_message().await.unwrap(), Message::Choke));
    c.remote_unchoked = false;
    assert!(
        p2p::transfer::Downloader::new(PieceManager::new_empty(1, 1).unwrap())
            .request_piece(&mut c, 0)
            .await
            .is_err()
    );
    assert!(matches!(
        c.receive_message().await.unwrap(),
        Message::Unchoke
    ));
    c.remote_unchoked = true;
    c.send_unchoke().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn server_periodically_unchokes_the_peer_sharing_pieces() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut seed_manager = PieceManager::new_empty(4, 1).unwrap();
    seed_manager
        .add_piece(p2p::piece::new_piece(0, vec![1]))
        .unwrap();
    let server = tokio::spawn(p2p::network::tcp_server::serve_peer_with_fairness(
        listener,
        "seed".into(),
        std::sync::Arc::new(seed_manager),
        p2p::network::tcp_server::FairnessConfig {
            max_unchoked_peers: 1,
            min_shared_pieces: 1,
            reconsider_every: std::time::Duration::from_millis(25),
        },
    ));

    async fn connect(id: &str, address: std::net::SocketAddr) -> PeerConnection {
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut connection = PeerConnection::new(String::new(), stream);
        connection.handshake(id).await.unwrap();
        connection.exchange_bitfield(vec![0]).await.unwrap();
        connection.receive_choke_state().await.unwrap();
        connection
    }

    let mut contributor = connect("z-contributor", address).await;
    assert!(contributor.remote_unchoked);
    let mut non_contributor = connect("a-idle", address).await;
    assert!(non_contributor.remote_unchoked);

    
    assert!(matches!(
        contributor.receive_message().await.unwrap(),
        Message::Choke
    ));
    contributor.send_have(3).await.unwrap();
    let promoted = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        contributor.receive_message(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(promoted, Message::Unchoke));
    let demoted = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        non_contributor.receive_message(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(demoted, Message::Choke));
    server.abort();
}
