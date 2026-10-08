use tokio::net::TcpListener;
use tokio::net::TcpStream;

pub async fn start_server(address: &str) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(address).await?;

    println!("Server listening on {}", address);

    loop {
        let (socket, addr) = listener.accept().await?;

        println!("New connection from {}", addr);

        tokio::spawn(async move {
            // handeled later
            let _socket = socket;
        });
    }
}
