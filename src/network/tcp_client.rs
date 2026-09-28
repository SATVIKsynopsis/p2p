use tokio::net::TcpStream;

pub async fn connect_to_peer(address: &str) -> Result<TcpStream, std::io::Error> {

    let stream = TcpStream::connect(address).await?;

    Ok(stream)

}