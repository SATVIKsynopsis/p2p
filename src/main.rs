use p2p::dht::DhtTable;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let table = Arc::new(Mutex::new(DhtTable::new()));

    p2p::dht::server::start_dht_server(
        "127.0.0.1:7000",
        table,
    )
    .await?;

 
    Ok(())
}