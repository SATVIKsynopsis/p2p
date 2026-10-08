use p2p::dht::DhtTable;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let table = Arc::new(Mutex::new(DhtTable::new()));

    p2p::dht::server::start_dht_server("0.0.0.0:5001", table).await?;

    Ok(())
}