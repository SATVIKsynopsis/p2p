use p2p::{
    dht::{DhtTable, announce_pieces, join_dht, remove_peer, server::start_dht_server},
    network::tcp_server::serve_peer,
    peer::Peer,
    piece::PieceManager,
    transfer::Downloader,
};
use std::{env, sync::Arc};
use tokio::{net::TcpListener, sync::Mutex};

fn usage() {
    eprintln!(
        "Usage:\n  p2p dht-server [listen_addr]\n  p2p seed <peer_id> <listen_addr> <dht_addr> <file> <piece_size> [piece_ranges]\n  p2p download <peer_id> <dht_addr> <output> <total_pieces> <piece_size>"
    );
}

fn parse_piece_ranges(spec: &str, total: usize) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    if spec == "all" {
        return Ok((0..total).collect());
    }
    let mut selected = std::collections::BTreeSet::new();
    for item in spec.split(',') {
        if let Some((start, end)) = item.split_once('-') {
            let start: usize = start.parse()?;
            let end: usize = end.parse()?;
            if start > end || end >= total {
                return Err(format!("invalid piece range: {item}").into());
            }
            selected.extend(start..=end);
        } else {
            let index: usize = item.parse()?;
            if index >= total {
                return Err(format!("piece index out of range: {index}").into());
            }
            selected.insert(index);
        }
    }
    if selected.is_empty() {
        return Err("piece range selection cannot be empty".into());
    }
    Ok(selected.into_iter().collect())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("dht-server") => {
            let address = args.get(2).map(String::as_str).unwrap_or("0.0.0.0:5001");
            start_dht_server(address, Arc::new(Mutex::new(DhtTable::new()))).await?;
        }
        Some("seed") if (7..=8).contains(&args.len()) => {
            let peer_id = &args[2];
            let listen_addr = &args[3];
            let dht_addr = &args[4];
            let file = &args[5];
            let piece_size: usize = args[6].parse()?;
            if peer_id.trim().is_empty() || piece_size == 0 {
                return Err("peer ID and piece size must be valid".into());
            }
            let complete = PieceManager::try_from_file(file, piece_size)?;
            let selected = parse_piece_ranges(
                args.get(7).map(String::as_str).unwrap_or("all"),
                complete.total_pieces(),
            )?;
            let mut manager = PieceManager::new_empty(complete.total_pieces(), piece_size)?;
            for index in selected {
                manager.add_piece(complete.get_piece(index as u32).unwrap().clone())?;
            }
            let manager = Arc::new(manager);
            let listener = TcpListener::bind(listen_addr).await?;
            let actual_addr = listener.local_addr()?.to_string();
            join_dht(dht_addr, peer_id, &actual_addr).await?;
            if let Err(error) = announce_pieces(dht_addr, peer_id, &actual_addr, &manager).await {
                let _ = remove_peer(dht_addr, peer_id).await;
                return Err(error.into());
            }
            println!(
                "Peer {peer_id} seeding {} of {} pieces at {actual_addr}",
                manager
                    .bitfield()
                    .iter()
                    .map(|byte| byte.count_ones() as usize)
                    .sum::<usize>(),
                manager.total_pieces(),
            );
           
            let heartbeat_dht = dht_addr.clone();
            let heartbeat_peer = peer_id.clone();
            let heartbeat_addr = actual_addr.clone();
            let heartbeat_manager = Arc::clone(&manager);
            let heartbeat = tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if let Err(error) =
                        join_dht(&heartbeat_dht, &heartbeat_peer, &heartbeat_addr).await
                    {
                        eprintln!("DHT lease refresh failed: {error}");
                        continue;
                    }
                    if let Err(error) = announce_pieces(
                        &heartbeat_dht,
                        &heartbeat_peer,
                        &heartbeat_addr,
                        &heartbeat_manager,
                    )
                    .await
                    {
                        eprintln!("DHT piece refresh failed: {error}");
                    }
                }
            }); 
            let serve_result = tokio::select! {
                result = serve_peer(listener, peer_id.clone(), manager) => result,
                signal = tokio::signal::ctrl_c() => signal,
            };
            heartbeat.abort();
            let _ = heartbeat.await;
            let cleanup_result = remove_peer(dht_addr, peer_id).await;
            serve_result?;
            cleanup_result?;
        }
        Some("download") if args.len() == 7 => {
            let peer_id = &args[2];
            let dht_addr = &args[3];
            let output = &args[4];
            let total_pieces: usize = args[5].parse()?;
            let piece_size: usize = args[6].parse()?;
            let manager = PieceManager::new_empty(total_pieces, piece_size)?;
            let peer = Peer::new(
                peer_id.clone(),
                PieceManager::new_empty(total_pieces, piece_size)?,
            );
            let connections = peer
                .discover_connections_for_missing_pieces(dht_addr, &manager)
                .await?;
            if connections.is_empty() {
                return Err("DHT returned no reachable peers".into());
            }
            let downloader = Arc::new(Downloader::new(manager));
            downloader
                .clone()
                .download_concurrently(connections)
                .await?;
            downloader.reassemble(output).await?;
            println!("Download complete: {output}");
        }
        _ => {
            usage();
            return Err("invalid command or arguments".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_piece_ranges;

    #[test]
    fn parses_piece_range_lists_and_rejects_out_of_range_values() {
        assert_eq!(parse_piece_ranges("0-2,4,4", 6).unwrap(), vec![0, 1, 2, 4]);
        assert_eq!(parse_piece_ranges("all", 3).unwrap(), vec![0, 1, 2]);
        assert!(parse_piece_ranges("2-8", 6).is_err());
        assert!(parse_piece_ranges("", 6).is_err());
    }
}
