use clap::{Arg, Command};
use lib::Serializer;
use tokio::signal;

use crate::metastore::{load_metastore, save_metastore};
mod executor;
mod metastore;
mod planner;
mod query;
mod server;

const METASTORE_FILE: &str = "metastore.json";

#[tokio::main]
async fn main() {
    env_logger::init();

    let matches = Command::new("server")
        .arg(
            Arg::new("https")
                .long("https")
                .help("Whether to use HTTPS or not"),
        )
        .get_matches();

    let addr = "127.0.0.1:8080";

    let serializer = Serializer::new();

    let metastore = load_metastore(METASTORE_FILE, &serializer).await;

    let server_handler = tokio::spawn(server::create(
        addr,
        matches.contains_id("https"),
        metastore.clone(),
    ));

    signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");

    println!("Shutting down server, saving metastore...");
    save_metastore(metastore, METASTORE_FILE, &serializer).await;

    server_handler.abort();
}
