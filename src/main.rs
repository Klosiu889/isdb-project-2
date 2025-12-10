use clap::{Arg, Command};
use lib::Serializer;
use tokio::signal;

use crate::{
    consts::METASTORE_FILE,
    metastore::{load_metastore, save_metastore},
};
mod consts;
mod executor;
mod metastore;
mod planner;
mod query;
mod server;
mod utils;

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

    let addr = "0.0.0.0:8080";

    let serializer = Serializer::new();

    let metastore = load_metastore(METASTORE_FILE, &serializer).await;

    let server_handler = tokio::spawn(server::create(
        addr,
        matches.contains_id("https"),
        metastore.clone(),
    ));

    let shutdown_signal = async {
        let ctrl_c = signal::ctrl_c();

        #[cfg(unix)]
        let sigterm = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await
        };

        #[cfg(not(unix))]
        let sigterm = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => println!("Received Ctrl+C (SIGINT)"),
            _ = sigterm => println!("Received Docker Stop (SIGTERM)"),
        }
    };

    shutdown_signal.await;

    println!("Shutting down server, saving metastore...");
    save_metastore(metastore, METASTORE_FILE, &serializer).await;

    server_handler.abort();
    println!("Server Stopped.");
}
