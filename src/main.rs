// Main entry point for Blink Kafka broker
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//

pub mod alloc;
mod kafka;
mod plugins;
mod profiling;
mod server;
mod settings;
mod util;

use crate::server::BlinkServer;
use crate::settings::Settings;
use clap::Parser;
use std::io;
use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "blink")]
#[command(about = "Blink - A Kafka-compatible message broker")]
#[command(version)]
struct Args {
    /// Path to the settings YAML file
    #[arg(short, long, default_value = "settings.yaml")]
    settings: String,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    // Load settings first to check if GCP logging is enabled
    let settings = Settings::load_from_path(&args.settings)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

    let level = std::env::var("LOG_LEVEL")
        .unwrap_or_else(|_| "INFO".to_string())
        .parse::<Level>()
        .unwrap_or(Level::INFO);

    // Initialize logging with optional GCP layer
    if settings.gcp_logging_enabled() {
        let project_id = settings.gcp_project_id().unwrap();
        match nano_gcp_logging::GcpLoggingLayer::new(project_id.clone()).await {
            Ok(gcp_layer) => {
                tracing_subscriber::registry()
                    .with(tracing_subscriber::fmt::layer())
                    .with(gcp_layer)
                    .with(tracing_subscriber::filter::LevelFilter::from_level(level))
                    .init();
                eprintln!("GCP logging enabled for project: {}", project_id);
            }
            Err(e) => {
                eprintln!(
                    "Failed to initialize GCP logging: {}. Falling back to console logging.",
                    e
                );
                tracing_subscriber::fmt().with_max_level(level).init();
            }
        }
    } else {
        tracing_subscriber::fmt().with_max_level(level).init();
    }

    // Initialize settings globally after we've loaded them
    Settings::init_with_settings(settings)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    BlinkServer::run(&args.settings).await;
    Ok(())
}
