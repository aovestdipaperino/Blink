// Server modules
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
mod rest;

use crate::kafka::broker::{Broker, BROKER};

use crate::kafka::Kafka;
use crate::plugins::{initialize_plugin_manager, initialize_plugins, load_plugins};
use crate::server::rest::RestApi;
use crate::settings::SETTINGS;
use get_if_addrs::get_if_addrs;

use crate::kafka::shutdown::Shutdown;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::Server;
use once_cell::sync::Lazy;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{debug, error, info, trace};

pub const CARGO_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const BUILD: &str = include_str!("../resources/version.txt");
pub static VERSION: Lazy<String> = Lazy::new(|| {
    let version_parts: Vec<&str> = CARGO_VERSION.split('.').collect();
    let major_minor = format!("{}.{}", version_parts[0], version_parts[1]);
    format!("Cleafy Blink! v{}.{}", major_minor, BUILD).replace("\n", "")
});

pub struct BlinkServer {}
impl BlinkServer {
    fn get_local_ip() -> Option<IpAddr> {
        let if_addrs = get_if_addrs().ok()?;
        for if_addr in if_addrs {
            if if_addr.is_loopback() {
                continue;
            }
            return Some(if_addr.ip());
        }
        None
    }

    pub async fn run(settings_file: &str) {
        let boot_start = Instant::now();

        // Initialize settings before using them
        crate::settings::Settings::init(settings_file)
            .await
            .expect("Unable to load settings");

        // Initialize plugin system
        if !SETTINGS.plugin_paths.is_empty() {
            info!(
                "Initializing plugin system with {} plugins",
                SETTINGS.plugin_paths.len()
            );

            if let Err(e) = initialize_plugin_manager().await {
                error!("Failed to create plugin manager: {}", e);
            } else if let Err(e) = load_plugins(&SETTINGS.plugin_paths).await {
                error!("Failed to load plugins: {}", e);
            } else if let Err(e) = initialize_plugins().await {
                error!("Failed to initialize plugins: {}", e);
            } else {
                info!("Plugin system initialized successfully");
            }
        } else {
            info!("No plugins configured");
        }

        println!("{}", include_str!("../resources/logo.txt"));
        println!("\x1b[94mBlink is  not  Kafka!\x1b[39m");
        print!("Running {} ", VERSION.as_str());
        println!(
            "on {}:{:?} with: {:#?}",
            Self::get_local_ip().unwrap(),
            SETTINGS.broker_ports,
            *SETTINGS
        );
        let rest_addr: SocketAddr =
            SocketAddr::new(Ipv4Addr::new(0, 0, 0, 0).into(), SETTINGS.rest_port);

        // Create listeners for all broker ports
        let mut kafka_listeners = Vec::new();
        for &port in &SETTINGS.broker_ports {
            let kafka_protocol_addr: SocketAddr =
                SocketAddr::new(Ipv4Addr::new(0, 0, 0, 0).into(), port);
            let listener = TcpListener::bind(kafka_protocol_addr)
                .await
                .unwrap_or_else(|_| panic!("Unable to bind to port {}", port));
            kafka_listeners.push(listener);
        }

        let make_svc = make_service_fn(move |_conn: &AddrStream| async move {
            Ok::<_, hyper::Error>(service_fn(move |req| async move {
                RestApi::handle_request(req).await
            }))
        });

        let partitions = SETTINGS.kafka_cfg_num_partitions;
        if let Some(topic) = &SETTINGS.boot_topic {
            trace!("Creating the {topic} topic with {partitions} partitions");
            let _ = BROKER.create_topic(topic.to_string(), partitions as i32);
        }

        // Start background purge task
        trace!(
            "Starting background purge task (every {} seconds)",
            SETTINGS.purge_delay_seconds
        );
        Broker::start_background_purge_task();

        // Start idle time tracking task
        trace!("Starting idle time tracking task");
        Broker::start_idle_time_tracking_task();

        if SETTINGS.enable_consumer_groups {
            info!("Starting consumer group background tasks");
            Broker::start_consumer_group_tasks();
        }

        tokio::spawn(async move {
            if let Err(e) = Server::bind(&rest_addr).serve(make_svc).await {
                error!("Server error: {}", e);
            }
        });

        // Spawn a task for each listener
        for listener in kafka_listeners.into_iter() {
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(async move {
                trace!("Starting Kafka listener on port {}", port);
                loop {
                    let (sx, rx) = tokio::sync::broadcast::channel(1);
                    let shutdown = Shutdown::new(rx);

                    tokio::select! {
                        result = listener.accept() => {
                            match result {
                                Ok((client, _)) => {
                                    debug!("new kafka request from {:?} on port {}", client.peer_addr(), port);
                                    tokio::spawn(Kafka::handle_request(client, Arc::new(shutdown)));
                                }
                                Err(e) => {
                                    error!("Error accepting connection on port {}: {}", port, e);
                                    break;
                                }
                            }
                        }
                        _ = tokio::signal::ctrl_c() => {
                            trace!("Shutting down listener on port {}", port);
                            let _ = sx.send(());
                            break;
                        }
                    }
                }
            });
        }

        let boot_time = boot_start.elapsed();
        info!("Server started, boot time: {:.2?}", boot_time);

        // Main thread waits for Ctrl+C
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl+c");
        info!("Main server shutting down");
    }
}
