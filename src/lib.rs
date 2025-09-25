// Library module definitions for Blink Kafka broker
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//

pub mod alloc;
pub mod kafka;
pub mod plugins;
pub mod profiling;
pub mod server;
pub mod settings;
pub mod util;

use crate::server::BlinkServer;

pub async fn start() {
    BlinkServer::run("settings.yaml").await;
}
