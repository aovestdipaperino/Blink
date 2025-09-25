// Demo plugin implementation for Blink Kafka broker
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//

wit_bindgen::generate!({
    world: "blink-plugin",
    path: "../src/resources/blink-plugin.wit"
});

use blink::plugin::host::{log, LogLevel};
use exports::blink::plugin::plugin::Guest;

struct DemoPlugin;

impl Guest for DemoPlugin {
    fn init() -> Option<u16> {
        // Use host logging function to log to the host
        log(LogLevel::Info, "Hello world from Rust WASM plugin!");
        log(LogLevel::Debug, "Demo plugin initialization complete");

        // Return None to indicate no specific port
        None
    }

    fn on_record(topic: String, partition: u32, key: Option<String>, value: String) -> bool {
        // Log a dot for each record (visible in host logs)
        log(LogLevel::Trace, ".");

        // Log detailed record information at debug level
        let key_str = key.as_deref().unwrap_or("<none>");
        let message = format!(
            "Processing record: topic={}, partition={}, key={}, value_len={}",
            topic,
            partition,
            key_str,
            value.len()
        );
        log(LogLevel::Debug, &message);

        // Always return true to allow the record to be processed
        true
    }
}

// Export the plugin implementation
export!(DemoPlugin);
