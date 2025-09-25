// Plugin system modules
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use anyhow::{anyhow, Result};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use wasmtime::component::*;
use wasmtime::{Config, Engine};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};

wasmtime::component::bindgen!({
    world: "blink-plugin",
    path: "src/resources",
    async: false,
});

use self::blink::plugin::host::{Host, LogLevel};

pub struct PluginManager {
    engine: Engine,
    plugins: Vec<PluginMetadata>,
}

struct PluginMetadata {
    name: String,
    path: String,
}

struct WasiPluginState {
    wasi: WasiCtx,
    table: ResourceTable,
}

impl WasiView for WasiPluginState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

// Implement the host logging function for plugins
impl Host for WasiPluginState {
    fn log(&mut self, level: LogLevel, message: String) {
        match level {
            LogLevel::Trace => tracing::trace!("🔌 Plugin: {}", message),
            LogLevel::Debug => tracing::debug!("🔌 Plugin: {}", message),
            LogLevel::Info => tracing::info!("🔌 Plugin: {}", message),
            LogLevel::Warn => tracing::warn!("🔌 Plugin: {}", message),
            LogLevel::Error => tracing::error!("🔌 Plugin: {}", message),
        }
    }
}

impl PluginManager {
    pub fn new() -> Result<Self> {
        // Create a WASM engine with component model support
        let mut config = Config::new();
        config.wasm_component_model(true);
        let engine =
            Engine::new(&config).map_err(|e| anyhow!("Failed to create WASM engine: {}", e))?;

        info!("Using real WASM plugins");

        Ok(Self {
            engine,
            plugins: Vec::new(),
        })
    }

    pub async fn load_plugins(&mut self, plugin_paths: &[String]) -> Result<()> {
        info!("Loading {} WASM plugins", plugin_paths.len());

        for path in plugin_paths {
            if Path::new(path).exists() {
                let plugin_name = Path::new(path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                self.plugins.push(PluginMetadata {
                    name: plugin_name.clone(),
                    path: path.clone(),
                });
                info!("Registered plugin: {} at {}", plugin_name, path);
            } else {
                error!("Plugin file does not exist: {}", path);
            }
        }

        info!("Registered {} plugins successfully", self.plugins.len());
        Ok(())
    }

    fn create_plugin_instance(
        &self,
        metadata: &PluginMetadata,
    ) -> Result<(wasmtime::Store<WasiPluginState>, BlinkPlugin)> {
        // Load the WASM component
        let component = Component::from_file(&self.engine, &metadata.path).map_err(|e| {
            anyhow!(
                "Failed to load WASM component from {}: {}",
                metadata.path,
                e
            )
        })?;

        // Create WASI context
        let wasi_ctx = WasiCtxBuilder::new().inherit_stdio().inherit_args().build();

        let state = WasiPluginState {
            wasi: wasi_ctx,
            table: ResourceTable::new(),
        };
        let mut store = wasmtime::Store::new(&self.engine, state);

        // Create a linker and add WASI
        let mut linker = Linker::new(&self.engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker)
            .map_err(|e| anyhow!("Failed to add WASI to linker: {}", e))?;

        // Add the host functions to the linker
        BlinkPlugin::add_to_linker(&mut linker, |state: &mut WasiPluginState| state)
            .map_err(|e| anyhow!("Failed to add host functions to linker: {}", e))?;

        // Instantiate the component
        let bindings = BlinkPlugin::instantiate(&mut store, &component, &linker)
            .map_err(|e| anyhow!("Failed to instantiate WASM component: {}", e))?;

        Ok((store, bindings))
    }

    pub async fn initialize_plugins(&self) -> Result<()> {
        info!("Initializing {} plugins", self.plugins.len());

        for metadata in &self.plugins {
            match self.create_plugin_instance(metadata) {
                Ok((mut store, bindings)) => {
                    match bindings.blink_plugin_plugin().call_init(&mut store) {
                        Ok(port) => {
                            if let Some(port_num) = port {
                                info!(
                                    "Plugin '{}' initialized with port: {}",
                                    metadata.name, port_num
                                );
                            } else {
                                info!("Plugin '{}' initialized successfully", metadata.name);
                            }
                        }
                        Err(e) => {
                            error!("Failed to initialize plugin '{}': {}", metadata.name, e);
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to create instance for plugin '{}': {}",
                        metadata.name, e
                    );
                }
            }
        }

        Ok(())
    }

    pub async fn on_record(&self, topic: &str, partition: u32, key: Option<&str>, value: &str) {
        for metadata in &self.plugins {
            match self.create_plugin_instance(metadata) {
                Ok((mut store, bindings)) => {
                    match bindings
                        .blink_plugin_plugin()
                        .call_on_record(&mut store, topic, partition, key, value)
                    {
                        Ok(should_continue) => {
                            if !should_continue {
                                debug!(
                                    "Plugin '{}' rejected record for topic: {}, partition: {}",
                                    metadata.name, topic, partition
                                );
                                // Note: For now we ignore the return value as specified
                                // In the future, this could be used to reject the record
                            }
                        }
                        Err(e) => {
                            warn!("Plugin '{}' failed to process record: {}", metadata.name, e);
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Plugin '{}' failed to create instance: {}",
                        metadata.name, e
                    );
                }
            }
        }
    }
}

// Global plugin manager instance
use once_cell::sync::Lazy;

pub static PLUGIN_MANAGER: Lazy<Arc<Mutex<Option<PluginManager>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

pub async fn initialize_plugin_manager() -> Result<()> {
    let manager = PluginManager::new()?;
    let mut guard = PLUGIN_MANAGER.lock().await;
    *guard = Some(manager);
    Ok(())
}

pub async fn load_plugins(plugin_paths: &[String]) -> Result<()> {
    let mut guard = PLUGIN_MANAGER.lock().await;
    if let Some(manager) = guard.as_mut() {
        manager.load_plugins(plugin_paths).await
    } else {
        Err(anyhow!("Plugin manager not initialized"))
    }
}

pub async fn initialize_plugins() -> Result<()> {
    let guard = PLUGIN_MANAGER.lock().await;
    if let Some(manager) = guard.as_ref() {
        manager.initialize_plugins().await
    } else {
        Err(anyhow!("Plugin manager not initialized"))
    }
}

pub async fn call_plugins_on_record(topic: &str, partition: u32, key: Option<&str>, value: &str) {
    let guard = PLUGIN_MANAGER.lock().await;
    if let Some(manager) = guard.as_ref() {
        manager.on_record(topic, partition, key, value).await;
    }
}
