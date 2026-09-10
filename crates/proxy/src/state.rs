//! Shared server state passed to every handler.

use std::sync::Arc;

use crate::config::ProxyConfig;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<ProxyConfig>,
    pub http_client: reqwest::Client,
}

impl AppState {
    pub fn new(cfg: ProxyConfig) -> anyhow::Result<Self> {
        // No total timeout: pass-through must be transparent; timeouts are a
        // fault we inject deliberately in Phase 2.
        let http_client = reqwest::Client::builder().build()?;
        Ok(Self {
            cfg: Arc::new(cfg),
            http_client,
        })
    }
}
