//! Shared server state passed to every handler.

use std::sync::Arc;

use tracing::info;

use crate::config::ProxyConfig;
use crate::fault::FaultEngine;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<ProxyConfig>,
    pub http_client: reqwest::Client,
    pub fault: Arc<FaultEngine>,
}

impl AppState {
    pub fn new(cfg: ProxyConfig) -> anyhow::Result<Self> {
        let fault = Arc::new(cfg.faults.clone().compile()?);
        if fault.is_empty() {
            info!(target: "chain_chaos", "no fault rules, running as pass-through");
        } else {
            info!(
                target: "chain_chaos",
                seed = fault.seed(),
                rules = fault.rule_count(),
                "fault engine ready (reproduce with this seed)"
            );
        }
        Self::with_engine(cfg, fault)
    }

    pub fn with_engine(cfg: ProxyConfig, fault: Arc<FaultEngine>) -> anyhow::Result<Self> {
        let http_client = reqwest::Client::builder().build()?;
        Ok(Self {
            cfg: Arc::new(cfg),
            http_client,
            fault,
        })
    }
}
