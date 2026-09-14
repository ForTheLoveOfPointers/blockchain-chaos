//! Shared server state passed to every handler.

use std::sync::Arc;

use tracing::info;

use crate::config::ProxyConfig;
use crate::fault::{FaultEngine, FaultObserver};
use crate::observe::Observations;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<ProxyConfig>,
    pub http_client: reqwest::Client,
    pub fault: Arc<FaultEngine>,
    /// Present only when a caller (the `test` command) wants delivered traffic
    /// recorded for assertion evaluation. `None` for a plain proxy.
    pub observe: Option<Arc<Observations>>,
    /// Optional sink notified of every fault decision (e.g. the observability
    /// engine). `None` leaves the proxy silent, as before.
    pub fault_observer: Option<Arc<dyn FaultObserver>>,
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
            observe: None,
            fault_observer: None,
        })
    }

    /// Attach an observation recorder so delivered traffic is captured for
    /// assertion evaluation.
    pub fn with_observations(mut self, obs: Arc<Observations>) -> Self {
        self.observe = Some(obs);
        self
    }

    /// Attach a fault-decision observer so each decision is reported to an
    /// external sink (e.g. the observability engine).
    pub fn with_fault_observer(mut self, observer: Arc<dyn FaultObserver>) -> Self {
        self.fault_observer = Some(observer);
        self
    }
}
