use std::time::SystemTime;

use chain_chaos_proxy::fault::FaultDecision;
use chain_chaos_scenario::{config::EventConfig, Scenario};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum LoggingLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityEngineConfig {
    pub enabled: bool,
    pub level: LoggingLevel,
    pub buffer: u32,
    pub exporter: Vec<String>, // Vendors like Prometheus, OTel, etc.
}

#[derive(Debug, Serialize)]
pub struct EventLog {
    pub timestamp: SystemTime,
    pub scenario: Option<Scenario>,
    pub detail: EventDetail,
}

#[derive(Debug, Serialize)]
pub enum EventDetail {
    Decision(FaultDecision),
    Action(chain_chaos_proxy::fault::Action),
    ScenarioEvent(EventConfig),
}
