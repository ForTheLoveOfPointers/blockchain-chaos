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
    /// Names of the exporters to enable (e.g. "json-stdout", "prometheus").
    /// The engine turns these into live `Box<dyn Exporter>` sinks at startup.
    pub exporter: Vec<String>,
}

impl Default for ObservabilityEngineConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            level: LoggingLevel::Info,
            buffer: 1024,
            exporter: vec![String::from("json-stdout")],
        }
    }
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
    // Boxed: `EventConfig` is ~536 bytes, far larger than the other variants, so
    // an unboxed variant bloats every buffered `EventLog`. Box keeps the enum
    // small (and satisfies `clippy::large_enum_variant`).
    ScenarioEvent(Box<EventConfig>),
}
