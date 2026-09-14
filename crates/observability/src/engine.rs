use std::{collections::VecDeque, sync::Mutex, time::SystemTime};

use chain_chaos_proxy::fault::{FaultContext, FaultDecision, FaultObserver};
use chain_chaos_scenario::Scenario;

use crate::{
    config::{EventDetail, EventLog, LoggingLevel, ObservabilityEngineConfig},
    exporters::{json_stdout::JsonStdout, Exporter},
};

#[derive(Debug)]
struct InnerEvents {
    events: VecDeque<EventLog>,
}

#[derive(Debug)]
pub struct ObservabilityEngine {
    pub config: ObservabilityEngineConfig,
    inner: Mutex<InnerEvents>,
    exporters: Vec<Box<dyn Exporter>>,
}

/// Turns the configured exporter names into live sinks.
/// Unknown names are skipped with a warning rather than failing startup.
fn build_exporters(config: &ObservabilityEngineConfig) -> Vec<Box<dyn Exporter>> {
    config
        .exporter
        .iter()
        .filter_map(|name| -> Option<Box<dyn Exporter>> {
            match name.as_str() {
                "json-stdout" => Some(Box::new(JsonStdout::new())),
                other => {
                    eprintln!("observability: unknown exporter {other:?}, skipping");
                    None
                }
            }
        })
        .collect()
}

impl ObservabilityEngine {
    pub fn new(config: ObservabilityEngineConfig) -> Self {
        let exporters = build_exporters(&config);
        ObservabilityEngine {
            config,
            inner: Mutex::new(InnerEvents {
                events: VecDeque::new(),
            }),
            exporters,
        }
    }

    pub fn record(
        &self,
        level: LoggingLevel,
        scenario: Option<Scenario>,
        detail: EventDetail,
    ) -> bool {
        if !self.config.enabled || level < self.config.level {
            return false;
        }

        let mut inner = self.inner.lock().unwrap();

        if inner.events.len() >= self.config.buffer.try_into().unwrap() {
            inner.events.pop_front(); // drops oldest entry
        }

        let log = EventLog {
            timestamp: SystemTime::now(),
            detail,
            scenario,
        };

        inner.events.push_back(log);

        // Fan out the just-recorded event to every configured sink.
        if let Some(event) = inner.events.back() {
            for exporter in &self.exporters {
                exporter.export(event);
            }
        }

        true
    }
}

impl FaultObserver for ObservabilityEngine {
    /// Bridges a proxy fault decision into an observability event. Runs on the
    /// request hot path, so it only records (buffer + export) and returns; the
    /// `record` short-circuits cheaply when the engine is disabled.
    fn on_decision(&self, _ctx: &FaultContext, decision: &FaultDecision) {
        self.record(
            LoggingLevel::Info,
            None,
            EventDetail::Decision(decision.clone()),
        );
    }
}
