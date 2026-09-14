use std::{collections::VecDeque, io::Write, sync::Mutex, time::SystemTime};

use chain_chaos_scenario::Scenario;

use crate::config::{EventDetail, EventLog, LoggingLevel, ObservabilityEngineConfig};

pub trait Exporter: Send + Sync {

    fn export(&self, event: &EventLog);

    fn name(&self) -> &str;

    fn flush(&self) {}
}

#[derive(Debug)]
struct InnerEvents {
    events: VecDeque<EventLog>,
}

#[derive(Debug)]
pub struct ObservabilityEngine {
    pub config: ObservabilityEngineConfig,
    inner: Mutex<InnerEvents>,
}

impl ObservabilityEngine {
    pub fn new(config: ObservabilityEngineConfig) -> Self {
        ObservabilityEngine {
            config,
            inner: Mutex::new(InnerEvents {
                events: VecDeque::new(),
            }),
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

        true
    }
}

impl Exporter for ObservabilityEngine  {
    fn export(&self, event: &EventLog) {
        if let Ok(line) = serde_json::to_string(event) {
            writeln!(std::io::stdout().lock(), "{line}").expect("Failed to writeln!");
        }
    }

    fn name(&self) -> &str {
        self.config.exporter[0].as_str()
    }

    fn flush(&self) {}
}