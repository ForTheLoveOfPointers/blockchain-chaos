pub mod json_stdout;

use crate::config::EventLog;

pub trait Exporter: Send + Sync + std::fmt::Debug {
    fn export(&self, event: &EventLog);

    fn name(&self) -> &str;

    fn flush(&self) {}
}
