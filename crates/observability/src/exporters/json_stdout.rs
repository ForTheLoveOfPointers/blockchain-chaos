use std::io::Write;

use crate::config::EventLog;
use crate::exporters::Exporter;

#[derive(Debug)]
pub struct JsonStdout {
    name: String,
}

impl JsonStdout {
    pub fn new() -> Self {
        JsonStdout {
            name: String::from("json-stdout"),
        }
    }
}

impl Default for JsonStdout {
    fn default() -> Self {
        Self::new()
    }
}

impl Exporter for JsonStdout {
    fn export(&self, event: &EventLog) {
        let line = match serde_json::to_string(event) {
            Ok(line) => line,
            Err(err) => {
                eprintln!("[{}] failed to serialize event: {err}", self.name);
                return;
            }
        };

        let stdout = std::io::stdout();
        let mut handle = stdout.lock();

        if let Err(err) = writeln!(handle, "{line}") {
            eprintln!("[{}] failed to write event: {err}", self.name);
        }
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn flush(&self) {
        // stdout is line-buffered and each `writeln!` already emits a newline,
        // so there is nothing to force out. Kept for trait completeness.
    }
}
