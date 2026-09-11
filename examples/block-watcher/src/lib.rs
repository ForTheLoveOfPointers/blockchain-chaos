//! A minimal EVM block watcher — the first example application for chain-chaos.
//!
//! It does the smallest useful thing: poll `eth_blockNumber` on an interval,
//! track the head, and notice when the head jumps (a gap) or moves backwards (a
//! possible reorg). Crucially, it is *resilient*: an RPC failure is counted and
//! the next tick retries, rather than crashing the watcher. That resilience is
//! exactly what chain-chaos exists to test — point this at a faulted proxy and
//! assert it still converges on the true head.
//!
//! Deliberately HTTP-polling (not WebSocket subscriptions) to keep the example
//! and its chaos test simple and deterministic.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;
use tracing::{info, warn};

#[derive(Debug, Default, Clone)]
pub struct WatchState {
    pub head: Option<u64>,
    pub highest: u64,
    pub polls: u64,
    pub errors: u64,
    pub gaps: u64,
    pub regressions: u64,
}

#[derive(Debug, Clone)]
pub struct WatchConfig {
    pub rpc_url: String,
    pub poll_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum PollError {
    #[error("request failed: {0}")]
    Request(String),
    #[error("upstream returned status {0}")]
    Status(u16),
    #[error("no usable result in response: {0}")]
    BadBody(String),
}

pub struct Watcher {
    cfg: WatchConfig,
    client: reqwest::Client,
    state: Arc<Mutex<WatchState>>,
}

impl Watcher {
    pub fn new(cfg: WatchConfig) -> Self {
        Self {
            cfg,
            client: reqwest::Client::new(),
            state: Arc::new(Mutex::new(WatchState::default())),
        }
    }

    pub fn state_handle(&self) -> Arc<Mutex<WatchState>> {
        Arc::clone(&self.state)
    }

    pub fn snapshot(&self) -> WatchState {
        self.state.lock().unwrap().clone()
    }

    pub async fn poll_once(&self) -> Result<u64, PollError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
        });
        let resp = self
            .client
            .post(&self.cfg.rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| PollError::Request(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(PollError::Status(resp.status().as_u16()));
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| PollError::BadBody(e.to_string()))?;
        let hex = json
            .get("result")
            .and_then(|v| v.as_str())
            .ok_or_else(|| PollError::BadBody(json.to_string()))?;
        parse_hex_u64(hex).ok_or_else(|| PollError::BadBody(format!("bad hex: {hex}")))
    }

    fn record(&self, outcome: Result<u64, PollError>) {
        let mut s = self.state.lock().unwrap();
        s.polls += 1;
        match outcome {
            Ok(head) => {
                if s.highest > 0 {
                    if head > s.highest + 1 {
                        s.gaps += 1;
                    } else if head < s.highest {
                        s.regressions += 1;
                    }
                }
                s.head = Some(head);
                s.highest = s.highest.max(head);
            }
            Err(_) => s.errors += 1,
        }
    }

    pub async fn run_for(&self, total: Duration) {
        let deadline = Instant::now() + total;
        loop {
            let outcome = self.poll_once().await;
            match &outcome {
                Ok(head) => info!(target: "block_watcher", head, "observed head"),
                Err(e) => warn!(target: "block_watcher", error = %e, "poll failed; will retry"),
            }
            self.record(outcome);

            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let sleep = self.cfg.poll_interval.min(deadline - now);
            tokio::time::sleep(sleep).await;
        }
    }

    pub async fn run_forever(&self) {
        loop {
            let outcome = self.poll_once().await;
            match &outcome {
                Ok(head) => info!(target: "block_watcher", head, "observed head"),
                Err(e) => warn!(target: "block_watcher", error = %e, "poll failed; will retry"),
            }
            self.record(outcome);
            tokio::time::sleep(self.cfg.poll_interval).await;
        }
    }
}

fn parse_hex_u64(s: &str) -> Option<u64> {
    let trimmed = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(trimmed, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn watcher() -> Watcher {
        Watcher::new(WatchConfig {
            rpc_url: "http://127.0.0.1:1".to_string(),
            poll_interval: Duration::from_millis(10),
        })
    }

    #[test]
    fn parses_hex() {
        assert_eq!(parse_hex_u64("0x0"), Some(0));
        assert_eq!(parse_hex_u64("0x1a"), Some(26));
        assert_eq!(parse_hex_u64("10"), Some(16));
        assert_eq!(parse_hex_u64("0xzz"), None);
    }

    #[test]
    fn detects_gap() {
        let w = watcher();
        w.record(Ok(1));
        w.record(Ok(5));
        let s = w.snapshot();
        assert_eq!(s.gaps, 1);
        assert_eq!(s.highest, 5);
    }

    #[test]
    fn detects_regression() {
        let w = watcher();
        w.record(Ok(5));
        w.record(Ok(3));
        let s = w.snapshot();
        assert_eq!(s.regressions, 1);
        assert_eq!(s.highest, 5, "highest stays monotonic");
    }

    #[test]
    fn counts_errors_without_advancing_head() {
        let w = watcher();
        w.record(Ok(2));
        w.record(Err(PollError::Status(429)));
        let s = w.snapshot();
        assert_eq!(s.errors, 1);
        assert_eq!(s.head, Some(2), "head unchanged by an error");
        assert_eq!(s.polls, 2);
    }

    #[test]
    fn sequential_heads_are_not_gaps() {
        let w = watcher();
        for h in 1..=5 {
            w.record(Ok(h));
        }
        let s = w.snapshot();
        assert_eq!(s.gaps, 0);
        assert_eq!(s.regressions, 0);
        assert_eq!(s.highest, 5);
    }
}
