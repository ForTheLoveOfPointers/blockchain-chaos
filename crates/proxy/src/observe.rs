//! Opt-in observation of what the proxy delivers to the client. The scenario
//! `test` command turns this on to evaluate assertions against the wire; plain
//! `proxy`/`run`/`cluster` leave it off, so there is no cost and no unbounded
//! growth on a long-lived proxy. Everything here is post-fault: the head sequence
//! and delivery outcomes are what the application actually saw.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::rpc::RpcView;

#[derive(Debug, Clone)]
pub struct Delivery {
    pub at: Duration,
    pub method: Option<String>,
    /// A fault was injected on this request (reject, timeout, or drop).
    pub faulted: bool,
    /// The client received a usable success (upstream 2xx and no injected error).
    pub ok: bool,
}

#[derive(Debug)]
pub struct Observations {
    start: Instant,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Delivered head values in delivery order, each with its elapsed offset.
    heads: Vec<(Duration, u64)>,
    deliveries: Vec<Delivery>,
}

impl Observations {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            inner: Mutex::new(Inner::default()),
        }
    }

    fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Record an HTTP response as delivered to the client. `faulted` marks an
    /// injected transport fault; `ok` marks a usable success.
    pub fn record_http(&self, view: &RpcView, delivered: &[u8], faulted: bool, ok: bool) {
        let method = single_method(view).map(str::to_owned);
        if ok {
            if let (Some("eth_blockNumber"), Some(head)) =
                (method.as_deref(), block_number_result(delivered))
            {
                self.push_head(head);
            }
        }
        self.push_delivery(Delivery {
            at: self.elapsed(),
            method,
            faulted,
            ok,
        });
    }

    /// Record a `newHeads` frame as delivered to the client (post-reorg rewrite).
    pub fn record_newhead(&self, frame: &str) {
        if let Some(head) = newhead_number(frame) {
            self.push_head(head);
        }
    }

    fn push_head(&self, head: u64) {
        let at = self.elapsed();
        self.inner.lock().unwrap().heads.push((at, head));
    }

    fn push_delivery(&self, d: Delivery) {
        self.inner.lock().unwrap().deliveries.push(d);
    }

    pub fn heads(&self) -> Vec<(Duration, u64)> {
        self.inner.lock().unwrap().heads.clone()
    }

    pub fn deliveries(&self) -> Vec<Delivery> {
        self.inner.lock().unwrap().deliveries.clone()
    }
}

impl Default for Observations {
    fn default() -> Self {
        Self::new()
    }
}

fn single_method(view: &RpcView) -> Option<&str> {
    match view {
        RpcView::Single(call) => call.method.as_deref(),
        _ => None,
    }
}

fn block_number_result(response: &[u8]) -> Option<u64> {
    let body: Value = serde_json::from_slice(response).ok()?;
    let hex = body.get("result")?.as_str()?;
    parse_hex_u64(hex)
}

fn newhead_number(frame: &str) -> Option<u64> {
    let v: Value = serde_json::from_str(frame).ok()?;
    if v.get("method").and_then(Value::as_str)? != "eth_subscription" {
        return None;
    }
    let hex = v.pointer("/params/result/number").and_then(Value::as_str)?;
    parse_hex_u64(hex)
}

fn parse_hex_u64(s: &str) -> Option<u64> {
    let trimmed = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(trimmed, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(method: &str) -> RpcView {
        RpcView::parse(format!(r#"{{"method":"{method}","id":1}}"#).as_bytes())
    }

    #[test]
    fn records_delivered_head_from_block_number() {
        let obs = Observations::new();
        obs.record_http(
            &view("eth_blockNumber"),
            br#"{"jsonrpc":"2.0","id":1,"result":"0x10"}"#,
            false,
            true,
        );
        let heads: Vec<u64> = obs.heads().into_iter().map(|(_, h)| h).collect();
        assert_eq!(heads, vec![0x10]);
    }

    #[test]
    fn ignores_head_on_faulted_delivery() {
        let obs = Observations::new();
        obs.record_http(&view("eth_blockNumber"), b"", true, false);
        assert!(obs.heads().is_empty());
        let d = obs.deliveries();
        assert_eq!(d.len(), 1);
        assert!(d[0].faulted && !d[0].ok);
    }

    #[test]
    fn records_head_from_newhead_frame() {
        let obs = Observations::new();
        let frame = r#"{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x1","result":{"number":"0x2a"}}}"#;
        obs.record_newhead(frame);
        let heads: Vec<u64> = obs.heads().into_iter().map(|(_, h)| h).collect();
        assert_eq!(heads, vec![0x2a]);
    }

    #[test]
    fn non_block_number_success_records_delivery_only() {
        let obs = Observations::new();
        obs.record_http(
            &view("eth_chainId"),
            br#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#,
            false,
            true,
        );
        assert!(obs.heads().is_empty());
        assert_eq!(obs.deliveries().len(), 1);
    }
}
