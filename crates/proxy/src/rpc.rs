//! Lightweight, read-only views over JSON-RPC payloads.
//!
//! IMPORTANT: these are used for *logging only*. The proxy forwards the original
//! request/response bytes untouched, which is what guarantees the JSON-RPC `id`
//! (number, string, or null) is preserved exactly. Never forward a re-serialized
//! value produced from these views.

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct RpcCall {
    pub method: Option<String>,
    pub id: Option<Value>,
}

#[derive(Debug, Clone)]
pub enum RpcView {
    Single(RpcCall),
    Batch(Vec<RpcCall>),
    Unknown,
}

impl RpcView {
    pub fn parse(body: &[u8]) -> Self {
        match serde_json::from_slice::<Value>(body) {
            Ok(Value::Array(items)) => RpcView::Batch(items.iter().map(call_from_value).collect()),
            Ok(obj @ Value::Object(_)) => RpcView::Single(call_from_value(&obj)),
            _ => RpcView::Unknown,
        }
    }

    pub fn summary(&self) -> String {
        match self {
            RpcView::Single(c) => format!("{} id={}", method_str(c), id_str(c)),
            RpcView::Batch(calls) => {
                let methods: Vec<&str> = calls
                    .iter()
                    .map(|c| c.method.as_deref().unwrap_or("?"))
                    .collect();
                format!("batch[{}] {}", calls.len(), methods.join(","))
            }
            RpcView::Unknown => "<non-json-rpc>".to_string(),
        }
    }
}

fn call_from_value(v: &Value) -> RpcCall {
    RpcCall {
        method: v.get("method").and_then(Value::as_str).map(str::to_owned),
        id: v.get("id").cloned(),
    }
}

fn method_str(c: &RpcCall) -> &str {
    c.method.as_deref().unwrap_or("?")
}

fn id_str(c: &RpcCall) -> String {
    match &c.id {
        Some(id) => id.to_string(),
        None => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single() {
        let body = br#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}"#;
        match RpcView::parse(body) {
            RpcView::Single(c) => {
                assert_eq!(c.method.as_deref(), Some("eth_blockNumber"));
                assert_eq!(c.id, Some(serde_json::json!(1)));
            }
            other => panic!("expected single, got {other:?}"),
        }
    }

    #[test]
    fn parses_batch() {
        let body = br#"[{"method":"eth_chainId","id":"a"},{"method":"eth_blockNumber","id":2}]"#;
        match RpcView::parse(body) {
            RpcView::Batch(calls) => assert_eq!(calls.len(), 2),
            other => panic!("expected batch, got {other:?}"),
        }
    }

    #[test]
    fn unknown_for_garbage() {
        assert!(matches!(RpcView::parse(b"not json"), RpcView::Unknown));
    }
}
