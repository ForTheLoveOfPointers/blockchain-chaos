//! Transport fault injection.
//!
//! This module is the fault engine that the two forwarding choke points
//! ([`crate::http::forward`] and [`crate::ws`] relay) consult before touching
//! the upstream. Layout:
//!
//! - [`config`] — serde/wire types deserialized from the proxy TOML, plus the
//!   step that compiles them into the runtime engine. Keep the wire format here
//!   so it can evolve without disturbing the hot path.
//! - [`rule`] — the runtime matcher/action types and the matching logic.
//! - [`rng`] — the seeded, deterministic RNG.
//!
//! The engine runs in two phases. [`FaultEngine::decide`] runs *before*
//! forwarding and turns a [`FaultContext`] into a [`FaultDecision`] for
//! request-side transport faults (delay, timeout, reject, drop, ws-disconnect).
//! [`FaultEngine::intercept`] runs *after* forwarding and rewrites the upstream
//! response for chain-aware faults (stale head, missing logs, malformed, reorg).
//! Each rule belongs to exactly one phase, distinguished by
//! [`Action::is_response`], so a rule's probability is rolled once in the phase
//! that owns it.
//!
//! Most chain-aware faults read and rewrite the JSON-RPC result but hold no
//! cross-request state. The [`reorg`] fault is the exception: it carries an
//! `Arc`-shared model so the fork point stays pinned across a reorg window, and
//! the WS relay consults [`FaultEngine::active_reorg`] to rewrite `newHeads`
//! frames the same way.
//!
//! Determinism caveat: the RNG is a single shared stream, so under concurrent
//! requests the *order* of draws is not deterministic and exact replays can
//! diverge. This is fine for single-client tests; a keyed per-request RNG is
//! the real fix.

pub mod config;
pub mod reorg;
pub mod rng;
pub mod rule;

use std::time::Duration;

use serde_json::Value;

use crate::rpc::RpcView;

pub use reorg::ReorgHandle;
use rng::FaultRng;
pub use rule::{Action, DelaySpec, Matcher, RejectSpec, Rule, Transport, TransportMatch};

pub struct FaultEngine {
    seed: u64,
    rules: std::sync::RwLock<Vec<Rule>>,
    rng: FaultRng,
}

impl std::fmt::Debug for FaultEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultEngine")
            .field("seed", &self.seed)
            .field("rules", &*self.rules.read().unwrap())
            .finish_non_exhaustive()
    }
}

pub struct FaultContext<'a> {
    pub transport: Transport,
    pub view: &'a RpcView,
}

#[derive(Debug, Clone)]
pub enum FaultDecision {
    Pass,
    Delay(Duration),
    Timeout(Duration),
    Reject(RejectSpec),
    Drop,
    WsDisconnect(Duration),
}

impl FaultEngine {
    pub fn new(seed: u64, rules: Vec<Rule>) -> Self {
        Self {
            seed,
            rules: std::sync::RwLock::new(rules),
            rng: FaultRng::from_seed(seed),
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn rule_count(&self) -> usize {
        self.rules.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.read().unwrap().is_empty()
    }

    pub fn set_rules(&self, rules: Vec<Rule>) {
        *self.rules.write().unwrap() = rules;
    }

    pub fn decide(&self, ctx: &FaultContext) -> FaultDecision {
        let rules = self.rules.read().unwrap();
        for rule in rules.iter() {
            if rule.action.is_response() {
                continue;
            }
            if !rule.matcher.matches(ctx.transport, ctx.view) {
                continue;
            }
            if !self.rng.chance(rule.probability) {
                continue;
            }
            return self.resolve(&rule.action);
        }
        FaultDecision::Pass
    }

    pub fn intercept(&self, ctx: &FaultContext, response: &[u8]) -> Option<Vec<u8>> {
        let method = single_method(ctx.view);
        let rules = self.rules.read().unwrap();
        for rule in rules.iter() {
            if !rule.action.is_response() {
                continue;
            }
            if !rule.matcher.matches(ctx.transport, ctx.view) {
                continue;
            }
            if !self.rng.chance(rule.probability) {
                continue;
            }
            return apply_response(&rule.action, method, response);
        }
        None
    }

    pub fn active_reorg(&self) -> Option<ReorgHandle> {
        let rules = self.rules.read().unwrap();
        rules.iter().find_map(|rule| match &rule.action {
            Action::Reorg(handle) => Some(handle.clone()),
            _ => None,
        })
    }

    fn resolve(&self, action: &Action) -> FaultDecision {
        match action {
            Action::Delay(spec) => FaultDecision::Delay(self.sample_delay(spec)),
            Action::Timeout(d) => FaultDecision::Timeout(*d),
            Action::Reject(spec) => FaultDecision::Reject(spec.clone()),
            Action::Drop => FaultDecision::Drop,
            Action::WsDisconnect(d) => FaultDecision::WsDisconnect(*d),
            Action::StaleHead { .. }
            | Action::MissingLogs
            | Action::Malformed
            | Action::Reorg(_) => FaultDecision::Pass,
        }
    }

    fn sample_delay(&self, spec: &DelaySpec) -> Duration {
        match spec {
            DelaySpec::Fixed(d) => *d,
            DelaySpec::Range { min, max } => {
                let lo = min.as_millis() as u64;
                let hi = max.as_millis() as u64;
                Duration::from_millis(self.rng.range_ms(lo, hi))
            }
        }
    }
}

fn single_method(view: &RpcView) -> Option<&str> {
    match view {
        RpcView::Single(call) => call.method.as_deref(),
        _ => None,
    }
}

fn apply_response(action: &Action, method: Option<&str>, response: &[u8]) -> Option<Vec<u8>> {
    if let Action::Reorg(handle) = action {
        return handle.rewrite(method, response);
    }
    let mut body: Value = serde_json::from_slice(response).ok()?;
    let obj = body.as_object_mut()?;
    if !obj.contains_key("result") {
        return None;
    }
    match action {
        Action::StaleHead { lag } => {
            let hex = obj.get("result").and_then(Value::as_str)?.to_owned();
            let head = parse_hex_u64(&hex)?;
            let stale = head.saturating_sub(*lag);
            obj.insert("result".to_string(), Value::String(format!("0x{stale:x}")));
        }
        Action::MissingLogs => {
            obj.insert("result".to_string(), Value::Array(Vec::new()));
        }
        Action::Malformed => {
            obj.insert("result".to_string(), Value::String("0xZZ".to_string()));
        }
        _ => return None,
    }
    serde_json::to_vec(&body).ok()
}

fn parse_hex_u64(s: &str) -> Option<u64> {
    let trimmed = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(trimmed, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::config::FaultConfig;

    fn engine(toml_src: &str) -> FaultEngine {
        let cfg: FaultConfig = toml::from_str(toml_src).expect("valid toml");
        cfg.compile().expect("compiles")
    }

    fn view(method: &str) -> RpcView {
        RpcView::parse(format!(r#"{{"method":"{method}","id":1}}"#).as_bytes())
    }

    #[test]
    fn passes_when_no_rule_matches() {
        let e = engine("[[rules]]\nmethods = [\"eth_call\"]\ndelay = \"1s\"\n");
        let d = e.decide(&FaultContext {
            transport: Transport::Http,
            view: &view("eth_blockNumber"),
        });
        assert!(matches!(d, FaultDecision::Pass));
    }

    #[test]
    fn fires_fixed_delay_on_match() {
        let e = engine("[[rules]]\nmethods = [\"eth_getLogs\"]\ndelay = \"2s\"\n");
        let d = e.decide(&FaultContext {
            transport: Transport::Http,
            view: &view("eth_getLogs"),
        });
        assert!(matches!(d, FaultDecision::Delay(dur) if dur == Duration::from_secs(2)));
    }

    #[test]
    fn zero_probability_never_fires() {
        let e = engine("[[rules]]\nprobability = 0.0\ndrop = true\n");
        for _ in 0..50 {
            let d = e.decide(&FaultContext {
                transport: Transport::Http,
                view: &view("eth_call"),
            });
            assert!(matches!(d, FaultDecision::Pass));
        }
    }

    #[test]
    fn range_delay_stays_within_bounds() {
        let e = engine("[[rules]]\ndelay = { min = \"100ms\", max = \"300ms\" }\n");
        for _ in 0..50 {
            match e.decide(&FaultContext {
                transport: Transport::Http,
                view: &view("eth_call"),
            }) {
                FaultDecision::Delay(d) => {
                    assert!(d >= Duration::from_millis(100) && d < Duration::from_millis(300));
                }
                other => panic!("expected delay, got {other:?}"),
            }
        }
    }

    #[test]
    fn same_seed_is_reproducible() {
        let src = "seed = 99\n[[rules]]\nprobability = 0.5\ndrop = true\n";
        let a = engine(src);
        let b = engine(src);
        for _ in 0..100 {
            let da = a.decide(&FaultContext {
                transport: Transport::Http,
                view: &view("eth_call"),
            });
            let db = b.decide(&FaultContext {
                transport: Transport::Http,
                view: &view("eth_call"),
            });
            assert_eq!(
                matches!(da, FaultDecision::Drop),
                matches!(db, FaultDecision::Drop),
            );
        }
    }

    fn response_rule(methods: &[&str], action: Action) -> Rule {
        Rule {
            name: None,
            matcher: Matcher {
                methods: Some(methods.iter().map(|m| m.to_string()).collect()),
                transport: TransportMatch::Http,
            },
            probability: 1.0,
            action,
        }
    }

    fn intercept(engine: &FaultEngine, method: &str, response: &[u8]) -> Option<serde_json::Value> {
        engine
            .intercept(
                &FaultContext {
                    transport: Transport::Http,
                    view: &view(method),
                },
                response,
            )
            .map(|bytes| serde_json::from_slice(&bytes).unwrap())
    }

    #[test]
    fn intercept_stale_head_rewrites_block_number() {
        let e = FaultEngine::new(
            1,
            vec![response_rule(
                &["eth_blockNumber"],
                Action::StaleHead { lag: 3 },
            )],
        );
        let resp = br#"{"jsonrpc":"2.0","id":1,"result":"0x10"}"#;
        let v = intercept(&e, "eth_blockNumber", resp).expect("rewritten");
        assert_eq!(v["result"], "0xd");
        assert_eq!(v["id"], 1);
    }

    #[test]
    fn intercept_missing_logs_empties_result() {
        let e = FaultEngine::new(
            1,
            vec![response_rule(&["eth_getLogs"], Action::MissingLogs)],
        );
        let resp = br#"{"jsonrpc":"2.0","id":2,"result":[{"blockNumber":"0x1"}]}"#;
        let v = intercept(&e, "eth_getLogs", resp).expect("rewritten");
        assert_eq!(v["result"], serde_json::json!([]));
    }

    #[test]
    fn intercept_malformed_corrupts_result() {
        let e = FaultEngine::new(1, vec![response_rule(&["eth_call"], Action::Malformed)]);
        let resp = br#"{"jsonrpc":"2.0","id":3,"result":"0x1"}"#;
        let v = intercept(&e, "eth_call", resp).expect("rewritten");
        assert_eq!(v["result"], "0xZZ");
    }

    #[test]
    fn intercept_passes_through_unmatched_method() {
        let e = FaultEngine::new(
            1,
            vec![response_rule(
                &["eth_blockNumber"],
                Action::StaleHead { lag: 1 },
            )],
        );
        let resp = br#"{"jsonrpc":"2.0","id":4,"result":"0x5"}"#;
        assert!(intercept(&e, "eth_chainId", resp).is_none());
    }

    #[test]
    fn decide_ignores_response_actions() {
        let e = FaultEngine::new(
            1,
            vec![response_rule(
                &["eth_blockNumber"],
                Action::StaleHead { lag: 1 },
            )],
        );
        let d = e.decide(&FaultContext {
            transport: Transport::Http,
            view: &view("eth_blockNumber"),
        });
        assert!(matches!(d, FaultDecision::Pass));
    }
}
