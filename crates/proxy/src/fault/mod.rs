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
//! The public surface is a [`FaultEngine`] plus [`FaultEngine::decide`], which
//! turns a [`FaultContext`] into a [`FaultDecision`] the choke points `match`
//! on.
//!
//! IMPORTANT: transport faults operate on the request *shape* only ([`RpcView`]:
//! method + id) and never decode blockchain semantics. Chain-aware faults
//! (stale heads, reorgs, missing logs) are a separate, stateful engine that
//! will sit behind this same seam in a later phase — keeping this layer
//! byte-oriented is what keeps that door open.
//!
//! Determinism caveat: the RNG is a single shared stream, so under concurrent
//! requests the *order* of draws is not deterministic and exact replays can
//! diverge. This is fine for single-client tests; a keyed per-request RNG is
//! the real fix and lands with the scenario engine.

pub mod config;
pub mod rng;
pub mod rule;

use std::time::Duration;

use crate::rpc::RpcView;

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

    fn resolve(&self, action: &Action) -> FaultDecision {
        match action {
            Action::Delay(spec) => FaultDecision::Delay(self.sample_delay(spec)),
            Action::Timeout(d) => FaultDecision::Timeout(*d),
            Action::Reject(spec) => FaultDecision::Reject(spec.clone()),
            Action::Drop => FaultDecision::Drop,
            Action::WsDisconnect(d) => FaultDecision::WsDisconnect(*d),
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
}
