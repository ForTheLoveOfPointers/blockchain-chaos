//! Assertions evaluated against what the proxy delivered to the client. These are
//! wire-observable invariants only (head monotonicity, recovery, catch-up); an
//! assertion about the application's own state would need the application's
//! cooperation and is out of scope here.

use std::collections::BTreeMap;
use std::time::Duration;

use chain_chaos_proxy::observe::{Delivery, Observations};
use serde::Deserialize;

use crate::ScenarioError;

const DEFAULT_WITHIN: Duration = Duration::from_secs(5);

/// How many blocks the delivered head may trail the true tip and still count as
/// caught up. The upstream tip is snapshotted at the window close, so the only gap
/// is the client's own polling lag: one block of slack covers it.
const CATCH_UP_SLACK: u64 = 1;

/// A parsed, validated assertion ready to evaluate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Assertion {
    /// The delivered head never jumps backward.
    HeadMonotonic,
    /// After the final `recover`, deliveries succeed again within the window.
    EventualRecovery { within: Duration },
    /// After the final `recover`, the delivered head reaches the upstream head
    /// (within one block) within the window.
    CatchesUp { within: Duration },
}

/// Wire form: either a bare name (`head_monotonic`) or a single-key map carrying
/// parameters (`catches_up: { within: 5s }`).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AssertionConfig {
    Simple(String),
    Mapped(BTreeMap<String, AssertionParams>),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssertionParams {
    pub within: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AssertionOutcome {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

impl AssertionConfig {
    pub fn compile(&self, index: usize) -> Result<Assertion, ScenarioError> {
        let (name, params) = match self {
            AssertionConfig::Simple(name) => (name.as_str(), AssertionParams::default()),
            AssertionConfig::Mapped(map) => {
                if map.len() != 1 {
                    return Err(assertion_err(
                        index,
                        "an assertion map must have exactly one key",
                    ));
                }
                let (k, v) = map.iter().next().expect("len checked");
                (k.as_str(), v.clone())
            }
        };

        let within = match &params.within {
            Some(s) => humantime::parse_duration(s).map_err(|e| {
                assertion_err(index, format!("invalid `within` (`{s}`): {e}"))
            })?,
            None => DEFAULT_WITHIN,
        };

        match name {
            "head_monotonic" => Ok(Assertion::HeadMonotonic),
            "eventual_recovery" => Ok(Assertion::EventualRecovery { within }),
            "catches_up" => Ok(Assertion::CatchesUp { within }),
            other => Err(assertion_err(
                index,
                format!(
                    "unknown assertion `{other}` (expected head_monotonic, eventual_recovery, catches_up)"
                ),
            )),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            AssertionConfig::Simple(name) => name,
            AssertionConfig::Mapped(map) => {
                map.keys().next().map(String::as_str).unwrap_or("<empty>")
            }
        }
    }
}

/// Ground truth gathered at evaluation time, kept separate from [`Observations`]
/// so evaluation stays a pure function that is easy to unit-test.
#[derive(Debug, Clone, Default)]
pub struct Ground {
    /// The real upstream head, queried directly at the end of the run.
    pub upstream_head: Option<u64>,
    /// When the scenario's final `recover` fired, if any.
    pub recover_at: Option<Duration>,
}

/// Evaluate a compiled timeline's assertions against observed traffic.
pub fn evaluate_timeline(
    timeline: &crate::Timeline,
    obs: &Observations,
    ground: &Ground,
) -> Vec<AssertionOutcome> {
    evaluate(&timeline.assertions, obs, ground)
}

pub fn evaluate(
    assertions: &[Assertion],
    obs: &Observations,
    ground: &Ground,
) -> Vec<AssertionOutcome> {
    let heads = obs.heads();
    let deliveries = obs.deliveries();
    let base = ground.recover_at.unwrap_or(Duration::ZERO);

    assertions
        .iter()
        .map(|a| match a {
            Assertion::HeadMonotonic => head_monotonic(&heads),
            Assertion::EventualRecovery { within } => {
                eventual_recovery(&deliveries, base, *within)
            }
            Assertion::CatchesUp { within } => {
                catches_up(&heads, ground.upstream_head, base, *within)
            }
        })
        .collect()
}

fn head_monotonic(heads: &[(Duration, u64)]) -> AssertionOutcome {
    let mut max = 0u64;
    let mut seen = false;
    for (at, h) in heads {
        if seen && *h < max {
            return fail(
                "head_monotonic",
                format!(
                    "delivered head went backward to {h} (below prior max {max}) at {}",
                    humantime::format_duration(*at)
                ),
            );
        }
        max = max.max(*h);
        seen = true;
    }
    if !seen {
        return fail("head_monotonic", "no head was delivered".to_string());
    }
    pass("head_monotonic", format!("{} heads, peak {max}", heads.len()))
}

fn eventual_recovery(
    deliveries: &[Delivery],
    base: Duration,
    within: Duration,
) -> AssertionOutcome {
    if deliveries.is_empty() {
        return fail("eventual_recovery", "no deliveries observed".to_string());
    }
    let last_ok = deliveries.last().map(|d| d.ok).unwrap_or(false);
    let first_ok_after = deliveries
        .iter()
        .find(|d| d.at >= base && d.ok)
        .map(|d| d.at);

    match (last_ok, first_ok_after) {
        (true, Some(at)) if at.saturating_sub(base) <= within => pass(
            "eventual_recovery",
            format!(
                "recovered {} after the window opened",
                humantime::format_duration(at.saturating_sub(base))
            ),
        ),
        (true, Some(at)) => fail(
            "eventual_recovery",
            format!(
                "first success came {} after recovery, over the {} budget",
                humantime::format_duration(at.saturating_sub(base)),
                humantime::format_duration(within)
            ),
        ),
        (false, _) => fail(
            "eventual_recovery",
            "the final delivery to the client was still an error".to_string(),
        ),
        (true, None) => fail(
            "eventual_recovery",
            "no successful delivery after the recovery window opened".to_string(),
        ),
    }
}

fn catches_up(
    heads: &[(Duration, u64)],
    upstream_head: Option<u64>,
    base: Duration,
    within: Duration,
) -> AssertionOutcome {
    let Some(target) = upstream_head else {
        return fail(
            "catches_up",
            "could not read the upstream head to compare against".to_string(),
        );
    };
    let deadline = base + within;
    let best = heads
        .iter()
        .filter(|(at, _)| *at <= deadline)
        .map(|(_, h)| *h)
        .max();

    match best {
        Some(h) if h + CATCH_UP_SLACK >= target => pass(
            "catches_up",
            format!("delivered head reached {h} vs upstream {target}"),
        ),
        Some(h) => fail(
            "catches_up",
            format!(
                "delivered head only reached {h} within the window, upstream was {target}"
            ),
        ),
        None => fail(
            "catches_up",
            "no head was delivered within the catch-up window".to_string(),
        ),
    }
}

fn pass(name: &str, detail: String) -> AssertionOutcome {
    AssertionOutcome {
        name: name.to_string(),
        passed: true,
        detail,
    }
}

fn fail(name: &str, detail: String) -> AssertionOutcome {
    AssertionOutcome {
        name: name.to_string(),
        passed: false,
        detail,
    }
}

fn assertion_err(index: usize, msg: impl Into<String>) -> ScenarioError {
    ScenarioError::Event {
        index,
        msg: format!("assertion[{index}]: {}", msg.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs_with(heads: &[u64]) -> Observations {
        let obs = Observations::new();
        for h in heads {
            obs.record_http(
                &chain_chaos_proxy::rpc::RpcView::parse(br#"{"method":"eth_blockNumber","id":1}"#),
                format!(r#"{{"result":"0x{h:x}"}}"#).as_bytes(),
                false,
                true,
            );
        }
        obs
    }

    #[test]
    fn head_monotonic_passes_on_rising_heads() {
        let obs = obs_with(&[1, 2, 2, 3, 5]);
        let out = evaluate(&[Assertion::HeadMonotonic], &obs, &Ground::default());
        assert!(out[0].passed, "{}", out[0].detail);
    }

    #[test]
    fn head_monotonic_fails_on_backward_jump() {
        let obs = obs_with(&[10, 11, 8]);
        let out = evaluate(&[Assertion::HeadMonotonic], &obs, &Ground::default());
        assert!(!out[0].passed);
        assert!(out[0].detail.contains("backward"), "{}", out[0].detail);
    }

    #[test]
    fn catches_up_passes_within_one_block() {
        let obs = obs_with(&[5, 9]);
        let ground = Ground {
            upstream_head: Some(10),
            recover_at: None,
        };
        let out = evaluate(
            &[Assertion::CatchesUp {
                within: Duration::from_secs(60),
            }],
            &obs,
            &ground,
        );
        assert!(out[0].passed, "{}", out[0].detail);
    }

    #[test]
    fn catches_up_fails_when_stuck_behind() {
        let obs = obs_with(&[5, 6]);
        let ground = Ground {
            upstream_head: Some(20),
            recover_at: None,
        };
        let out = evaluate(
            &[Assertion::CatchesUp {
                within: Duration::from_secs(60),
            }],
            &obs,
            &ground,
        );
        assert!(!out[0].passed, "{}", out[0].detail);
    }

    #[test]
    fn compiles_named_and_mapped_forms() {
        let simple = AssertionConfig::Simple("head_monotonic".to_string());
        assert_eq!(simple.compile(0).unwrap(), Assertion::HeadMonotonic);

        let mut map = BTreeMap::new();
        map.insert(
            "catches_up".to_string(),
            AssertionParams {
                within: Some("3s".to_string()),
            },
        );
        let mapped = AssertionConfig::Mapped(map);
        assert_eq!(
            mapped.compile(0).unwrap(),
            Assertion::CatchesUp {
                within: Duration::from_secs(3)
            }
        );
    }

    #[test]
    fn rejects_unknown_assertion() {
        let cfg = AssertionConfig::Simple("no_such_thing".to_string());
        assert!(cfg.compile(0).is_err());
    }
}
