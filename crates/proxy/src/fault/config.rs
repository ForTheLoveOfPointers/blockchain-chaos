//! Wire types for the `[faults]` TOML, and the step that compiles them into the
//! runtime [`super::rule`] types. The only place that knows TOML field names.

use std::time::Duration;

use serde::Deserialize;

use crate::config::ConfigError;

use super::reorg::ReorgHandle;
use super::rule::{Action, DelaySpec, Matcher, RejectSpec, Rule, TransportMatch};
use super::FaultEngine;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultConfig {
    pub seed: Option<u64>,
    #[serde(default)]
    pub rules: Vec<RuleConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleConfig {
    pub name: Option<String>,

    pub methods: Option<Vec<String>>,
    #[serde(default)]
    pub transport: TransportConfig,
    #[serde(default = "one")]
    pub probability: f64,

    pub delay: Option<DelayConfig>,
    pub timeout: Option<String>,
    pub reject: Option<RejectConfig>,
    #[serde(default)]
    pub drop: bool,
    pub ws_disconnect_after: Option<String>,
    pub stale_head: Option<u64>,
    #[serde(default)]
    pub missing_logs: bool,
    #[serde(default)]
    pub malformed: bool,
    pub reorg: Option<ReorgConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReorgConfig {
    pub depth: u64,
    pub remove_logs: Option<bool>,
    pub drop_transactions: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportConfig {
    Http,
    Ws,
    #[default]
    Both,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum DelayConfig {
    Fixed(String),
    Range { min: String, max: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejectConfig {
    pub http_status: Option<u16>,
    pub code: Option<i64>,
    pub message: Option<String>,
}

fn one() -> f64 {
    1.0
}

impl FaultConfig {
    pub fn compile(self) -> Result<FaultEngine, ConfigError> {
        let seed = self.seed.unwrap_or_else(generate_seed);
        let rules = self
            .rules
            .into_iter()
            .enumerate()
            .map(|(i, rc)| rc.compile(i, seed))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FaultEngine::new(seed, rules))
    }
}

impl RuleConfig {
    fn compile(self, index: usize, seed: u64) -> Result<Rule, ConfigError> {
        let label = self
            .name
            .clone()
            .unwrap_or_else(|| format!("rule[{index}]"));

        if !(0.0..=1.0).contains(&self.probability) {
            return Err(fault_err(
                &label,
                format!("probability {} is outside 0.0..=1.0", self.probability),
            ));
        }

        let action = self.collapse_action(&label, seed)?;
        let methods = self.methods.or_else(|| default_methods(&action));
        let matcher = Matcher {
            methods,
            transport: self.transport.into(),
        };

        Ok(Rule {
            name: self.name,
            matcher,
            probability: self.probability,
            action,
        })
    }

    fn collapse_action(&self, label: &str, seed: u64) -> Result<Action, ConfigError> {
        let mut actions: Vec<Action> = Vec::new();

        if let Some(delay) = &self.delay {
            actions.push(Action::Delay(delay.compile(label)?));
        }
        if let Some(timeout) = &self.timeout {
            actions.push(Action::Timeout(parse_duration(label, "timeout", timeout)?));
        }
        if let Some(reject) = &self.reject {
            actions.push(Action::Reject(reject.compile()));
        }
        if self.drop {
            actions.push(Action::Drop);
        }
        if let Some(after) = &self.ws_disconnect_after {
            actions.push(Action::WsDisconnect(parse_duration(
                label,
                "ws_disconnect_after",
                after,
            )?));
        }
        if let Some(lag) = self.stale_head {
            actions.push(Action::StaleHead { lag });
        }
        if self.missing_logs {
            actions.push(Action::MissingLogs);
        }
        if self.malformed {
            actions.push(Action::Malformed);
        }
        if let Some(reorg) = &self.reorg {
            actions.push(Action::Reorg(ReorgHandle::new(
                seed,
                reorg.depth,
                reorg.remove_logs.unwrap_or(true),
                reorg.drop_transactions.unwrap_or(true),
            )));
        }

        match actions.len() {
            0 => Err(fault_err(
                label,
                "no action set (expected one of: delay, timeout, reject, drop, ws_disconnect_after, stale_head, missing_logs, malformed, reorg)"
                    .to_string(),
            )),
            1 => Ok(actions.pop().expect("len checked")),
            n => Err(fault_err(
                label,
                format!("{n} actions set, expected exactly one"),
            )),
        }
    }
}

impl DelayConfig {
    fn compile(&self, label: &str) -> Result<DelaySpec, ConfigError> {
        match self {
            DelayConfig::Fixed(s) => Ok(DelaySpec::Fixed(parse_duration(label, "delay", s)?)),
            DelayConfig::Range { min, max } => {
                let min = parse_duration(label, "delay.min", min)?;
                let max = parse_duration(label, "delay.max", max)?;
                if max < min {
                    return Err(fault_err(
                        label,
                        format!("delay.min ({min:?}) is greater than delay.max ({max:?})"),
                    ));
                }
                Ok(DelaySpec::Range { min, max })
            }
        }
    }
}

impl RejectConfig {
    fn compile(&self) -> RejectSpec {
        RejectSpec {
            http_status: self.http_status.unwrap_or(200),
            code: self.code.unwrap_or(-32000),
            message: self
                .message
                .clone()
                .unwrap_or_else(|| "fault injected".to_string()),
        }
    }
}

impl From<TransportConfig> for TransportMatch {
    fn from(t: TransportConfig) -> Self {
        match t {
            TransportConfig::Http => TransportMatch::Http,
            TransportConfig::Ws => TransportMatch::Ws,
            TransportConfig::Both => TransportMatch::Both,
        }
    }
}

fn parse_duration(label: &str, field: &str, s: &str) -> Result<Duration, ConfigError> {
    humantime::parse_duration(s).map_err(|e| {
        fault_err(
            label,
            format!("invalid duration for `{field}` (`{s}`): {e}"),
        )
    })
}

fn default_methods(action: &Action) -> Option<Vec<String>> {
    match action {
        Action::StaleHead { .. } => Some(vec!["eth_blockNumber".to_string()]),
        Action::MissingLogs => Some(vec!["eth_getLogs".to_string()]),
        _ => None,
    }
}

fn fault_err(label: &str, msg: String) -> ConfigError {
    ConfigError::Fault(format!("{label}: {msg}"))
}

fn generate_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compile_toml(s: &str) -> Result<FaultEngine, ConfigError> {
        let cfg: FaultConfig = toml::from_str(s).expect("valid toml");
        cfg.compile()
    }

    #[test]
    fn compiles_a_full_config() {
        let engine = compile_toml(
            r#"
            seed = 7

            [[rules]]
            name = "slow getLogs"
            methods = ["eth_getLogs"]
            delay = "2s"

            [[rules]]
            methods = ["eth_call"]
            probability = 0.5
            delay = { min = "100ms", max = "1500ms" }

            [[rules]]
            methods = ["eth_sendRawTransaction"]
            reject = { http_status = 429, code = -32005, message = "rate limited" }

            [[rules]]
            transport = "ws"
            ws_disconnect_after = "30s"
            "#,
        )
        .expect("should compile");
        assert_eq!(engine.seed(), 7);
        assert_eq!(engine.rule_count(), 4);
    }

    #[test]
    fn rejects_rule_with_no_action() {
        let err = compile_toml("[[rules]]\nmethods = [\"eth_call\"]\n").unwrap_err();
        assert!(err.to_string().contains("no action"), "{err}");
    }

    #[test]
    fn rejects_rule_with_two_actions() {
        let err = compile_toml("[[rules]]\ndelay = \"1s\"\ndrop = true\n").unwrap_err();
        assert!(err.to_string().contains("expected exactly one"), "{err}");
    }

    #[test]
    fn rejects_out_of_range_probability() {
        let err = compile_toml("[[rules]]\nprobability = 1.5\ndrop = true\n").unwrap_err();
        assert!(err.to_string().contains("probability"), "{err}");
    }

    #[test]
    fn rejects_inverted_delay_range() {
        let err = compile_toml("[[rules]]\ndelay = { min = \"2s\", max = \"1s\" }\n").unwrap_err();
        assert!(err.to_string().contains("greater than"), "{err}");
    }

    #[test]
    fn compiles_chain_faults() {
        let engine = compile_toml(
            "[[rules]]\nstale_head = 3\n\n[[rules]]\nmissing_logs = true\n\n[[rules]]\nmethods = [\"eth_call\"]\nmalformed = true\n",
        )
        .expect("should compile");
        assert_eq!(engine.rule_count(), 3);
    }

    #[test]
    fn rejects_rule_with_transport_and_chain_action() {
        let err = compile_toml("[[rules]]\nstale_head = 1\ndrop = true\n").unwrap_err();
        assert!(err.to_string().contains("expected exactly one"), "{err}");
    }

    #[test]
    fn compiles_reorg_fault() {
        let engine =
            compile_toml("seed = 3\n[[rules]]\nreorg = { depth = 2, remove_logs = false }\n")
                .expect("should compile");
        assert_eq!(engine.rule_count(), 1);
        assert!(engine.active_reorg().is_some());
        assert_eq!(engine.active_reorg().unwrap().depth(), 2);
    }
}
