//! Compile a scenario into a timeline and drive a live [`FaultEngine`] through it.
//! Each step holds the cumulative active rule set; faults stay until a `recover`
//! clears them, so the driver keeps no state and replays deterministically.

use std::sync::Arc;
use std::time::Duration;

use chain_chaos_proxy::fault::{
    Action, DelaySpec, FaultEngine, Matcher, RejectSpec, ReorgHandle, Rule, TransportMatch,
};
use tokio::time::Instant;
use tracing::info;

use crate::config::{
    DelayAction, DisconnectAction, DropAction, EventConfig, MalformedAction, MissingLogsAction,
    RejectAction, ReorgAction, Scenario, StaleHeadAction, TimeoutAction,
};
use crate::ScenarioError;

#[derive(Debug, Clone)]
pub struct Timeline {
    pub name: String,
    pub seed: u64,
    pub steps: Vec<Step>,
    pub assertions: Vec<crate::assertions::Assertion>,
    /// Offset of the final `recover`, if any: the point after which the
    /// recovery and catch-up assertions expect the client to see health again.
    pub recover_at: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct Step {
    pub at: Duration,
    pub active: Vec<Rule>,
    pub summary: String,
}

enum Transition {
    Add(Rule),
    Recover,
}

impl Scenario {
    pub fn compile(self, seed: u64) -> Result<Timeline, ScenarioError> {
        let mut resolved: Vec<(Duration, usize, Transition, String)> = Vec::new();
        for (i, ev) in self.events.into_iter().enumerate() {
            let at = event_offset(&ev, i)?;
            let (transition, summary) = compile_event(ev, i, seed)?;
            resolved.push((at, i, transition, summary));
        }
        resolved.sort_by_key(|(at, i, _, _)| (*at, *i));

        let mut active: Vec<Rule> = Vec::new();
        let mut steps: Vec<Step> = Vec::new();
        let mut recover_at: Option<Duration> = None;
        for (at, _i, transition, summary) in resolved {
            match transition {
                Transition::Add(rule) => active.push(rule),
                Transition::Recover => {
                    active.clear();
                    recover_at = Some(at);
                }
            }
            match steps.last_mut() {
                Some(last) if last.at == at => {
                    last.active = active.clone();
                    last.summary.push_str("; ");
                    last.summary.push_str(&summary);
                }
                _ => steps.push(Step {
                    at,
                    active: active.clone(),
                    summary,
                }),
            }
        }

        let assertions = self
            .assertions
            .iter()
            .enumerate()
            .map(|(i, a)| a.compile(i))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Timeline {
            name: self.name,
            seed,
            steps,
            assertions,
            recover_at,
        })
    }
}

impl Timeline {
    pub fn initial_rules(&self) -> Vec<Rule> {
        self.steps
            .iter()
            .find(|s| s.at.is_zero())
            .map(|s| s.active.clone())
            .unwrap_or_default()
    }

    pub fn build_engine(&self) -> FaultEngine {
        FaultEngine::new(self.seed, self.initial_rules())
    }

    /// The last scheduled offset in the timeline: how long the scenario runs
    /// before a `test` should start waiting out its grace window.
    pub fn last_offset(&self) -> Duration {
        self.steps
            .iter()
            .map(|s| s.at)
            .max()
            .unwrap_or(Duration::ZERO)
    }

    pub fn describe(&self) -> String {
        let mut out = format!("scenario: {}\nseed: {}\n", self.name, self.seed);
        if self.steps.is_empty() {
            out.push_str("  (no events, pure pass-through)\n");
        }
        for step in &self.steps {
            out.push_str(&format!(
                "  @ {:>8}  [{} active]  {}\n",
                humantime::format_duration(step.at).to_string(),
                step.active.len(),
                step.summary
            ));
        }
        if !self.assertions.is_empty() {
            out.push_str("assertions:\n");
            for a in &self.assertions {
                out.push_str(&format!("  - {a:?}\n"));
            }
        }
        out
    }
}

pub async fn drive(engine: Arc<FaultEngine>, timeline: Timeline) {
    let start = Instant::now();
    for step in &timeline.steps {
        if step.at.is_zero() {
            continue;
        }
        tokio::time::sleep_until(start + step.at).await;
        engine.set_rules(step.active.clone());
        info!(
            target: "chain_chaos::scenario",
            elapsed = %humantime::format_duration(step.at),
            active = step.active.len(),
            summary = %step.summary,
            "scenario transition"
        );
    }
    info!(target: "chain_chaos::scenario", scenario = %timeline.name, "scenario complete");
}

fn event_offset(ev: &EventConfig, index: usize) -> Result<Duration, ScenarioError> {
    match (&ev.at, &ev.after) {
        (Some(_), Some(_)) => Err(event_err(index, "set only one of `at` or `after`")),
        (None, None) => Err(event_err(
            index,
            "missing trigger: set `at: startup` or `after`",
        )),
        (Some(at), None) => {
            if at.trim().eq_ignore_ascii_case("startup") {
                Ok(Duration::ZERO)
            } else {
                Err(event_err(
                    index,
                    format!("unknown `at` value `{at}` (only `startup` is supported)"),
                ))
            }
        }
        (None, Some(after)) => parse_duration(index, "after", after),
    }
}

fn compile_event(
    ev: EventConfig,
    index: usize,
    seed: u64,
) -> Result<(Transition, String), ScenarioError> {
    let set = [
        ev.delay.is_some(),
        ev.timeout.is_some(),
        ev.reject.is_some(),
        ev.drop.is_some(),
        ev.disconnect.is_some(),
        ev.stale_head.is_some(),
        ev.missing_logs.is_some(),
        ev.malformed.is_some(),
        ev.reorg.is_some(),
        ev.recover == Some(true),
    ]
    .iter()
    .filter(|b| **b)
    .count();
    if set != 1 {
        return Err(event_err(
            index,
            format!(
                "expected exactly one action (delay, timeout, reject, drop, disconnect, stale_head, missing_logs, malformed, reorg, recover), found {set}"
            ),
        ));
    }

    if ev.recover == Some(true) {
        return Ok((Transition::Recover, "recover → pass-through".to_string()));
    }
    if let Some(a) = ev.delay {
        return compile_delay(a, index);
    }
    if let Some(a) = ev.timeout {
        return compile_timeout(a, index);
    }
    if let Some(a) = ev.reject {
        return compile_reject(a, index);
    }
    if let Some(a) = ev.drop {
        return compile_drop(a, index);
    }
    if let Some(a) = ev.disconnect {
        return compile_disconnect(a, index);
    }
    if let Some(a) = ev.stale_head {
        return compile_stale_head(a, index);
    }
    if let Some(a) = ev.missing_logs {
        return compile_missing_logs(a, index);
    }
    if let Some(a) = ev.malformed {
        return compile_malformed(a, index);
    }
    if let Some(a) = ev.reorg {
        return compile_reorg(a, index, seed);
    }
    unreachable!("action count checked above")
}

fn compile_delay(a: DelayAction, index: usize) -> Result<(Transition, String), ScenarioError> {
    let spec = match (&a.duration, &a.min, &a.max) {
        (Some(d), None, None) => DelaySpec::Fixed(parse_duration(index, "delay.duration", d)?),
        (None, Some(min), Some(max)) => {
            let min = parse_duration(index, "delay.min", min)?;
            let max = parse_duration(index, "delay.max", max)?;
            if max < min {
                return Err(event_err(index, "delay.min is greater than delay.max"));
            }
            DelaySpec::Range { min, max }
        }
        _ => {
            return Err(event_err(
                index,
                "delay needs either `duration` or both `min` and `max`",
            ))
        }
    };
    let summary = format!("delay {}", describe_delay(&spec));
    rule(
        index,
        a.methods,
        a.transport,
        a.probability,
        Action::Delay(spec),
        summary,
    )
}

fn compile_timeout(a: TimeoutAction, index: usize) -> Result<(Transition, String), ScenarioError> {
    let d = parse_duration(index, "timeout.duration", &a.duration)?;
    let summary = format!("timeout after {}", humantime::format_duration(d));
    rule(
        index,
        a.methods,
        a.transport,
        a.probability,
        Action::Timeout(d),
        summary,
    )
}

fn compile_reject(a: RejectAction, index: usize) -> Result<(Transition, String), ScenarioError> {
    let spec = RejectSpec {
        http_status: a.http_status.unwrap_or(200),
        code: a.code.unwrap_or(-32000),
        message: a.message.unwrap_or_else(|| "fault injected".to_string()),
    };
    let summary = format!("reject http={} code={}", spec.http_status, spec.code);
    rule(
        index,
        a.methods,
        a.transport,
        a.probability,
        Action::Reject(spec),
        summary,
    )
}

fn compile_drop(a: DropAction, index: usize) -> Result<(Transition, String), ScenarioError> {
    rule(
        index,
        a.methods,
        a.transport,
        a.probability,
        Action::Drop,
        "drop connection".to_string(),
    )
}

fn compile_disconnect(
    a: DisconnectAction,
    index: usize,
) -> Result<(Transition, String), ScenarioError> {
    let d = parse_duration(index, "disconnect.after", &a.after)?;
    let summary = format!("ws disconnect after {}", humantime::format_duration(d));
    let matcher = Matcher {
        methods: None,
        transport: chain_chaos_proxy::fault::TransportMatch::Ws,
    };
    let rule = Rule {
        name: Some(format!("event[{index}]")),
        matcher,
        probability: probability(a.probability, index)?,
        action: Action::WsDisconnect(d),
    };
    Ok((Transition::Add(rule), summary))
}

fn compile_stale_head(
    a: StaleHeadAction,
    index: usize,
) -> Result<(Transition, String), ScenarioError> {
    let summary = format!("stale head lag={}", a.blocks);
    chain_rule(
        index,
        Some(vec!["eth_blockNumber".to_string()]),
        a.probability,
        Action::StaleHead { lag: a.blocks },
        summary,
    )
}

fn compile_missing_logs(
    a: MissingLogsAction,
    index: usize,
) -> Result<(Transition, String), ScenarioError> {
    chain_rule(
        index,
        Some(vec!["eth_getLogs".to_string()]),
        a.probability,
        Action::MissingLogs,
        "missing logs".to_string(),
    )
}

fn compile_malformed(
    a: MalformedAction,
    index: usize,
) -> Result<(Transition, String), ScenarioError> {
    if a.methods.is_empty() {
        return Err(event_err(
            index,
            "malformed requires a non-empty `methods` list",
        ));
    }
    let summary = format!("malformed {:?}", a.methods);
    chain_rule(
        index,
        Some(a.methods),
        a.probability,
        Action::Malformed,
        summary,
    )
}

fn compile_reorg(
    a: ReorgAction,
    index: usize,
    seed: u64,
) -> Result<(Transition, String), ScenarioError> {
    let handle = ReorgHandle::new(
        seed,
        a.depth,
        a.remove_logs.unwrap_or(true),
        a.drop_transactions.unwrap_or(true),
    );
    let summary = format!("reorg depth={}", a.depth);
    chain_rule(index, None, a.probability, Action::Reorg(handle), summary)
}

fn chain_rule(
    index: usize,
    methods: Option<Vec<String>>,
    prob: Option<f64>,
    action: Action,
    summary: String,
) -> Result<(Transition, String), ScenarioError> {
    let matcher = Matcher {
        methods,
        transport: TransportMatch::Http,
    };
    let rule = Rule {
        name: Some(format!("event[{index}]")),
        matcher,
        probability: probability(prob, index)?,
        action,
    };
    Ok((Transition::Add(rule), summary))
}

fn rule(
    index: usize,
    methods: Option<Vec<String>>,
    transport: crate::config::TransportSel,
    prob: Option<f64>,
    action: Action,
    summary: String,
) -> Result<(Transition, String), ScenarioError> {
    let matcher = Matcher {
        methods,
        transport: transport.into(),
    };
    let rule = Rule {
        name: Some(format!("event[{index}]")),
        matcher,
        probability: probability(prob, index)?,
        action,
    };
    Ok((Transition::Add(rule), summary))
}

fn probability(p: Option<f64>, index: usize) -> Result<f64, ScenarioError> {
    let p = p.unwrap_or(1.0);
    if !(0.0..=1.0).contains(&p) {
        return Err(event_err(
            index,
            format!("probability {p} outside 0.0..=1.0"),
        ));
    }
    Ok(p)
}

fn describe_delay(spec: &DelaySpec) -> String {
    match spec {
        DelaySpec::Fixed(d) => humantime::format_duration(*d).to_string(),
        DelaySpec::Range { min, max } => format!(
            "{}..{}",
            humantime::format_duration(*min),
            humantime::format_duration(*max)
        ),
    }
}

fn parse_duration(index: usize, field: &str, s: &str) -> Result<Duration, ScenarioError> {
    humantime::parse_duration(s).map_err(|e| {
        event_err(
            index,
            format!("invalid duration for `{field}` (`{s}`): {e}"),
        )
    })
}

fn event_err(index: usize, msg: impl Into<String>) -> ScenarioError {
    ScenarioError::Event {
        index,
        msg: msg.into(),
    }
}
