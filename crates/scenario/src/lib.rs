//! Deterministic scenario engine. A YAML [`Scenario`] compiles into a [`Timeline`]
//! of rule-set transitions, and [`drive`] swaps the live fault engine's rules as
//! wall-clock time advances.

pub mod config;
pub mod timeline;

pub use config::Scenario;
pub use timeline::{drive, Step, Timeline};

pub fn resolve_seed(cli_override: Option<u64>, scenario: &Scenario) -> u64 {
    cli_override.or(scenario.seed).unwrap_or_else(generate_seed)
}

fn generate_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[derive(Debug, thiserror::Error)]
pub enum ScenarioError {
    #[error("invalid scenario YAML: {0}")]
    Parse(String),
    #[error("event[{index}]: {msg}")]
    Event { index: usize, msg: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn compile(src: &str) -> Timeline {
        let scenario = Scenario::from_yaml_str(src).expect("valid yaml");
        let seed = resolve_seed(Some(7), &scenario);
        scenario.compile(seed).expect("compiles")
    }

    #[test]
    fn empty_scenario_has_no_steps() {
        let t = compile("name: empty\n");
        assert!(t.steps.is_empty());
        assert!(t.initial_rules().is_empty());
    }

    #[test]
    fn startup_event_becomes_initial_rules() {
        let t = compile(
            "name: s\nevents:\n  - at: startup\n    delay: { methods: [eth_getLogs], duration: 2s }\n",
        );
        assert_eq!(t.steps.len(), 1);
        assert_eq!(t.steps[0].at, Duration::ZERO);
        assert_eq!(t.initial_rules().len(), 1);
    }

    #[test]
    fn faults_accumulate_then_recover_clears() {
        let t = compile(
            "name: s
events:
  - at: startup
    delay: { duration: 1s }
  - after: 5s
    reject: { methods: [eth_sendRawTransaction], http_status: 429 }
  - after: 10s
    recover: true
",
        );
        assert_eq!(t.steps.len(), 3);
        assert_eq!(t.steps[0].active.len(), 1);
        assert_eq!(t.steps[1].active.len(), 2);
        assert_eq!(t.steps[1].at, Duration::from_secs(5));
        assert_eq!(t.steps[2].active.len(), 0);
    }

    #[test]
    fn events_are_sorted_by_offset() {
        let t = compile(
            "name: s
events:
  - after: 30s
    recover: true
  - at: startup
    drop: {}
",
        );
        assert_eq!(t.steps[0].at, Duration::ZERO);
        assert_eq!(t.steps[1].at, Duration::from_secs(30));
    }

    #[test]
    fn same_offset_events_coalesce_into_one_step() {
        let t = compile(
            "name: s
events:
  - at: startup
    delay: { duration: 1s }
  - at: startup
    reject: { http_status: 500 }
",
        );
        assert_eq!(t.steps.len(), 1, "both startup events share one step");
        assert_eq!(t.steps[0].active.len(), 2);
    }

    #[test]
    fn rejects_event_with_no_action() {
        let err = Scenario::from_yaml_str("name: s\nevents:\n  - after: 1s\n")
            .unwrap()
            .compile(1)
            .unwrap_err();
        assert!(err.to_string().contains("exactly one action"), "{err}");
    }

    #[test]
    fn rejects_event_with_two_actions() {
        let err = Scenario::from_yaml_str(
            "name: s\nevents:\n  - after: 1s\n    drop: {}\n    recover: true\n",
        )
        .unwrap()
        .compile(1)
        .unwrap_err();
        assert!(err.to_string().contains("exactly one action"), "{err}");
    }

    #[test]
    fn rejects_missing_trigger() {
        let err = Scenario::from_yaml_str("name: s\nevents:\n  - drop: {}\n")
            .unwrap()
            .compile(1)
            .unwrap_err();
        assert!(err.to_string().contains("missing trigger"), "{err}");
    }

    #[test]
    fn rejects_bad_probability() {
        let err = Scenario::from_yaml_str(
            "name: s\nevents:\n  - at: startup\n    drop: { probability: 1.5 }\n",
        )
        .unwrap()
        .compile(1)
        .unwrap_err();
        assert!(err.to_string().contains("probability"), "{err}");
    }

    #[test]
    fn cli_seed_overrides_file_seed() {
        let s = Scenario::from_yaml_str("name: s\nseed: 100\n").unwrap();
        assert_eq!(resolve_seed(Some(42), &s), 42);
        assert_eq!(resolve_seed(None, &s), 100);
    }

    #[test]
    fn compiles_chain_fault_events() {
        let t = compile(
            "name: s
events:
  - at: startup
    stale_head: { blocks: 2 }
  - after: 5s
    missing_logs: {}
  - after: 10s
    recover: true
",
        );
        assert_eq!(t.steps.len(), 3);
        assert_eq!(t.steps[0].active.len(), 1);
        assert_eq!(t.steps[1].active.len(), 2);
        assert_eq!(t.steps[2].active.len(), 0);
    }

    #[test]
    fn compiles_reorg_event() {
        let t = compile(
            "name: s
events:
  - after: 5s
    reorg: { depth: 3 }
  - after: 20s
    recover: true
",
        );
        assert_eq!(t.steps.len(), 2);
        assert_eq!(t.steps[0].active.len(), 1);
        assert_eq!(t.steps[1].active.len(), 0);
    }

    #[test]
    fn rejects_malformed_without_methods() {
        let err = Scenario::from_yaml_str(
            "name: s\nevents:\n  - at: startup\n    malformed: { methods: [] }\n",
        )
        .unwrap()
        .compile(1)
        .unwrap_err();
        assert!(err.to_string().contains("malformed requires"), "{err}");
    }
}
