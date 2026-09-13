//! End-to-end assertion tests: anvil, a scenario-driven proxy with observation on,
//! and block-watcher driving traffic. One scenario recovers cleanly (all assertions
//! pass); one traps a number-only client with a stale head (head_monotonic fails).
//! Skips if anvil is absent.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use block_watcher::{WatchConfig, Watcher};
use chain_chaos_proxy::{AppState, Observations, ProxyConfig};
use chain_chaos_scenario::{evaluate_timeline, resolve_seed, AssertionOutcome, Ground, Scenario};
use serde_json::{json, Value};

fn anvil_available() -> bool {
    Command::new("anvil")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn head_of(client: &reqwest::Client, url: &str) -> Option<u64> {
    let body = json!({"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1});
    let resp = client.post(url).json(&body).send().await.ok()?;
    let json: Value = resp.json().await.ok()?;
    let hex = json.get("result")?.as_str()?;
    u64::from_str_radix(hex.strip_prefix("0x").unwrap_or(hex), 16).ok()
}

async fn wait_ready(client: &reqwest::Client, url: &str) {
    for _ in 0..100 {
        if head_of(client, url).await.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("upstream at {url} never became ready");
}

/// Run a scenario through the proxy while block-watcher polls it, then evaluate the
/// scenario's assertions against what the proxy delivered.
async fn run_assertions(
    port: u16,
    scenario_yaml: &str,
    run_for: Duration,
) -> Vec<AssertionOutcome> {
    let client = reqwest::Client::new();
    let upstream = format!("http://127.0.0.1:{port}");
    wait_ready(&client, &upstream).await;

    let scenario = Scenario::from_yaml_str(scenario_yaml).expect("valid scenario");
    let seed = resolve_seed(None, &scenario);
    let timeline = scenario.compile(seed).expect("compiles");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = ProxyConfig::new(upstream.clone(), None, addr, false).unwrap();
    let engine = Arc::new(timeline.build_engine());
    let obs = Arc::new(Observations::new());
    let state = AppState::with_engine(cfg, engine.clone())
        .unwrap()
        .with_observations(obs.clone());
    let app = chain_chaos_proxy::router(state);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::spawn(chain_chaos_scenario::drive(engine, timeline.clone()));

    let proxy_url = format!("http://{addr}");
    let watcher = Watcher::new(WatchConfig {
        rpc_url: proxy_url,
        poll_interval: Duration::from_millis(250),
    });
    watcher.run_for(run_for).await;

    let upstream_head = head_of(&client, &upstream).await;
    let ground = Ground {
        upstream_head,
        recover_at: timeline.recover_at,
    };
    let outcomes = evaluate_timeline(&timeline, &obs, &ground);

    server.abort();
    outcomes
}

fn outcome<'a>(outcomes: &'a [AssertionOutcome], name: &str) -> &'a AssertionOutcome {
    outcomes
        .iter()
        .find(|o| o.name == name)
        .unwrap_or_else(|| panic!("no assertion named {name} in {outcomes:?}"))
}

#[tokio::test]
async fn recovery_scenario_passes_all_assertions() {
    if !anvil_available() {
        eprintln!("skipping recovery_scenario_passes_all_assertions: `anvil` not found on PATH");
        return;
    }
    let port = 18548;
    let mut anvil = spawn_anvil(port);

    let outcomes = run_assertions(
        port,
        r#"
name: head-recovery
seed: 12345
events:
  - at: startup
    reject:
      methods: [eth_blockNumber]
      http_status: 503
      message: upstream down
  - after: 2s
    recover: true
assertions:
  - eventual_recovery
  - head_monotonic
  - catches_up: { within: 5s }
"#,
        // Evaluate right at the catch-up deadline (recover_at 2s + within 5s), as
        // the CLI does, so the upstream head is read when the window closes.
        Duration::from_secs(7),
    )
    .await;

    for o in &outcomes {
        assert!(o.passed, "expected {} to pass: {}", o.name, o.detail);
    }

    let _ = anvil.kill();
    let _ = anvil.wait();
}

#[tokio::test]
async fn stale_head_trap_fails_head_monotonic() {
    if !anvil_available() {
        eprintln!("skipping stale_head_trap_fails_head_monotonic: `anvil` not found on PATH");
        return;
    }
    let port = 18549;
    let mut anvil = spawn_anvil(port);

    let outcomes = run_assertions(
        port,
        r#"
name: stale-head-trap
seed: 12345
events:
  - after: 3s
    stale_head: { blocks: 5 }
assertions:
  - head_monotonic
"#,
        Duration::from_secs(6),
    )
    .await;

    let mono = outcome(&outcomes, "head_monotonic");
    assert!(
        !mono.passed,
        "stale head should have broken monotonicity, but it passed: {}",
        mono.detail
    );
    assert!(mono.detail.contains("backward"), "detail: {}", mono.detail);

    let _ = anvil.kill();
    let _ = anvil.wait();
}

fn spawn_anvil(port: u16) -> std::process::Child {
    Command::new("anvil")
        .args(["--silent", "--port", &port.to_string(), "--block-time", "1"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn anvil")
}
