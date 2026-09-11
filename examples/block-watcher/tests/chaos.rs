//! End-to-end chaos test: the whole product wired together.
//!
//!   anvil  <--  chain-chaos proxy (scenario-driven)  <--  block-watcher
//!
//! A scenario rejects every `eth_blockNumber` for the first second, then
//! recovers. The watcher must survive the outage (counting errors, never
//! crashing) and, once the fault clears, catch back up to anvil's real head.
//! This is the roadmap's `eventual_recovery` + `canonical_block_sequence`
//! assertions in miniature.
//!
//! Skips (does not fail) if `anvil` is not installed, so it stays CI-friendly.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use block_watcher::{WatchConfig, Watcher};
use serde_json::{json, Value};

const ANVIL_PORT: u16 = 18547;

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

#[tokio::test]
async fn watcher_recovers_after_rpc_outage() {
    if !anvil_available() {
        eprintln!("skipping watcher_recovers_after_rpc_outage: `anvil` not found on PATH");
        return;
    }

    let mut anvil = Command::new("anvil")
        .args([
            "--silent",
            "--port",
            &ANVIL_PORT.to_string(),
            "--block-time",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn anvil");

    let client = reqwest::Client::new();
    let upstream = format!("http://127.0.0.1:{ANVIL_PORT}");
    wait_ready(&client, &upstream).await;

    let scenario = chain_chaos_scenario::Scenario::from_yaml_str(
        r#"
name: watcher-recovery
seed: 42
events:
  - at: startup
    reject:
      methods: [eth_blockNumber]
      http_status: 503
      message: upstream down
  - after: 1s
    recover: true
"#,
    )
    .expect("valid scenario");
    let seed = chain_chaos_scenario::resolve_seed(None, &scenario);
    let timeline = scenario.compile(seed).expect("compiles");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = chain_chaos_proxy::ProxyConfig::new(upstream.clone(), None, addr, false).unwrap();
    let engine = Arc::new(timeline.build_engine());
    let state = chain_chaos_proxy::AppState::with_engine(cfg, engine.clone()).unwrap();
    let app = chain_chaos_proxy::router(state);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::spawn(chain_chaos_scenario::drive(engine, timeline));

    let proxy_url = format!("http://{addr}");

    let watcher = Watcher::new(WatchConfig {
        rpc_url: proxy_url.clone(),
        poll_interval: Duration::from_millis(150),
    });
    watcher.run_for(Duration::from_secs(3)).await;

    let observed = watcher.snapshot();
    let anvil_head = head_of(&client, &upstream).await.expect("anvil head");

    assert!(
        observed.errors >= 1,
        "expected the injected outage to cause poll errors, got {observed:?}"
    );

    let head = observed
        .head
        .expect("watcher observed a head after recovery");
    assert!(head > 0, "head should have advanced past genesis");
    assert!(
        anvil_head.saturating_sub(head) <= 1,
        "watcher head {head} should have caught up to anvil head {anvil_head}"
    );

    assert_eq!(
        observed.regressions, 0,
        "head must be monotonic, got {observed:?}"
    );

    server.abort();
    let _ = anvil.kill();
    let _ = anvil.wait();
}
