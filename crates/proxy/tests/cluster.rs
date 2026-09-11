//! End-to-end multi-provider test: two providers front one node, one healthy and
//! one stale, and the cluster reports divergent heads. Skips if anvil is absent.

use std::process::{Command, Stdio};
use std::time::Duration;

use chain_chaos_proxy::ClusterConfig;
use serde_json::{json, Value};

const ANVIL_PORT: u16 = 18549;
const HEALTHY_PORT: u16 = 18650;
const STALE_PORT: u16 = 18651;
const LAG: u64 = 5;

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

async fn wait_until_head(client: &reqwest::Client, url: &str, target: u64) -> u64 {
    for _ in 0..200 {
        if let Some(h) = head_of(client, url).await {
            if h >= target {
                return h;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("head at {url} never reached {target}");
}

#[tokio::test]
async fn cluster_providers_report_divergent_heads() {
    if !anvil_available() {
        eprintln!("skipping cluster_providers_report_divergent_heads: `anvil` not found on PATH");
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
    wait_until_head(&client, &upstream, 1).await;

    let cfg = ClusterConfig::from_toml_str(&format!(
        r#"
        seed = 1

        [providers.healthy]
        upstream = "{upstream}"
        listen   = "127.0.0.1:{HEALTHY_PORT}"

        [providers.stale]
        upstream = "{upstream}"
        listen   = "127.0.0.1:{STALE_PORT}"
        [providers.stale.faults]
        [[providers.stale.faults.rules]]
        stale_head = {LAG}
        "#
    ))
    .expect("valid cluster toml");

    let cluster = tokio::spawn(chain_chaos_proxy::run_cluster(cfg));

    let healthy_url = format!("http://127.0.0.1:{HEALTHY_PORT}");
    let stale_url = format!("http://127.0.0.1:{STALE_PORT}");
    wait_until_head(&client, &healthy_url, LAG + 2).await;

    let healthy_head = head_of(&client, &healthy_url).await.expect("healthy head");
    let stale_head = head_of(&client, &stale_url).await.expect("stale head");

    assert!(stale_head > 0, "stale head should still be past genesis");
    let behind = healthy_head - stale_head;
    assert!(
        (LAG..=LAG + 1).contains(&behind),
        "stale provider {stale_head} should trail healthy provider {healthy_head} by ~{LAG} (was {behind})"
    );

    cluster.abort();
    let _ = anvil.kill();
    let _ = anvil.wait();
}
