//! End-to-end chain-fault test: a stale-head rule makes the proxy report a head
//! behind the real one.
//!
//! Anvil mines a block every second; a `stale_head = 3` rule rewrites the
//! `eth_blockNumber` response on the way back. Skips (does not fail) when
//! `anvil` is not installed, so it stays CI-friendly.

use std::process::{Command, Stdio};
use std::time::Duration;

use chain_chaos_proxy::{AppState, ProxyConfig};
use serde_json::{json, Value};

const ANVIL_PORT: u16 = 18548;
const LAG: u64 = 3;

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
async fn stale_head_reports_behind_real_head() {
    if !anvil_available() {
        eprintln!("skipping stale_head_reports_behind_real_head: `anvil` not found on PATH");
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

    let faults = toml::from_str(&format!("seed = 1\n[[rules]]\nstale_head = {LAG}\n"))
        .expect("valid faults toml");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = ProxyConfig::new(upstream.clone(), None, addr, false)
        .expect("config")
        .with_faults(faults);
    let state = AppState::new(cfg).expect("state");
    let app = chain_chaos_proxy::router(state);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let proxy_url = format!("http://{addr}");

    wait_until_head(&client, &upstream, LAG + 2).await;

    let proxy_head = head_of(&client, &proxy_url).await.expect("proxy head");
    let real_head = head_of(&client, &upstream).await.expect("real head");

    assert!(proxy_head > 0, "stale head should still be past genesis");
    let behind = real_head - proxy_head;
    assert!(
        (LAG..=LAG + 1).contains(&behind),
        "proxy head {proxy_head} should trail real head {real_head} by ~{LAG} (was {behind})"
    );

    server.abort();
    let _ = anvil.kill();
    let _ = anvil.wait();
}
