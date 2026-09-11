//! End-to-end reorg test: a depth-2 reorg rule makes the proxy present the top
//! two blocks with new hashes and a re-linked parent chain, while leaving blocks
//! below the fork point untouched — the same-height replacement an indexer must
//! detect by hash.
//!
//! Skips (does not fail) when `anvil` is not installed, so it stays CI-friendly.

use std::process::{Command, Stdio};
use std::time::Duration;

use chain_chaos_proxy::{AppState, ProxyConfig};
use serde_json::{json, Value};

const ANVIL_PORT: u16 = 18550;
const DEPTH: u64 = 2;

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
    let json: Value = client
        .post(url)
        .json(&body)
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    let hex = json.get("result")?.as_str()?;
    u64::from_str_radix(hex.strip_prefix("0x").unwrap_or(hex), 16).ok()
}

async fn block_at(client: &reqwest::Client, url: &str, number: u64) -> Value {
    let body = json!({
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [format!("0x{number:x}"), false],
        "id": 1,
    });
    let json: Value = client
        .post(url)
        .json(&body)
        .send()
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    json["result"].clone()
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
async fn reorg_replaces_top_blocks_with_relinked_branch() {
    if !anvil_available() {
        eprintln!(
            "skipping reorg_replaces_top_blocks_with_relinked_branch: `anvil` not found on PATH"
        );
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

    let faults = toml::from_str(&format!(
        "seed = 7\n[[rules]]\nreorg = {{ depth = {DEPTH} }}\n"
    ))
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

    wait_until_head(&client, &upstream, 6).await;

    let head = head_of(&client, &proxy_url)
        .await
        .expect("pin fork via proxy head");

    let proxy_top = block_at(&client, &proxy_url, head).await;
    let proxy_below_top = block_at(&client, &proxy_url, head - 1).await;
    let real_top = block_at(&client, &upstream, head).await;

    let proxy_deep = block_at(&client, &proxy_url, head - DEPTH - 1).await;
    let real_deep = block_at(&client, &upstream, head - DEPTH - 1).await;

    assert_eq!(proxy_top["number"], real_top["number"], "same height");
    assert_ne!(
        proxy_top["hash"], real_top["hash"],
        "top block should carry an alternative hash"
    );
    assert_eq!(
        proxy_top["parentHash"], proxy_below_top["hash"],
        "alt branch must chain: top.parentHash == child.hash"
    );
    assert_eq!(
        proxy_deep["hash"], real_deep["hash"],
        "blocks below the fork point are untouched"
    );

    server.abort();
    let _ = anvil.kill();
    let _ = anvil.wait();
}
