//! Anvil-backed integration test: the proxy must be transparent.
//!
//! Spawns a local Anvil node, runs the proxy in front of it, and asserts that
//! responses through the proxy match talking to the node directly, that batch
//! requests round-trip, and that JSON-RPC ids are preserved. Skips (does not
//! fail) if `anvil` is not installed, so it stays CI-friendly.

use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};

const ANVIL_PORT: u16 = 18545;

fn anvil_available() -> bool {
    Command::new("anvil")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn wait_ready(client: &reqwest::Client, url: &str) {
    for _ in 0..100 {
        if rpc_raw(client, url, "eth_chainId", json!(1)).await.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("upstream at {url} never became ready");
}

async fn rpc_raw(client: &reqwest::Client, url: &str, method: &str, id: Value) -> Option<Value> {
    let body = json!({"jsonrpc":"2.0","method":method,"params":[],"id":id});
    let resp = client.post(url).json(&body).send().await.ok()?;
    resp.json::<Value>().await.ok()
}

async fn result(client: &reqwest::Client, url: &str, method: &str) -> String {
    let v = rpc_raw(client, url, method, json!(1)).await.expect("response");
    v.get("result")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("no result in {v}"))
        .to_string()
}

#[tokio::test]
async fn proxy_is_transparent_over_anvil() {
    if !anvil_available() {
        eprintln!("skipping proxy_is_transparent_over_anvil: `anvil` not found on PATH");
        return;
    }

    let mut anvil = Command::new("anvil")
        .args(["--silent", "--port", &ANVIL_PORT.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn anvil");

    let client = reqwest::Client::new();
    let upstream = format!("http://127.0.0.1:{ANVIL_PORT}");
    wait_ready(&client, &upstream).await;

    // Bring up the proxy on an ephemeral port pointing at anvil.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = chain_chaos_proxy::ProxyConfig::new(upstream.clone(), None, addr, false).unwrap();
    let state = chain_chaos_proxy::AppState::new(cfg).unwrap();
    let app = chain_chaos_proxy::router(state);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let proxy_url = format!("http://{addr}");

    // 1. Single calls match the upstream exactly.
    assert_eq!(
        result(&client, &upstream, "eth_chainId").await,
        result(&client, &proxy_url, "eth_chainId").await,
        "chainId must match upstream"
    );
    assert_eq!(
        result(&client, &upstream, "eth_blockNumber").await,
        result(&client, &proxy_url, "eth_blockNumber").await,
        "blockNumber must match upstream"
    );

    // 2. Batch round-trips with ids matched to the right sub-responses.
    let batch = json!([
        {"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":"a"},
        {"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":7}
    ]);
    let resp: Value = client
        .post(&proxy_url)
        .json(&batch)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = resp.as_array().expect("batch response is an array");
    assert_eq!(arr.len(), 2);
    let by_id = |id: &Value| arr.iter().find(|r| r.get("id") == Some(id));
    assert!(by_id(&json!("a")).is_some(), "string id 'a' preserved");
    assert!(by_id(&json!(7)).is_some(), "numeric id 7 preserved");

    // 3. A string id survives verbatim on a single call.
    let single = rpc_raw(&client, &proxy_url, "eth_chainId", json!("str-id"))
        .await
        .unwrap();
    assert_eq!(single.get("id"), Some(&json!("str-id")));

    server.abort();
    let _ = anvil.kill();
    let _ = anvil.wait();
}
