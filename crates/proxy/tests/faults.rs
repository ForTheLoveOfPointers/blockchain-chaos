//! End-to-end fault-injection tests through the real router. A `reject` rule never
//! forwards upstream, so these need no anvil and stay hermetic.

use std::time::Instant;

use chain_chaos_proxy::{AppState, ProxyConfig};
use serde_json::{json, Value};

async fn spawn_proxy(faults_toml: &str) -> (String, tokio::task::JoinHandle<()>) {
    let faults = toml::from_str(faults_toml).expect("valid faults toml");
    let cfg = ProxyConfig::new("http://127.0.0.1:1".to_string(), None, unused_addr(), false)
        .expect("config")
        .with_faults(faults);
    let state = AppState::new(cfg).expect("state");
    let app = chain_chaos_proxy::router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), handle)
}

fn unused_addr() -> std::net::SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn post(client: &reqwest::Client, url: &str, body: Value) -> reqwest::Response {
    client.post(url).json(&body).send().await.unwrap()
}

#[tokio::test]
async fn reject_rule_returns_error_with_preserved_id() {
    let (url, server) = spawn_proxy(
        r#"
        seed = 1
        [[rules]]
        methods = ["eth_call"]
        reject = { http_status = 429, code = -32005, message = "rate limited" }
        "#,
    )
    .await;
    let client = reqwest::Client::new();

    let resp = post(
        &client,
        &url,
        json!({"jsonrpc":"2.0","method":"eth_call","params":[],"id":"abc"}),
    )
    .await;

    assert_eq!(resp.status().as_u16(), 429);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["id"], json!("abc"), "id must be preserved");
    assert_eq!(body["error"]["code"], json!(-32005));
    assert_eq!(body["error"]["message"], json!("rate limited"));

    server.abort();
}

#[tokio::test]
async fn unmatched_method_is_not_rejected() {
    let (url, server) = spawn_proxy(
        "seed = 1\n[[rules]]\nmethods = [\"eth_call\"]\nreject = { http_status = 429 }\n",
    )
    .await;
    let client = reqwest::Client::new();

    let resp = post(
        &client,
        &url,
        json!({"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}),
    )
    .await;

    assert_ne!(
        resp.status().as_u16(),
        429,
        "must not hit the eth_call fault"
    );
    server.abort();
}

#[tokio::test]
async fn batch_reject_returns_one_error_per_call() {
    let (url, server) =
        spawn_proxy("seed = 1\n[[rules]]\nreject = { code = -32000, message = \"down\" }\n").await;
    let client = reqwest::Client::new();

    let resp = post(
        &client,
        &url,
        json!([
            {"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":"a"},
            {"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":2}
        ]),
    )
    .await;

    let body: Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("batch error is an array");
    assert_eq!(arr.len(), 2);
    let ids: Vec<&Value> = arr.iter().map(|e| &e["id"]).collect();
    assert!(ids.contains(&&json!("a")) && ids.contains(&&json!(2)));

    server.abort();
}

#[tokio::test]
async fn delay_rule_adds_measurable_latency() {
    let (url, server) =
        spawn_proxy("seed = 1\n[[rules]]\nmethods = [\"eth_call\"]\ndelay = \"400ms\"\n").await;
    let client = reqwest::Client::new();

    let started = Instant::now();
    let _ = post(
        &client,
        &url,
        json!({"jsonrpc":"2.0","method":"eth_call","params":[],"id":1}),
    )
    .await;
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(400),
        "injected delay should have elapsed"
    );

    server.abort();
}
