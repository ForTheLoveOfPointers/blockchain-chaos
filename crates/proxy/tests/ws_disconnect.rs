//! Anvil-backed integration test: the WebSocket disconnect fault.
//!
//! `ws_disconnect_after` is a connection-level fault: it is decided once, at
//! upgrade time, against `RpcView::Unknown` (no method is known yet), so only a
//! rule with no method filter and transport `ws`/`both` can match. This test
//! opens a real WS client through the proxy, verifies normal pass-through first,
//! then asserts the proxy tears the connection down at roughly the configured
//! deadline.
//!
//! It must be Anvil-backed, not hermetic: `ws::relay` opens the upstream WS with
//! `connect_async` *before* the disconnect matters, so a black-hole upstream
//! would fail the connect and never reach the disconnect logic.
//!
//! Skips (does not fail) if `anvil` is not installed, so it stays CI-friendly.

use std::process::{Command, Stdio};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::Message;

const ANVIL_PORT: u16 = 18546;
const DISCONNECT_AFTER: Duration = Duration::from_secs(1);

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
        let body = json!({"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1});
        if let Ok(resp) = client.post(url).json(&body).send().await {
            if resp.json::<Value>().await.is_ok() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("upstream at {url} never became ready");
}

#[tokio::test]
async fn ws_disconnect_fault_tears_down_connection() {
    if !anvil_available() {
        eprintln!("skipping ws_disconnect_fault_tears_down_connection: `anvil` not found on PATH");
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

    let faults = toml::from_str(&format!(
        "seed = 1\n[[rules]]\ntransport = \"ws\"\nws_disconnect_after = \"{}s\"\n",
        DISCONNECT_AFTER.as_secs()
    ))
    .expect("valid faults toml");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = chain_chaos_proxy::ProxyConfig::new(upstream.clone(), None, addr, false)
        .unwrap()
        .with_faults(faults);
    let state = chain_chaos_proxy::AppState::new(cfg).unwrap();
    let app = chain_chaos_proxy::router(state);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let ws_url = format!("ws://{addr}/");
    let connected_at = Instant::now();
    let (ws_stream, _resp) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("client connects to proxy websocket");
    let (mut tx, mut rx) = ws_stream.split();

    tx.send(Message::Text(
        r#"{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"#.into(),
    ))
    .await
    .expect("send over ws");

    let reply = read_json_reply(&mut rx).await.expect("a JSON-RPC reply");
    assert_eq!(reply["id"], json!(1), "reply id preserved through relay");
    assert!(
        reply.get("result").is_some(),
        "expected an eth_blockNumber result, got {reply}"
    );

    let terminated = timeout(
        DISCONNECT_AFTER + Duration::from_secs(7),
        drain_until_closed(&mut rx),
    )
    .await
    .expect("proxy must close the connection; it stayed open past the deadline");
    assert!(terminated, "stream ended via close/eof");

    let elapsed = connected_at.elapsed();
    assert!(
        elapsed >= DISCONNECT_AFTER - Duration::from_millis(150),
        "disconnect fired too early ({elapsed:?}); expected ~{DISCONNECT_AFTER:?}"
    );

    server.abort();
    let _ = anvil.kill();
    let _ = anvil.wait();
}

async fn read_json_reply<S>(rx: &mut S) -> Option<Value>
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(frame) = rx.next().await {
        match frame.ok()? {
            Message::Text(t) => {
                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                    return Some(v);
                }
            }
            Message::Close(_) => return None,
            _ => continue,
        }
    }
    None
}

async fn drain_until_closed<S>(rx: &mut S) -> bool
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match rx.next().await {
            Some(Ok(Message::Close(_))) => return true,
            Some(Ok(_)) => continue,
            Some(Err(_)) => return true,
            None => return true,
        }
    }
}
