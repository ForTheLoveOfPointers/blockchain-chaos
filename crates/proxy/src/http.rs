//! HTTP JSON-RPC pass-through, with Phase 2 fault injection.
//!
//! A single POST choke point forwards the raw request body to the upstream and
//! returns the upstream's raw response. Because the bytes are never rewritten on
//! the pass-through path, JSON-RPC ids and batch ordering are preserved exactly.
//! `forward` first consults the fault engine; only faulted requests get a
//! synthesized response (whose ids we *do* build from the parsed view).

use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::fault::{FaultContext, FaultDecision, RejectSpec, Transport};
use crate::rpc::{RpcCall, RpcView};
use crate::state::AppState;

pub async fn http_handler(
    State(state): State<AppState>,
    _headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(&state, body).await
}

async fn forward(state: &AppState, body: Bytes) -> Response {
    let view = RpcView::parse(&body);

    match state.fault.decide(&FaultContext {
        transport: Transport::Http,
        view: &view,
    }) {
        FaultDecision::Pass => {}
        FaultDecision::Delay(d) => {
            info!(target: "chain_chaos::fault", rpc = %view.summary(), delay_ms = d.as_millis(), "injecting latency");
            tokio::time::sleep(d).await;
        }
        FaultDecision::Timeout(d) => {
            info!(target: "chain_chaos::fault", rpc = %view.summary(), hold_ms = d.as_millis(), "injecting timeout");
            tokio::time::sleep(d).await;
            return reject_response(&view, &timeout_spec());
        }
        FaultDecision::Reject(spec) => {
            info!(target: "chain_chaos::fault", rpc = %view.summary(), http_status = spec.http_status, code = spec.code, "injecting rejection");
            return reject_response(&view, &spec);
        }
        FaultDecision::Drop => {
            info!(target: "chain_chaos::fault", rpc = %view.summary(), "injecting connection drop");
            return drop_response();
        }
        FaultDecision::WsDisconnect(_) => {}
    }

    forward_upstream(state, &view, body).await
}

async fn forward_upstream(state: &AppState, view: &RpcView, body: Bytes) -> Response {
    let started = std::time::Instant::now();

    if state.cfg.log_bodies {
        info!(target: "chain_chaos::http", rpc = %view.summary(), body = %String::from_utf8_lossy(&body), "-> upstream");
    } else {
        info!(target: "chain_chaos::http", rpc = %view.summary(), bytes = body.len(), "-> upstream");
    }

    let resp = state
        .http_client
        .post(&state.cfg.upstream_http)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await;

    let elapsed_ms = started.elapsed().as_millis();

    match resp {
        Ok(upstream) => {
            let status = upstream.status();
            let bytes = match upstream.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    warn!(target: "chain_chaos::http", error = %e, "failed reading upstream body");
                    return (StatusCode::BAD_GATEWAY, "upstream body read error").into_response();
                }
            };

            let bytes = if status.is_success() {
                match state.fault.intercept(
                    &FaultContext {
                        transport: Transport::Http,
                        view,
                    },
                    &bytes,
                ) {
                    Some(rewritten) => {
                        info!(target: "chain_chaos::fault", rpc = %view.summary(), "rewriting upstream response");
                        Bytes::from(rewritten)
                    }
                    None => bytes,
                }
            } else {
                bytes
            };

            if state.cfg.log_bodies {
                info!(target: "chain_chaos::http", %status, elapsed_ms, body = %String::from_utf8_lossy(&bytes), "<- upstream");
            } else {
                info!(target: "chain_chaos::http", %status, elapsed_ms, bytes = bytes.len(), "<- upstream");
            }

            (
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                [(header::CONTENT_TYPE, "application/json")],
                bytes,
            )
                .into_response()
        }
        Err(e) => {
            warn!(target: "chain_chaos::http", error = %e, elapsed_ms, "upstream request failed");
            (StatusCode::BAD_GATEWAY, format!("upstream error: {e}")).into_response()
        }
    }
}

fn timeout_spec() -> RejectSpec {
    RejectSpec {
        http_status: 504,
        code: -32603,
        message: "request timed out (fault injected)".to_string(),
    }
}

fn reject_response(view: &RpcView, spec: &RejectSpec) -> Response {
    let body = match view {
        RpcView::Single(call) => error_object(call, spec),
        RpcView::Batch(calls) => {
            Value::Array(calls.iter().map(|c| error_object(c, spec)).collect())
        }
        RpcView::Unknown => error_for_id(Value::Null, spec),
    };

    let status = StatusCode::from_u16(spec.http_status).unwrap_or(StatusCode::BAD_GATEWAY);
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

fn error_object(call: &RpcCall, spec: &RejectSpec) -> Value {
    error_for_id(call.id.clone().unwrap_or(Value::Null), spec)
}

fn error_for_id(id: Value, spec: &RejectSpec) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": spec.code, "message": spec.message },
    })
}

fn drop_response() -> Response {
    (
        StatusCode::BAD_GATEWAY,
        [(header::CONNECTION, "close")],
        Bytes::new(),
    )
        .into_response()
}
