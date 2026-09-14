//! HTTP JSON-RPC pass-through. One POST forwards the raw body upstream and returns
//! the raw response, so ids and batch order survive. The fault engine runs before
//! forwarding and can rewrite the response after.

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

    let ctx = FaultContext {
        transport: Transport::Http,
        view: &view,
    };
    let decision = state.fault.decide(&ctx);
    if let Some(observer) = &state.fault_observer {
        observer.on_decision(&ctx, &decision);
    }

    match decision {
        FaultDecision::Pass => {}
        FaultDecision::Delay(d) => {
            info!(target: "chain_chaos::fault", rpc = %view.summary(), delay_ms = d.as_millis(), "injecting latency");
            tokio::time::sleep(d).await;
        }
        FaultDecision::Timeout(d) => {
            info!(target: "chain_chaos::fault", rpc = %view.summary(), hold_ms = d.as_millis(), "injecting timeout");
            tokio::time::sleep(d).await;
            record(state, &view, b"", true, false);
            return reject_response(&view, &timeout_spec());
        }
        FaultDecision::Reject(spec) => {
            info!(target: "chain_chaos::fault", rpc = %view.summary(), http_status = spec.http_status, code = spec.code, "injecting rejection");
            record(state, &view, b"", true, false);
            return reject_response(&view, &spec);
        }
        FaultDecision::Drop => {
            info!(target: "chain_chaos::fault", rpc = %view.summary(), "injecting connection drop");
            record(state, &view, b"", true, false);
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

            record(state, view, &bytes, false, status.is_success());

            (
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                [(header::CONTENT_TYPE, "application/json")],
                bytes,
            )
                .into_response()
        }
        Err(e) => {
            warn!(target: "chain_chaos::http", error = %e, elapsed_ms, "upstream request failed");
            record(state, view, b"", false, false);
            (StatusCode::BAD_GATEWAY, format!("upstream error: {e}")).into_response()
        }
    }
}

/// Record a delivered HTTP response when observation is enabled; a no-op otherwise.
fn record(state: &AppState, view: &RpcView, delivered: &[u8], faulted: bool, ok: bool) {
    if let Some(obs) = &state.observe {
        obs.record_http(view, delivered, faulted, ok);
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
