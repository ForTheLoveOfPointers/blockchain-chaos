//! HTTP JSON-RPC pass-through.
//!
//! A single POST choke point forwards the raw request body to the upstream and
//! returns the upstream's raw response. Because the bytes are never rewritten,
//! JSON-RPC ids and batch ordering are preserved exactly. `forward` is the one
//! place a future phase will consult the fault engine before/after forwarding.

use std::time::Instant;

use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use tracing::{info, warn};

use crate::rpc::RpcView;
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
    let started = Instant::now();

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
