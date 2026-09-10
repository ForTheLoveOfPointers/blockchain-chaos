//! WebSocket JSON-RPC pass-through.
//!
//! The client upgrades against us; we open an upstream WS and relay frames in
//! both directions. Relaying whole frames makes subscriptions (`newHeads`,
//! `logs`) transparent for free and preserves ids without response-matching —
//! that matching only becomes necessary once Phase 2 reorders/delays frames.

use axum::{
    extract::{
        ws::{Message as AxumMsg, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message as TungMsg;
use tracing::{info, warn};

use crate::rpc::RpcView;
use crate::state::AppState;

pub async fn ws_handler(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = relay(state, socket).await {
            warn!(target: "chain_chaos::ws", error = %e, "ws relay ended with error");
        }
    })
}

async fn relay(state: AppState, client: WebSocket) -> anyhow::Result<()> {
    let url = state.cfg.upstream_ws.clone();
    info!(target: "chain_chaos::ws", %url, "opening upstream websocket");

    let (upstream, _resp) = tokio_tungstenite::connect_async(&url).await?;

    let (mut client_tx, mut client_rx) = client.split();
    let (mut up_tx, mut up_rx) = upstream.split();

    let log_bodies = state.cfg.log_bodies;

    loop {
        tokio::select! {
            incoming = client_rx.next() => {
                match incoming {
                    Some(Ok(msg)) => {
                        log_client_msg(&msg, log_bodies);
                        if matches!(msg, AxumMsg::Close(_)) {
                            let _ = up_tx.send(TungMsg::Close(None)).await;
                            break;
                        }
                        if let Some(t) = axum_to_tung(msg) {
                            if up_tx.send(t).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        warn!(target: "chain_chaos::ws", error = %e, "client recv error");
                        break;
                    }
                    None => break,
                }
            }
            outgoing = up_rx.next() => {
                match outgoing {
                    Some(Ok(msg)) => {
                        log_upstream_msg(&msg, log_bodies);
                        if matches!(msg, TungMsg::Close(_)) {
                            let _ = client_tx.send(AxumMsg::Close(None)).await;
                            break;
                        }
                        if let Some(a) = tung_to_axum(msg) {
                            if client_tx.send(a).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        warn!(target: "chain_chaos::ws", error = %e, "upstream recv error");
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    let _ = up_tx.close().await;
    let _ = client_tx.close().await;
    info!(target: "chain_chaos::ws", "ws relay closed");
    Ok(())
}

fn axum_to_tung(msg: AxumMsg) -> Option<TungMsg> {
    match msg {
        AxumMsg::Text(t) => Some(TungMsg::Text(t.as_str().into())),
        AxumMsg::Binary(b) => Some(TungMsg::Binary(b)),
        AxumMsg::Ping(p) => Some(TungMsg::Ping(p)),
        AxumMsg::Pong(p) => Some(TungMsg::Pong(p)),
        AxumMsg::Close(_) => Some(TungMsg::Close(None)),
    }
}

fn tung_to_axum(msg: TungMsg) -> Option<AxumMsg> {
    match msg {
        TungMsg::Text(t) => Some(AxumMsg::Text(t.as_str().into())),
        TungMsg::Binary(b) => Some(AxumMsg::Binary(b)),
        TungMsg::Ping(p) => Some(AxumMsg::Ping(p)),
        TungMsg::Pong(p) => Some(AxumMsg::Pong(p)),
        TungMsg::Close(_) => Some(AxumMsg::Close(None)),
        TungMsg::Frame(_) => None,
    }
}

fn log_client_msg(msg: &AxumMsg, log_bodies: bool) {
    if let AxumMsg::Text(t) = msg {
        let view = RpcView::parse(t.as_bytes());
        if log_bodies {
            info!(target: "chain_chaos::ws", rpc = %view.summary(), body = %t.as_str(), "-> upstream");
        } else {
            info!(target: "chain_chaos::ws", rpc = %view.summary(), "-> upstream");
        }
    }
}

fn log_upstream_msg(msg: &TungMsg, log_bodies: bool) {
    if let TungMsg::Text(t) = msg {
        let view = RpcView::parse(t.as_bytes());
        if log_bodies {
            info!(target: "chain_chaos::ws", rpc = %view.summary(), body = %t.as_str(), "<- upstream");
        } else {
            info!(target: "chain_chaos::ws", rpc = %view.summary(), "<- upstream");
        }
    }
}
