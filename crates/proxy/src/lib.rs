//! Transparent HTTP and WebSocket JSON-RPC proxy between an EVM app and its RPC
//! endpoint. With no rules it forwards traffic untouched; the fault engine only
//! changes the requests it matches.

pub mod cluster;
pub mod config;
pub mod fault;
pub mod http;
pub mod observe;
pub mod rpc;
pub mod state;
pub mod ws;

use axum::{routing::post, Router};
use tokio::net::TcpListener;
use tracing::info;

pub use cluster::{run_cluster, ClusterConfig, ProviderConfig};
pub use config::{ConfigError, FileConfig, ProxyConfig};
pub use observe::Observations;
pub use state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", post(http::http_handler).get(ws::ws_handler))
        .with_state(state)
}

pub async fn run(cfg: ProxyConfig) -> anyhow::Result<()> {
    let state = AppState::new(cfg)?;
    serve(state).await
}

pub async fn serve(state: AppState) -> anyhow::Result<()> {
    let listen = state.cfg.listen;
    let upstream_http = state.cfg.upstream_http.clone();
    let upstream_ws = state.cfg.upstream_ws.clone();

    let app = router(state);

    let listener = TcpListener::bind(listen).await?;
    info!(
        target: "chain_chaos",
        %listen,
        %upstream_http,
        %upstream_ws,
        "chain-chaos proxy listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!(target: "chain_chaos", "shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!(target: "chain_chaos", "received Ctrl-C, shutting down"),
        _ = terminate => info!(target: "chain_chaos", "received SIGTERM, shutting down"),
    }
}
