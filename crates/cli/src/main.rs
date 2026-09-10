//! `chain-chaos` command-line entrypoint.
//!
//! Phase 1 exposes a single subcommand: `proxy`, a transparent pass-through.
//! Later phases add `run`, `test`, `replay`, and `inspect`.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chain_chaos_proxy::{FileConfig, ProxyConfig};
use clap::{Parser, Subcommand};
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Parser)]
#[command(name = "chain-chaos", version, about = "Chaos testing for blockchain infrastructure")]
struct Cli {
    /// Emit logs as JSON instead of human-readable text.
    #[arg(long, global = true)]
    json_logs: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a transparent HTTP + WebSocket JSON-RPC pass-through proxy.
    Proxy(ProxyArgs),
}

#[derive(clap::Args)]
struct ProxyArgs {
    /// Upstream HTTP(S) JSON-RPC endpoint to forward to.
    #[arg(long)]
    upstream: Option<String>,

    /// Upstream WS(S) endpoint. Defaults to `--upstream` with the scheme swapped.
    #[arg(long)]
    upstream_ws: Option<String>,

    /// Local address to listen on.
    #[arg(long, default_value = "127.0.0.1:8545")]
    listen: SocketAddr,

    /// Log full request/response bodies (verbose).
    #[arg(long)]
    log_bodies: bool,

    /// Optional TOML config file supplying defaults (CLI flags override it).
    #[arg(long)]
    config: Option<PathBuf>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.json_logs);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    match cli.command {
        Command::Proxy(args) => {
            let cfg = resolve_proxy_config(args)?;
            runtime.block_on(chain_chaos_proxy::run(cfg))?;
        }
    }
    Ok(())
}

fn resolve_proxy_config(args: ProxyArgs) -> Result<ProxyConfig> {
    let file = match &args.config {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading config file {}", path.display()))?;
            FileConfig::from_toml_str(&text).context("parsing config file")?
        }
        None => FileConfig::default(),
    };

    let upstream = args
        .upstream
        .or(file.upstream)
        .context("no upstream provided: pass --upstream or set `upstream` in the config file")?;

    let upstream_ws = args.upstream_ws.or(file.upstream_ws);

    // CLI listen has a default, so it always wins unless left at default and the
    // file provides one. We treat an explicit flag as authoritative; the file
    // only fills in when the flag is at its default value.
    let listen = if args.listen == default_listen() {
        file.listen.unwrap_or(args.listen)
    } else {
        args.listen
    };

    let log_bodies = args.log_bodies || file.log_bodies.unwrap_or(false);

    ProxyConfig::new(upstream, upstream_ws, listen, log_bodies).map_err(Into::into)
}

fn default_listen() -> SocketAddr {
    "127.0.0.1:8545".parse().expect("valid default addr")
}

fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,chain_chaos=info"));
    let registry = tracing_subscriber::registry().with(filter);
    if json {
        registry.with(tracing_subscriber::fmt::layer().json()).init();
    } else {
        registry.with(tracing_subscriber::fmt::layer()).init();
    }
}
