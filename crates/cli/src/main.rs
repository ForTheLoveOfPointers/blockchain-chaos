//! `chain-chaos` command-line entrypoint.
//!
//! Subcommands:
//! - `proxy` — transparent HTTP + WebSocket pass-through, with optional static
//!   fault rules from a TOML `--config`.
//! - `run` — drive the proxy through a deterministic YAML scenario, injecting
//!   faults that change over wall-clock time.
//! - `inspect` — compile a scenario and print its timeline without running it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use chain_chaos_proxy::{AppState, FileConfig, ProxyConfig};
use chain_chaos_scenario::{resolve_seed, Scenario};
use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Parser)]
#[command(
    name = "chain-chaos",
    version,
    about = "Chaos testing for blockchain infrastructure"
)]
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
    /// Run the proxy driven by a deterministic YAML scenario.
    Run(RunArgs),
    /// Compile a scenario and print its timeline without running it.
    Inspect(InspectArgs),
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

#[derive(clap::Args)]
struct RunArgs {
    /// Path to the YAML scenario file.
    scenario: PathBuf,

    /// Upstream HTTP(S) JSON-RPC endpoint to forward to.
    #[arg(long)]
    upstream: String,

    /// Upstream WS(S) endpoint. Defaults to `--upstream` with the scheme swapped.
    #[arg(long)]
    upstream_ws: Option<String>,

    /// Local address to listen on.
    #[arg(long, default_value = "127.0.0.1:8545")]
    listen: SocketAddr,

    /// Log full request/response bodies (verbose).
    #[arg(long)]
    log_bodies: bool,

    /// Override the scenario's seed (for a fresh random sequence).
    #[arg(long)]
    seed: Option<u64>,
}

#[derive(clap::Args)]
struct InspectArgs {
    /// Path to the YAML scenario file.
    scenario: PathBuf,

    /// Override the scenario's seed before printing.
    #[arg(long)]
    seed: Option<u64>,
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
        Command::Run(args) => {
            runtime.block_on(run_scenario(args))?;
        }
        Command::Inspect(args) => {
            let scenario = load_scenario(&args.scenario)?;
            let seed = resolve_seed(args.seed, &scenario);
            let timeline = scenario.compile(seed).context("compiling scenario")?;
            print!("{}", timeline.describe());
        }
    }
    Ok(())
}

async fn run_scenario(args: RunArgs) -> Result<()> {
    let scenario = load_scenario(&args.scenario)?;
    let seed = resolve_seed(args.seed, &scenario);
    let timeline = scenario.compile(seed).context("compiling scenario")?;
    info!(
        target: "chain_chaos",
        scenario = %timeline.name,
        seed,
        steps = timeline.steps.len(),
        "scenario loaded (reproduce with --seed {seed})"
    );

    let cfg = ProxyConfig::new(
        args.upstream,
        args.upstream_ws,
        args.listen,
        args.log_bodies,
    )?;
    let engine = Arc::new(timeline.build_engine());
    let state = AppState::with_engine(cfg, engine.clone())?;

    tokio::spawn(chain_chaos_scenario::drive(engine, timeline));

    chain_chaos_proxy::serve(state).await
}

fn load_scenario(path: &PathBuf) -> Result<Scenario> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading scenario file {}", path.display()))?;
    Scenario::from_yaml_str(&text).with_context(|| format!("parsing scenario {}", path.display()))
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

    let listen = if args.listen == default_listen() {
        file.listen.unwrap_or(args.listen)
    } else {
        args.listen
    };

    let log_bodies = args.log_bodies || file.log_bodies.unwrap_or(false);

    let mut cfg = ProxyConfig::new(upstream, upstream_ws, listen, log_bodies)?;
    if let Some(faults) = file.faults {
        cfg = cfg.with_faults(faults);
    }
    Ok(cfg)
}

fn default_listen() -> SocketAddr {
    "127.0.0.1:8545".parse().expect("valid default addr")
}

fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,chain_chaos=info"));
    let registry = tracing_subscriber::registry().with(filter);
    if json {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        registry.with(tracing_subscriber::fmt::layer()).init();
    }
}
