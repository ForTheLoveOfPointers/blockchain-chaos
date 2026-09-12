//! `chain-chaos` command-line entrypoint: `proxy`, `run`, `cluster`, `inspect`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use chain_chaos_proxy::{AppState, ClusterConfig, FileConfig, Observations, ProxyConfig};
use chain_chaos_scenario::{evaluate_timeline, resolve_seed, Assertion, Ground, Scenario};
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
    /// Run a scenario, evaluate its assertions, and report pass/fail.
    Test(TestArgs),
    /// Run one proxy per provider from a multi-provider TOML config.
    Cluster(ClusterArgs),
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
struct TestArgs {
    /// Path to the YAML scenario file (its `assertions:` block is the test).
    scenario: PathBuf,

    /// Upstream HTTP(S) JSON-RPC endpoint to forward to.
    #[arg(long)]
    upstream: String,

    /// Upstream WS(S) endpoint. Defaults to `--upstream` with the scheme swapped.
    #[arg(long)]
    upstream_ws: Option<String>,

    /// Local address to listen on.
    #[arg(long, default_value = "127.0.0.1:8546")]
    listen: SocketAddr,

    /// Command that drives traffic through the proxy. `{rpc}` is replaced with
    /// the proxy's URL. Without it, point your application at the proxy yourself.
    #[arg(long)]
    app_cmd: Option<String>,

    /// Extra time to wait after the scenario ends before evaluating assertions.
    #[arg(long, default_value = "5s")]
    grace: String,

    /// Log full request/response bodies (verbose).
    #[arg(long)]
    log_bodies: bool,

    /// Override the scenario's seed.
    #[arg(long)]
    seed: Option<u64>,
}

#[derive(clap::Args)]
struct ClusterArgs {
    /// Path to the multi-provider TOML config.
    #[arg(long)]
    config: PathBuf,

    /// Override the cluster seed applied to providers without their own.
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
        Command::Test(args) => {
            let passed = runtime.block_on(run_test(args))?;
            if !passed {
                std::process::exit(1);
            }
        }
        Command::Cluster(args) => {
            runtime.block_on(run_cluster(args))?;
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

/// Run a scenario with observation on, drive traffic through it, then evaluate the
/// scenario's assertions against what the client was served. Returns whether every
/// assertion passed.
async fn run_test(args: TestArgs) -> Result<bool> {
    let scenario = load_scenario(&args.scenario)?;
    let seed = resolve_seed(args.seed, &scenario);
    let timeline = scenario.compile(seed).context("compiling scenario")?;

    if timeline.assertions.is_empty() {
        anyhow::bail!(
            "scenario `{}` has no `assertions:` block; nothing to test",
            timeline.name
        );
    }

    let grace = humantime::parse_duration(&args.grace).context("parsing --grace")?;
    let run_for = timeline.last_offset() + grace;

    info!(
        target: "chain_chaos",
        scenario = %timeline.name,
        seed,
        assertions = timeline.assertions.len(),
        "test starting (reproduce with --seed {seed})"
    );

    let cfg = ProxyConfig::new(
        args.upstream.clone(),
        args.upstream_ws.clone(),
        args.listen,
        args.log_bodies,
    )?;
    let engine = Arc::new(timeline.build_engine());
    let obs = Arc::new(Observations::new());
    let state = AppState::with_engine(cfg, engine.clone())?.with_observations(obs.clone());

    // The catch-up window closes at `recover_at + within`; read the upstream tip
    // then, not at the end of the run, so `catches_up` compares against the head
    // the client was actually racing rather than a later, higher one. The run
    // still lasts at least until that moment.
    let base = timeline.recover_at.unwrap_or(std::time::Duration::ZERO);
    let catch_deadline = timeline
        .assertions
        .iter()
        .filter_map(|a| match a {
            Assertion::CatchesUp { within } => Some(base + *within),
            _ => None,
        })
        .max();
    let run_until = catch_deadline.map_or(run_for, |d| run_for.max(d));

    let proxy_url = format!("http://{}", args.listen);
    let server = tokio::spawn(async move {
        let _ = chain_chaos_proxy::serve(state).await;
    });
    let clock = tokio::time::Instant::now();
    let driver = tokio::spawn(chain_chaos_scenario::drive(engine, timeline.clone()));

    // Give the proxy a moment to bind before traffic starts.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    // Drive traffic, either via the given command or by asking the operator to.
    let mut child = match &args.app_cmd {
        Some(cmd) => Some(spawn_app(cmd, &proxy_url)?),
        None => {
            info!(
                target: "chain_chaos",
                %proxy_url,
                "no --app-cmd: point your application at the proxy now"
            );
            None
        }
    };

    let upstream_head = match catch_deadline {
        Some(deadline) => {
            tokio::time::sleep_until(clock + deadline).await;
            let head = fetch_head(&args.upstream).await;
            tokio::time::sleep_until(clock + run_until).await;
            head
        }
        None => {
            tokio::time::sleep_until(clock + run_until).await;
            fetch_head(&args.upstream).await
        }
    };

    let ground = Ground {
        upstream_head,
        recover_at: timeline.recover_at,
    };
    let outcomes = evaluate_timeline(&timeline, &obs, &ground);

    if let Some(child) = child.as_mut() {
        let _ = child.start_kill();
    }
    driver.abort();
    server.abort();

    let all_passed = report(&timeline.name, seed, upstream_head, &outcomes);
    Ok(all_passed)
}

fn spawn_app(cmd: &str, proxy_url: &str) -> Result<tokio::process::Child> {
    let substituted = cmd.replace("{rpc}", proxy_url);
    let mut parts = substituted.split_whitespace();
    let program = parts
        .next()
        .context("--app-cmd is empty")?
        .to_string();
    let rest: Vec<String> = parts.map(str::to_owned).collect();
    info!(target: "chain_chaos", cmd = %substituted, "spawning app under test");
    tokio::process::Command::new(program)
        .args(rest)
        .env("CHAIN_CHAOS_RPC_URL", proxy_url)
        .spawn()
        .context("spawning --app-cmd")
}

async fn fetch_head(upstream: &str) -> Option<u64> {
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1
    });
    let resp = client.post(upstream).json(&body).send().await.ok()?;
    let json: serde_json::Value = resp.json().await.ok()?;
    let hex = json.get("result")?.as_str()?;
    u64::from_str_radix(hex.strip_prefix("0x").unwrap_or(hex), 16).ok()
}

fn report(
    name: &str,
    seed: u64,
    upstream_head: Option<u64>,
    outcomes: &[chain_chaos_scenario::AssertionOutcome],
) -> bool {
    let failed = outcomes.iter().filter(|o| !o.passed).count();
    println!("\nScenario: {name}");
    println!("Seed: {seed}");
    match upstream_head {
        Some(h) => println!("Upstream head (reference): {h}"),
        None => println!("Upstream head (reference): <unavailable>"),
    }
    println!();
    for o in outcomes {
        let tag = if o.passed { "PASS" } else { "FAIL" };
        println!("[{tag}] {} — {}", o.name, o.detail);
    }
    println!();
    if failed == 0 {
        println!("{} assertion(s) passed.", outcomes.len());
    } else {
        println!(
            "{failed} of {} assertion(s) failed. Reproduce with:\n    chain-chaos test <scenario> --upstream <url> --seed {seed}",
            outcomes.len()
        );
    }
    failed == 0
}

async fn run_cluster(args: ClusterArgs) -> Result<()> {
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("reading cluster config {}", args.config.display()))?;
    let mut cfg = ClusterConfig::from_toml_str(&text).context("parsing cluster config")?;
    if let Some(seed) = args.seed {
        cfg.seed = Some(seed);
    }
    chain_chaos_proxy::run_cluster(cfg).await
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
    // Diagnostics go to stderr so a command's real output on stdout (a `test`
    // verdict, an `inspect` timeline) can be read or piped on its own.
    let registry = tracing_subscriber::registry().with(filter);
    if json {
        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(std::io::stderr),
            )
            .init();
    } else {
        registry
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .init();
    }
}
