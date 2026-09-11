//! block-watcher example. Polls the head and logs it. Point it at a chain-chaos
//! proxy to watch it survive injected RPC faults.

use std::time::Duration;

use anyhow::{Context, Result};
use block_watcher::{WatchConfig, Watcher};

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mut rpc_url = "http://127.0.0.1:8545".to_string();
    let mut interval = Duration::from_secs(1);

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--rpc-url" => {
                rpc_url = args.next().context("--rpc-url needs a value")?;
            }
            "--interval" => {
                let raw = args.next().context("--interval needs a value")?;
                interval = humantime_parse(&raw)?;
            }
            "-h" | "--help" => {
                println!("usage: block-watcher --rpc-url <url> --interval <dur>");
                return Ok(());
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }

    let watcher = Watcher::new(WatchConfig {
        rpc_url,
        poll_interval: interval,
    });

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(watcher.run_forever());
    Ok(())
}

fn humantime_parse(s: &str) -> Result<Duration> {
    if let Some(ms) = s.strip_suffix("ms") {
        Ok(Duration::from_millis(ms.parse().context("bad millis")?))
    } else if let Some(secs) = s.strip_suffix('s') {
        Ok(Duration::from_secs(secs.parse().context("bad seconds")?))
    } else {
        anyhow::bail!("interval must end in `ms` or `s` (e.g. 500ms, 2s)")
    }
}
