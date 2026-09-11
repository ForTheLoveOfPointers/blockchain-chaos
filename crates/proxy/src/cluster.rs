//! Multi-provider chaos (Phase 5).
//!
//! Models an application that talks to several RPC providers at once by running
//! one independent proxy per provider. Each provider has its own upstream,
//! listen address, and fault set, so a config can make provider A report a stale
//! head while B suffers an outage and C stays healthy — which is what exercises
//! an application's failover and provider-disagreement logic. Providers share
//! nothing but the process: each is the same single-upstream proxy the rest of
//! the crate already builds, so no fault, scenario, or transport code changes.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde::Deserialize;
use tracing::info;

use crate::config::{ConfigError, ProxyConfig};
use crate::fault::config::FaultConfig;
use crate::state::AppState;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub seed: Option<u64>,
    pub log_bodies: Option<bool>,
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    pub upstream: String,
    pub upstream_ws: Option<String>,
    pub listen: SocketAddr,
    pub log_bodies: Option<bool>,
    pub faults: Option<FaultConfig>,
}

impl ClusterConfig {
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        toml::from_str(s).map_err(|e| ConfigError::Toml(e.to_string()))
    }

    pub fn proxy_configs(&self) -> Result<Vec<(String, ProxyConfig)>, ConfigError> {
        if self.providers.is_empty() {
            return Err(ConfigError::Missing("providers (cluster config has none)"));
        }
        let mut out = Vec::with_capacity(self.providers.len());
        for (name, provider) in &self.providers {
            let mut faults = provider.faults.clone().unwrap_or_default();
            if faults.seed.is_none() {
                faults.seed = self.seed;
            }
            let log_bodies = provider.log_bodies.or(self.log_bodies).unwrap_or(false);
            let cfg = ProxyConfig::new(
                provider.upstream.clone(),
                provider.upstream_ws.clone(),
                provider.listen,
                log_bodies,
            )?
            .with_faults(faults);
            out.push((name.clone(), cfg));
        }
        Ok(out)
    }
}

pub async fn run_cluster(cfg: ClusterConfig) -> anyhow::Result<()> {
    let configs = cfg.proxy_configs()?;
    info!(target: "chain_chaos::cluster", providers = configs.len(), "starting provider cluster");

    let mut set = tokio::task::JoinSet::new();
    for (name, provider_cfg) in configs {
        let state = AppState::new(provider_cfg)?;
        set.spawn(async move {
            info!(
                target: "chain_chaos::cluster",
                provider = %name,
                listen = %state.cfg.listen,
                "provider proxy up"
            );
            let result = crate::serve(state).await;
            (name, result)
        });
    }

    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((name, Ok(()))) => {
                info!(target: "chain_chaos::cluster", provider = %name, "provider proxy stopped");
            }
            Ok((name, Err(e))) => return Err(e.context(format!("provider `{name}` failed"))),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(src: &str) -> ClusterConfig {
        ClusterConfig::from_toml_str(src).expect("valid cluster toml")
    }

    #[test]
    fn builds_a_config_per_provider() {
        let cfg = parse(
            r#"
            seed = 99

            [providers.healthy]
            upstream = "http://127.0.0.1:8545"
            listen = "127.0.0.1:9001"

            [providers.stale]
            upstream = "http://127.0.0.1:8545"
            listen = "127.0.0.1:9002"
            [providers.stale.faults]
            [[providers.stale.faults.rules]]
            stale_head = 4
            "#,
        );
        let configs = cfg.proxy_configs().expect("compiles");
        assert_eq!(configs.len(), 2);
        let by_name: BTreeMap<_, _> = configs.into_iter().collect();
        assert_eq!(by_name["healthy"].faults.rules.len(), 0);
        assert_eq!(by_name["stale"].faults.rules.len(), 1);
    }

    #[test]
    fn cluster_seed_fills_providers_without_one() {
        let cfg = parse(
            r#"
            seed = 7

            [providers.a]
            upstream = "http://127.0.0.1:8545"
            listen = "127.0.0.1:9001"

            [providers.b]
            upstream = "http://127.0.0.1:8545"
            listen = "127.0.0.1:9002"
            [providers.b.faults]
            seed = 123
            "#,
        );
        let by_name: BTreeMap<_, _> = cfg.proxy_configs().unwrap().into_iter().collect();
        assert_eq!(by_name["a"].faults.seed, Some(7));
        assert_eq!(by_name["b"].faults.seed, Some(123));
    }

    #[test]
    fn rejects_empty_cluster() {
        assert!(parse("seed = 1\n").proxy_configs().is_err());
    }
}
