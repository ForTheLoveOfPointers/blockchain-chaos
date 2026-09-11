//! Proxy configuration: the upstream to forward to, the address to listen on,
//! and any static fault rules. A TOML file supplies defaults; CLI values win.

use std::net::SocketAddr;

use serde::Deserialize;

use crate::fault::config::FaultConfig;

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub upstream_http: String,
    pub upstream_ws: String,
    pub listen: SocketAddr,
    pub log_bodies: bool,
    pub faults: FaultConfig,
}

impl ProxyConfig {
    pub fn new(
        upstream_http: String,
        upstream_ws: Option<String>,
        listen: SocketAddr,
        log_bodies: bool,
    ) -> Result<Self, ConfigError> {
        let upstream_ws = match upstream_ws {
            Some(ws) => ws,
            None => derive_ws_url(&upstream_http)?,
        };
        Ok(Self {
            upstream_http,
            upstream_ws,
            listen,
            log_bodies,
            faults: FaultConfig::default(),
        })
    }

    pub fn with_faults(mut self, faults: FaultConfig) -> Self {
        self.faults = faults;
        self
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub upstream: Option<String>,
    pub upstream_ws: Option<String>,
    pub listen: Option<SocketAddr>,
    pub log_bodies: Option<bool>,
    pub faults: Option<FaultConfig>,
}

impl FileConfig {
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        toml::from_str(s).map_err(|e| ConfigError::Toml(e.to_string()))
    }
}

pub fn derive_ws_url(http_url: &str) -> Result<String, ConfigError> {
    let mut parsed =
        url::Url::parse(http_url).map_err(|e| ConfigError::Url(format!("{http_url}: {e}")))?;
    let ws_scheme = match parsed.scheme() {
        "http" => "ws",
        "https" => "wss",
        "ws" | "wss" => return Ok(parsed.to_string()),
        other => {
            return Err(ConfigError::Url(format!(
                "unsupported upstream scheme `{other}` (expected http/https/ws/wss)"
            )))
        }
    };
    parsed
        .set_scheme(ws_scheme)
        .map_err(|_| ConfigError::Url(format!("could not set scheme on {http_url}")))?;
    Ok(parsed.to_string())
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid url: {0}")]
    Url(String),
    #[error("invalid TOML config: {0}")]
    Toml(String),
    #[error("missing required config value: {0}")]
    Missing(&'static str),
    #[error("invalid fault config: {0}")]
    Fault(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_ws_from_http() {
        assert_eq!(
            derive_ws_url("http://127.0.0.1:8545").unwrap(),
            "ws://127.0.0.1:8545/"
        );
        assert_eq!(
            derive_ws_url("https://example.com/rpc").unwrap(),
            "wss://example.com/rpc"
        );
    }

    #[test]
    fn passes_through_ws_scheme() {
        assert_eq!(
            derive_ws_url("ws://127.0.0.1:8545").unwrap(),
            "ws://127.0.0.1:8545/"
        );
    }

    #[test]
    fn rejects_unknown_scheme() {
        assert!(derive_ws_url("ftp://nope").is_err());
    }
}
