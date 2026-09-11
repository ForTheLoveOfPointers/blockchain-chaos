//! Wire types for the YAML scenario format: a name, a seed, and time-triggered
//! events, each carrying one action. The only place that knows YAML field names.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    pub seed: Option<u64>,
    #[serde(default)]
    pub events: Vec<EventConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventConfig {
    pub at: Option<String>,
    pub after: Option<String>,

    pub delay: Option<DelayAction>,
    pub timeout: Option<TimeoutAction>,
    pub reject: Option<RejectAction>,
    pub drop: Option<DropAction>,
    pub disconnect: Option<DisconnectAction>,
    pub stale_head: Option<StaleHeadAction>,
    pub missing_logs: Option<MissingLogsAction>,
    pub malformed: Option<MalformedAction>,
    pub reorg: Option<ReorgAction>,
    pub recover: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportSel {
    Http,
    Ws,
    #[default]
    Both,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelayAction {
    pub methods: Option<Vec<String>>,
    #[serde(default)]
    pub transport: TransportSel,
    pub probability: Option<f64>,
    pub duration: Option<String>,
    pub min: Option<String>,
    pub max: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeoutAction {
    pub methods: Option<Vec<String>>,
    #[serde(default)]
    pub transport: TransportSel,
    pub probability: Option<f64>,
    pub duration: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejectAction {
    pub methods: Option<Vec<String>>,
    #[serde(default)]
    pub transport: TransportSel,
    pub probability: Option<f64>,
    pub http_status: Option<u16>,
    pub code: Option<i64>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DropAction {
    pub methods: Option<Vec<String>>,
    #[serde(default)]
    pub transport: TransportSel,
    pub probability: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisconnectAction {
    pub probability: Option<f64>,
    pub after: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaleHeadAction {
    pub probability: Option<f64>,
    pub blocks: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissingLogsAction {
    pub probability: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MalformedAction {
    pub methods: Vec<String>,
    pub probability: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReorgAction {
    pub depth: u64,
    pub remove_logs: Option<bool>,
    pub drop_transactions: Option<bool>,
    pub probability: Option<f64>,
}

impl Scenario {
    pub fn from_yaml_str(s: &str) -> Result<Self, super::ScenarioError> {
        serde_yaml::from_str(s).map_err(|e| super::ScenarioError::Parse(e.to_string()))
    }
}

impl From<TransportSel> for chain_chaos_proxy::fault::TransportMatch {
    fn from(t: TransportSel) -> Self {
        use chain_chaos_proxy::fault::TransportMatch;
        match t {
            TransportSel::Http => TransportMatch::Http,
            TransportSel::Ws => TransportMatch::Ws,
            TransportSel::Both => TransportMatch::Both,
        }
    }
}
