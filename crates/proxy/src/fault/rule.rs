//! Runtime fault types: the matcher, the action, and the matching logic. These
//! are the validated counterparts of the wire types in [`super::config`].

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::fault::reorg::ReorgHandle;
use crate::rpc::RpcView;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Http,
    Ws,
}

#[derive(Debug, Clone)]
pub struct Rule {
    pub name: Option<String>,
    pub matcher: Matcher,
    pub probability: f64,
    pub action: Action,
}

#[derive(Debug, Clone)]
pub struct Matcher {
    pub methods: Option<Vec<String>>,
    pub transport: TransportMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMatch {
    Http,
    Ws,
    Both,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    Delay(DelaySpec),
    Timeout(Duration),
    Reject(RejectSpec),
    Drop,
    WsDisconnect(Duration),
    StaleHead { lag: u64 },
    MissingLogs,
    Malformed,
    Reorg(ReorgHandle),
}

impl Action {
    pub fn is_response(&self) -> bool {
        matches!(
            self,
            Action::StaleHead { .. } | Action::MissingLogs | Action::Malformed | Action::Reorg(_)
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DelaySpec {
    Fixed(Duration),
    Range { min: Duration, max: Duration },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectSpec {
    pub http_status: u16,
    pub code: i64,
    pub message: String,
}

impl TransportMatch {
    pub fn accepts(self, transport: Transport) -> bool {
        matches!(
            (self, transport),
            (TransportMatch::Both, _)
                | (TransportMatch::Http, Transport::Http)
                | (TransportMatch::Ws, Transport::Ws)
        )
    }
}

impl Matcher {
    pub fn matches(&self, transport: Transport, view: &RpcView) -> bool {
        self.transport.accepts(transport) && self.matches_methods(view)
    }

    fn matches_methods(&self, view: &RpcView) -> bool {
        let Some(filter) = &self.methods else {
            return true;
        };
        let hit = |method: Option<&str>| method.is_some_and(|m| filter.iter().any(|f| f == m));
        match view {
            RpcView::Single(call) => hit(call.method.as_deref()),
            RpcView::Batch(calls) => calls.iter().any(|c| hit(c.method.as_deref())),
            RpcView::Unknown => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::RpcView;

    fn view(method: &str) -> RpcView {
        RpcView::parse(format!(r#"{{"method":"{method}","id":1}}"#).as_bytes())
    }

    #[test]
    fn transport_match_accepts() {
        assert!(TransportMatch::Both.accepts(Transport::Http));
        assert!(TransportMatch::Both.accepts(Transport::Ws));
        assert!(TransportMatch::Http.accepts(Transport::Http));
        assert!(!TransportMatch::Http.accepts(Transport::Ws));
    }

    #[test]
    fn method_filter_matches_named_method() {
        let m = Matcher {
            methods: Some(vec!["eth_getLogs".into()]),
            transport: TransportMatch::Both,
        };
        assert!(m.matches(Transport::Http, &view("eth_getLogs")));
        assert!(!m.matches(Transport::Http, &view("eth_call")));
    }

    #[test]
    fn empty_filter_matches_anything() {
        let m = Matcher {
            methods: None,
            transport: TransportMatch::Both,
        };
        assert!(m.matches(Transport::Http, &view("anything")));
        assert!(m.matches(Transport::Ws, &RpcView::Unknown));
    }

    #[test]
    fn transport_narrows_match() {
        let m = Matcher {
            methods: None,
            transport: TransportMatch::Ws,
        };
        assert!(!m.matches(Transport::Http, &view("eth_call")));
        assert!(m.matches(Transport::Ws, &view("eth_call")));
    }
}
