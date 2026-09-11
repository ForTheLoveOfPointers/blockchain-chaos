//! Faithful reorg modelling (Phase 6).
//!
//! A reorg is the one chain fault that needs cross-request state: the fork point
//! must stay pinned for the life of the reorg so every rewritten block, log, and
//! header chains onto the *same* alternative branch. The model presents a
//! same-height replacement — blocks `fork+1..=fork+depth` keep their numbers but
//! take new, deterministically derived hashes and a re-linked parent chain — so
//! an indexer that tracks block *hashes* detects a reorg of the configured depth
//! while one that only tracks numbers silently corrupts. The block just above
//! the branch (`fork+depth+1`) has its `parentHash` re-pointed onto the alt tip,
//! so a consumer walking parent links from the head sees one consistent history.
//!
//! Everything is synthesized by rewriting the upstream's *own* responses, so the
//! alternative blocks are the real blocks with a rewritten identity (new hash,
//! re-linked parent, optionally emptied transactions and removed logs) — faithful
//! to what a same-height reorg produces without simulating consensus. The fork is
//! pinned lazily from the first observed head (`eth_blockNumber` or a `newHeads`
//! notification); block, log, and receipt responses are left untouched until then,
//! which is why the shared-RNG single-client ordering caveat in [`super`] applies.
//! On `recover` the rule leaves the active set, this model is dropped, and the
//! real hashes flow again — the convergence half of the experiment.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Map, Value};

const UNSET: u64 = u64::MAX;

#[derive(Clone)]
pub struct ReorgHandle(Arc<ReorgModel>);

struct ReorgModel {
    seed: u64,
    depth: u64,
    remove_logs: bool,
    drop_transactions: bool,
    fork_point: AtomicU64,
}

impl std::fmt::Debug for ReorgHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reorg")
            .field("depth", &self.0.depth)
            .field("remove_logs", &self.0.remove_logs)
            .field("drop_transactions", &self.0.drop_transactions)
            .field("fork_point", &self.pinned())
            .finish()
    }
}

impl ReorgHandle {
    pub fn new(seed: u64, depth: u64, remove_logs: bool, drop_transactions: bool) -> Self {
        ReorgHandle(Arc::new(ReorgModel {
            seed,
            depth: depth.max(1),
            remove_logs,
            drop_transactions,
            fork_point: AtomicU64::new(UNSET),
        }))
    }

    pub fn depth(&self) -> u64 {
        self.0.depth
    }

    pub fn rewrite(&self, method: Option<&str>, response: &[u8]) -> Option<Vec<u8>> {
        match method? {
            "eth_blockNumber" => {
                self.observe_head(response);
                None
            }
            "eth_getBlockByNumber" | "eth_getBlockByHash" => self.rewrite_block(response),
            "eth_getLogs" => self.rewrite_logs(response),
            "eth_getTransactionReceipt" | "eth_getTransactionByHash" => self.rewrite_tx(response),
            _ => None,
        }
    }

    pub fn rewrite_newhead(&self, text: &str) -> Option<String> {
        let mut v: Value = serde_json::from_str(text).ok()?;
        if v.get("method").and_then(Value::as_str)? != "eth_subscription" {
            return None;
        }
        let number = v
            .pointer("/params/result/number")
            .and_then(Value::as_str)
            .and_then(parse_hex_u64)?;
        let fork = self.pin_from_head(number);
        let header = v.pointer_mut("/params/result")?.as_object_mut()?;
        if self.rewrite_header(fork, header) {
            serde_json::to_string(&v).ok()
        } else {
            None
        }
    }

    fn observe_head(&self, response: &[u8]) {
        if let Some(head) = serde_json::from_slice::<Value>(response)
            .ok()
            .and_then(|body| {
                body.get("result")
                    .and_then(Value::as_str)
                    .and_then(parse_hex_u64)
            })
        {
            self.pin_from_head(head);
        }
    }

    fn rewrite_block(&self, response: &[u8]) -> Option<Vec<u8>> {
        let fork = self.pinned()?;
        let mut body: Value = serde_json::from_slice(response).ok()?;
        if body.get("result")?.is_null() {
            return None;
        }
        let header = body.get_mut("result")?.as_object_mut()?;
        if self.rewrite_header(fork, header) {
            serde_json::to_vec(&body).ok()
        } else {
            None
        }
    }

    fn rewrite_logs(&self, response: &[u8]) -> Option<Vec<u8>> {
        let fork = self.pinned()?;
        let (lo, hi) = self.range(fork);
        let mut body: Value = serde_json::from_slice(response).ok()?;
        let logs = body.get_mut("result")?.as_array_mut()?;
        let mut changed = false;
        if self.0.remove_logs {
            let before = logs.len();
            logs.retain(|log| !in_range(log_number(log), lo, hi));
            changed = logs.len() != before;
        } else {
            for log in logs.iter_mut() {
                let n = log_number(log);
                if in_range(n, lo, hi) {
                    if let Some(obj) = log.as_object_mut() {
                        obj.insert("blockHash".to_string(), alt_hash(self.0.seed, n.unwrap()));
                        changed = true;
                    }
                }
            }
        }
        changed.then(|| serde_json::to_vec(&body).ok()).flatten()
    }

    fn rewrite_tx(&self, response: &[u8]) -> Option<Vec<u8>> {
        let fork = self.pinned()?;
        let (lo, hi) = self.range(fork);
        let mut body: Value = serde_json::from_slice(response).ok()?;
        if body.get("result")?.is_null() {
            return None;
        }
        let number = body
            .get("result")?
            .get("blockNumber")
            .and_then(Value::as_str)
            .and_then(parse_hex_u64)?;
        if !(lo..=hi).contains(&number) {
            return None;
        }
        if self.0.drop_transactions {
            body["result"] = Value::Null;
        } else if let Some(obj) = body.get_mut("result").and_then(Value::as_object_mut) {
            obj.insert("blockHash".to_string(), alt_hash(self.0.seed, number));
        }
        serde_json::to_vec(&body).ok()
    }

    fn rewrite_header(&self, fork: u64, header: &mut Map<String, Value>) -> bool {
        let Some(number) = header
            .get("number")
            .and_then(Value::as_str)
            .and_then(parse_hex_u64)
        else {
            return false;
        };
        let (lo, hi) = self.range(fork);
        if (lo..=hi).contains(&number) {
            header.insert("hash".to_string(), alt_hash(self.0.seed, number));
            if number > lo {
                header.insert("parentHash".to_string(), alt_hash(self.0.seed, number - 1));
            }
            if self.0.drop_transactions && header.contains_key("transactions") {
                header.insert("transactions".to_string(), Value::Array(Vec::new()));
            }
            true
        } else if number == hi + 1 {
            header.insert("parentHash".to_string(), alt_hash(self.0.seed, hi));
            true
        } else {
            false
        }
    }

    fn range(&self, fork: u64) -> (u64, u64) {
        (fork + 1, fork + self.0.depth)
    }

    fn pinned(&self) -> Option<u64> {
        match self.0.fork_point.load(Ordering::Relaxed) {
            UNSET => None,
            v => Some(v),
        }
    }

    fn pin_from_head(&self, head: u64) -> u64 {
        let fork = head.saturating_sub(self.0.depth);
        match self
            .0
            .fork_point
            .compare_exchange(UNSET, fork, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => fork,
            Err(existing) => existing,
        }
    }
}

fn alt_hash(seed: u64, number: u64) -> Value {
    let mix = seed ^ number.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut rng = ChaCha8Rng::seed_from_u64(mix);
    let mut s = String::with_capacity(66);
    s.push_str("0x");
    for _ in 0..32 {
        let byte: u8 = rng.random();
        s.push_str(&format!("{byte:02x}"));
    }
    Value::String(s)
}

fn log_number(log: &Value) -> Option<u64> {
    log.get("blockNumber")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64)
}

fn in_range(number: Option<u64>, lo: u64, hi: u64) -> bool {
    matches!(number, Some(n) if (lo..=hi).contains(&n))
}

fn parse_hex_u64(s: &str) -> Option<u64> {
    let trimmed = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(trimmed, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pinned_handle(depth: u64, head: u64) -> ReorgHandle {
        let h = ReorgHandle::new(1, depth, true, true);
        h.pin_from_head(head);
        h
    }

    fn block(number: u64, hash: &str, parent: &str) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","id":1,"result":{{"number":"0x{number:x}","hash":"{hash}","parentHash":"{parent}","transactions":["0xabc"]}}}}"#
        )
    }

    fn block_result(bytes: &[u8]) -> Value {
        serde_json::from_slice::<Value>(bytes).unwrap()["result"].clone()
    }

    #[test]
    fn unpinned_model_passes_blocks_through() {
        let h = ReorgHandle::new(1, 2, true, true);
        assert!(h
            .rewrite(
                Some("eth_getBlockByNumber"),
                block(10, "0x1", "0x0").as_bytes()
            )
            .is_none());
    }

    #[test]
    fn head_observation_pins_fork_point() {
        let h = ReorgHandle::new(1, 3, true, true);
        h.rewrite(
            Some("eth_blockNumber"),
            br#"{"jsonrpc":"2.0","id":1,"result":"0x14"}"#,
        );
        assert_eq!(h.pinned(), Some(0x14 - 3));
    }

    #[test]
    fn block_in_forked_range_gets_new_hash_same_number() {
        let h = pinned_handle(2, 20);
        let out = h
            .rewrite(
                Some("eth_getBlockByNumber"),
                block(20, "0xreal", "0xreal19").as_bytes(),
            )
            .expect("rewritten");
        let result = block_result(&out);
        assert_eq!(result["number"], "0x14");
        assert_ne!(result["hash"], "0xreal");
        assert_eq!(result["transactions"], serde_json::json!([]));
    }

    #[test]
    fn alt_parent_chains_across_the_branch() {
        let h = pinned_handle(2, 20);
        let top = block_result(
            &h.rewrite(
                Some("eth_getBlockByNumber"),
                block(20, "0xr20", "0xr19").as_bytes(),
            )
            .unwrap(),
        );
        let below = block_result(
            &h.rewrite(
                Some("eth_getBlockByNumber"),
                block(19, "0xr19", "0xr18").as_bytes(),
            )
            .unwrap(),
        );
        assert_eq!(top["parentHash"], below["hash"]);
    }

    #[test]
    fn first_alt_block_keeps_real_parent() {
        let h = pinned_handle(2, 20);
        let first = block_result(
            &h.rewrite(
                Some("eth_getBlockByNumber"),
                block(19, "0xr19", "0xr18").as_bytes(),
            )
            .unwrap(),
        );
        assert_eq!(first["parentHash"], "0xr18");
    }

    #[test]
    fn block_above_branch_repoints_parent_only() {
        let h = pinned_handle(2, 20);
        let above = block_result(
            &h.rewrite(
                Some("eth_getBlockByNumber"),
                block(21, "0xr21", "0xr20").as_bytes(),
            )
            .unwrap(),
        );
        assert_eq!(above["hash"], "0xr21");
        assert_ne!(above["parentHash"], "0xr20");
    }

    #[test]
    fn block_below_fork_is_untouched() {
        let h = pinned_handle(2, 20);
        assert!(h
            .rewrite(
                Some("eth_getBlockByNumber"),
                block(10, "0xr10", "0xr9").as_bytes()
            )
            .is_none());
    }

    #[test]
    fn logs_in_range_are_removed() {
        let h = pinned_handle(2, 20);
        let resp =
            br#"{"jsonrpc":"2.0","id":1,"result":[{"blockNumber":"0x14"},{"blockNumber":"0xa"}]}"#;
        let out = h.rewrite(Some("eth_getLogs"), resp).expect("rewritten");
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["result"].as_array().unwrap().len(), 1);
        assert_eq!(v["result"][0]["blockNumber"], "0xa");
    }

    #[test]
    fn logs_kept_when_remove_disabled() {
        let h = ReorgHandle::new(1, 2, false, true);
        h.pin_from_head(20);
        let resp =
            br#"{"jsonrpc":"2.0","id":1,"result":[{"blockNumber":"0x14","blockHash":"0xr"}]}"#;
        let out = h.rewrite(Some("eth_getLogs"), resp).expect("rewritten");
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["result"].as_array().unwrap().len(), 1);
        assert_ne!(v["result"][0]["blockHash"], "0xr");
    }

    #[test]
    fn receipt_in_range_disappears() {
        let h = pinned_handle(2, 20);
        let resp = br#"{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x14","transactionHash":"0xabc"}}"#;
        let out = h
            .rewrite(Some("eth_getTransactionReceipt"), resp)
            .expect("rewritten");
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert!(v["result"].is_null());
    }

    #[test]
    fn newhead_in_range_is_rewritten_and_pins() {
        let h = ReorgHandle::new(1, 2, true, true);
        let frame = r#"{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x1","result":{"number":"0x14","hash":"0xreal","parentHash":"0xreal19"}}}"#;
        let out = h.rewrite_newhead(frame).expect("rewritten");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["params"]["result"]["number"], "0x14");
        assert_ne!(v["params"]["result"]["hash"], "0xreal");
        assert_eq!(h.pinned(), Some(0x14 - 2));
    }

    #[test]
    fn same_seed_same_alt_hash() {
        assert_eq!(alt_hash(7, 100), alt_hash(7, 100));
        assert_ne!(alt_hash(7, 100), alt_hash(7, 101));
        assert_ne!(alt_hash(7, 100), alt_hash(8, 100));
    }
}
