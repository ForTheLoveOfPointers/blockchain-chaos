# chain-chaos design

> Status: living document. Covers what exists today (the RPC chaos proxy, transport
> faults, response-rewriting chain faults, multi-provider clusters, reorg modelling,
> and the deterministic scenario engine) and the seams left open for the assertion
> phases.

## Problem statement

Blockchain applications (indexers, watchers, relayers, bridges) are written against
an idealized RPC provider: requests succeed, blocks arrive in order, logs are
complete, and the head never moves backwards. Production is not like that. Providers
rate-limit, time out, and disconnect WebSockets; heads go briefly stale; logs lag
behind `newHeads`; chains reorganize.

These failures are the hardest to reproduce, because they depend on timing and on
the provider's internal state, neither of which the application controls. The usual
answer, forking a chain and poking it, tests the fork, not the failure path
*between* the app and the chain.

**chain-chaos injects the failure directly.** It sits between the application and a
real RPC endpoint as a transparent proxy and makes the infrastructure misbehave on
purpose, deterministically, so a bug can be reproduced from a scenario plus a seed.

The question it answers: *does my application stay correct when the blockchain and
its infrastructure behave badly?*

## Initial scope

In scope today:

- Transparent HTTP and WebSocket JSON-RPC pass-through (Phase 1).
- Transport-layer faults: latency (fixed or range), timeout, reject (a synthetic
  JSON-RPC error such as HTTP 429), connection drop, WebSocket disconnect (Phase 2).
- Deterministic, seeded fault decisions: the same seed and request sequence produce
  the same faults (Phase 2).
- A YAML scenario engine that turns individual faults into a time-ordered,
  reproducible experiment, with `recover` to return to healthy (Phase 3).
- EVM-aware faults that rewrite the upstream response: stale head, missing logs,
  malformed result (Phase 4).
- Multi-provider chaos: one independent proxy per RPC provider, each with its own
  upstream and fault set, for testing failover and provider disagreement (Phase 5).
- Same-height reorg modelling: the top N blocks keep their numbers but take new
  hashes, a re-linked parent chain, removed logs, and disappearing transactions,
  over HTTP and `newHeads`, converging on `recover` (Phase 6).
- Assertions and a verdict: a scenario carries an `assertions:` block, and
  `chain-chaos test` runs it, drives an application through it, and reports a
  deterministic pass/fail, exiting non-zero on failure (Phase 8, wire-observable
  subset).

Out of scope for now, deferred deliberately:

- Saved failure artifacts (`timeline.json`, request/response logs) and richer
  app-state assertions (no-duplicate-events and the like), which need the
  application's cooperation rather than only the wire.

None of the above required re-architecting what exists: they slot in behind the
same forwarding seam.

## Architecture

```
Application
    |
    v                      +-----------------------------+
chain-chaos proxy  ------> |  choke point (http / ws)    |
    |                      |    1. RpcView::parse(bytes) |
    v                      |    2. fault engine .decide()|
Ethereum RPC provider      |    3. pass | delay | reject |
                           +-----------------------------+
```

Crates:

- `crates/proxy`, the library. The HTTP handler (`http.rs`) and WS relay (`ws.rs`)
  are the two **choke points**; every request passes through exactly one. Each
  parses a read-only `RpcView` (method and id only, never re-serialized onto the
  wire) and asks the `fault` engine for a `FaultDecision`.
- `crates/scenario`, the deterministic scenario engine. It parses a YAML scenario
  into time-ordered events and drives the proxy's *active rule set* over wall-clock
  time from a background task. Independent of EVM semantics by design. It also
  compiles the scenario's `assertions:` and evaluates them against observed traffic.
- `crates/cli`, the `chain-chaos` binary (`proxy`, `run`, `test`, `cluster`,
  `inspect`).

### Assertions (Phase 8)

`chain-chaos test` turns a scenario into a pass/fail experiment. The proxy carries
an opt-in `Observations` recorder (off for a plain proxy, so there is no cost and no
unbounded growth) that captures, at the same two choke points, what the client was
*served*: the delivered head sequence and each delivery's outcome. After the
scenario plays out, the engine evaluates the scenario's assertions against that
record plus the true upstream head, snapshotted when the catch-up window closes
(`recover_at + within`) so the tip compared against is the one the client was
actually racing rather than a later, higher one. Because it observes only
the wire, any application works unchanged — the cost is that it sees the stimulus,
not the application's internal state, so invariants like "no duplicate events" are
left to a later phase that the application opts into.

Key invariant: transparency survives the pass path. When no fault fires, the
original request and response bytes are forwarded untouched, which preserves
JSON-RPC ids and batch ordering without response-matching.

### Multi-provider (Phase 5)

An application that talks to several RPC providers is modelled by running one proxy
per provider, each with its own upstream, listen address, and fault set
(`crates/proxy/src/cluster.rs`). A provider is the same single-upstream proxy
described above; the cluster only spawns several and joins them. The fault engine,
scenario engine, and transport paths stay untouched: provider disagreement is an
emergent property of independently faulted proxies, not a new mechanism. The
application points each provider URL at the matching listen address, and its
failover logic meets, say, a provider whose head lags three blocks behind the
others.

### Determinism

All randomness derives from a single seed via `ChaCha8Rng` (stable across platforms,
unlike `StdRng`). The engine logs the seed on startup so any run can be replayed. Each
fault roll is derived from the seed, transport, request method and JSON-RPC id, that
request key's occurrence counter, and the rule index. This keeps a request's
decision stable when concurrent requests arrive in a different order. Reusing the
same method and id is supported, but its occurrence counter is assigned in arrival
order, so callers should use unique JSON-RPC ids when they need to identify repeated
requests in a replay.

### The fault seam (two phases)

The engine runs in two phases against the same rule set. `decide()` runs *before*
forwarding and returns a `FaultDecision` for request-side transport faults.
`intercept()` runs *after* forwarding and returns rewritten response bytes for
chain-aware faults. A rule belongs to exactly one phase (`Action::is_response`), so
its probability is rolled once, in the phase that owns it, and the transport
pass-path stays byte-oriented and untouched.

Most chain faults parse and rewrite the JSON-RPC `result` but hold no cross-request
state, which keeps them deterministic and cheap. The reorg fault is the exception:
it is the one fault that needs state, so it carries an `Arc`-shared model (fork
point, depth, flags) inside its `Action`. Because the scenario driver clones the
rule set at each transition, the `Arc` is shared and the fork point stays pinned for
the life of the reorg; when `recover` drops the rule, the model is dropped with it
and the real hashes flow again.

### Reorg model (Phase 6)

A reorg is synthesized by rewriting the upstream's *own* responses, so the
alternative blocks are the real blocks with a rewritten identity, faithful to what a
same-height reorg produces without simulating consensus. The fork point is pinned
lazily from the first observed head (`eth_blockNumber` or a `newHeads` notification)
as `head - depth`. Blocks `fork+1..=fork+depth` then take a deterministic
alternative hash (a seeded 32-byte value, so replays match) and a re-linked
`parentHash`; the block just above the branch has its `parentHash` re-pointed onto
the alt tip, so a consumer walking parent links from the head sees one consistent
history. `eth_getLogs` drops or re-hashes logs in the range, receipts and
transactions in the range disappear, and `newHeads` frames are rewritten in the WS
relay the same way. The reorg is connection-independent by design, so every consumer
sees the same canonical chain. That is what lets a Phase 5 cluster express provider
disagreement: reorg one provider, not another.

## Fault model

| Fault              | Transport | Effect                                                      |
|--------------------|-----------|-------------------------------------------------------------|
| `delay`            | http, ws  | Sleep (fixed or sampled range), then forward normally.      |
| `timeout`          | http      | Hold the request, then fail it without forwarding.          |
| `reject`           | http      | Return a synthetic JSON-RPC error (e.g. 429 rate-limit).    |
| `drop`             | http      | Close with no response body.                                |
| `ws_disconnect`    | ws        | Tear the WebSocket down after a delay (tests reconnect).    |
| `stale_head`       | http      | Rewrite `eth_blockNumber` to report N blocks behind reality.|
| `missing_logs`     | http      | Rewrite `eth_getLogs` to return an empty result.            |
| `malformed`        | http      | Rewrite the result of the named methods into garbage.       |
| `reorg`            | http, ws  | Replace the top N blocks with a re-linked alternative branch (hashes, parents, removed logs, disappearing txs, `newHeads`). |

Each rule carries a matcher (methods and transport), a `probability` (rolled per
matching request), and exactly one action. First matching rule wins. The first five
faults act on the request (`decide`, pre-forward); the rest rewrite the upstream
response (`intercept`, post-forward, and for `reorg` the WS relay too).

## Example scenario

```yaml
name: rpc-recovery
seed: 12345
events:
  - at: startup
    delay: { methods: [eth_getLogs], duration: 3s }
  - after: 5s
    disconnect: { after: 500ms }      # tear down WS subscriptions
  - after: 8s
    reject: { methods: [eth_sendRawTransaction], http_status: 429, probability: 0.5 }
  - after: 15s
    recover: true                     # back to healthy pass-through
```

Run it:

```sh
chain-chaos run scenarios/rpc-recovery.yaml --upstream http://127.0.0.1:8545
```

Every activation and deactivation is logged with a timestamp, forming the timeline a
developer uses to correlate their application's misbehaviour with the injected fault.

## Non-goals

- **Not a blockchain node or fork.** chain-chaos manipulates the *observations*
  available to an application; it does not implement consensus or execution. The
  reorg fault rewrites observed block identities, it does not re-execute the
  alternative branch, so state (`eth_call`, balances) is not forked.
- **Not a general HTTP proxy.** It understands just enough JSON-RPC to log method
  and id and to synthesize well-formed error responses.
- **No premature abstraction.** One rule set, two phases (`decide` pre-forward,
  `intercept` post-forward) on the same engine. The stateful reorg fault fits within
  it by carrying its own `Arc`-shared model, rather than forcing a third engine.
  Splitting the engine stays unjustified until a fault needs it.
