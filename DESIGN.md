# chain-chaos — Design

> Status: living document. Covers what exists today (the RPC chaos proxy, transport
> faults, and the deterministic scenario engine) and the seams left open for the
> chain-aware phases.

## Problem statement

Blockchain applications — indexers, watchers, relayers, bridges — are written
against an idealized RPC provider: requests succeed, blocks arrive in order, logs
are complete, and the head never moves backwards. Production is not like that.
Providers rate-limit, time out, and disconnect WebSockets; heads go briefly
stale; logs lag behind `newHeads`; chains reorganize.

These failures are precisely the ones that are hardest to reproduce, because they
depend on timing and on the provider's internal state, neither of which the
application controls. The usual answer — fork a chain and poke it — tests the
fork, not the failure path *between* the app and the chain.

**chain-chaos injects the failure directly.** It sits between the application and
a real RPC endpoint as a transparent proxy and makes the infrastructure misbehave
on purpose, deterministically, so a bug can be reproduced from a scenario plus a
seed.

The question it answers: *does my application stay correct when the blockchain and
its infrastructure behave badly?*

## Initial scope

In scope today:

- Transparent HTTP + WebSocket JSON-RPC pass-through (Phase 1).
- Transport-layer faults: latency (fixed/range), timeout, reject (synthetic
  JSON-RPC error, e.g. HTTP 429 rate-limit), connection drop, WebSocket
  disconnect (Phase 2).
- Deterministic, seeded fault decisions — same seed + same request sequence →
  same faults (Phase 2).
- A YAML scenario engine that turns individual faults into a time-ordered,
  reproducible experiment, with `recover` to return to healthy (Phase 3).

Explicitly *out* of the current scope, deferred to later phases as the roadmap
instructs:

- EVM-aware faults (stale head, delayed/missing/duplicated logs) — Phase 4.
- Multi-provider disagreement and failover — Phase 5.
- Reorg testing — Phase 6.
- Built-in assertions / test reports / `chain-chaos test` — Phases 7–8.

The design goal is that none of the above require re-architecting what exists:
they slot in behind the same forwarding seam.

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

- `crates/proxy` — the library. HTTP handler (`http.rs`) and WS relay (`ws.rs`)
  are the two **choke points**; every request passes through exactly one. Each
  parses a read-only `RpcView` (method + id only — never re-serialized onto the
  wire) and asks the `fault` engine for a `FaultDecision`.
- `crates/scenario` — the deterministic scenario engine. Parses a YAML scenario
  into time-ordered events and drives the proxy's *active rule set* over wall-clock
  time from a background scheduler. Independent of EVM semantics by design.
- `crates/cli` — the `chain-chaos` binary (`proxy`, `run`, `inspect`).

Key invariant: **transparency survives the pass path.** When no fault fires, the
original request/response bytes are forwarded untouched, which is what preserves
JSON-RPC ids and batch ordering without response-matching.

### Determinism

All randomness derives from a single seed via `ChaCha8Rng` (stable across
platforms, unlike `StdRng`). The engine logs the seed on startup so any run can
be replayed. Caveat: the RNG is one shared stream, so under concurrent requests
the *order* of draws is not deterministic; exact replay assumes a single client
driving requests in sequence. A keyed per-request RNG is the eventual fix.

### The fault seam (why chain-aware faults fit later)

Transport faults operate on request *shape* only (`RpcView`): they never decode
blockchain semantics. Chain-aware faults (stale heads, reorgs, missing logs) are
a separate, stateful engine that will sit behind the *same* `decide()` seam,
consuming parsed responses rather than just request shape. Keeping this layer
byte-oriented is what keeps that door open.

## Fault model

| Fault              | Transport | Effect                                                      |
|--------------------|-----------|-------------------------------------------------------------|
| `delay`            | http, ws  | Sleep (fixed or sampled range), then forward normally.      |
| `timeout`          | http      | Hold the request, then fail it without forwarding.          |
| `reject`           | http      | Return a synthetic JSON-RPC error (e.g. 429 rate-limit).    |
| `drop`             | http      | Close with no response body.                                |
| `ws_disconnect`    | ws        | Tear the WebSocket down after a delay (tests reconnect).    |

Each rule carries a matcher (methods + transport), a `probability` (rolled per
matching request), and exactly one action. First matching rule wins.

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

Every activation/deactivation is logged with a timestamp, forming the timeline a
developer uses to correlate their application's misbehaviour with the injected
fault.

## Non-goals

- **Not a blockchain node or fork.** chain-chaos manipulates the *observations*
  available to an application; it does not implement consensus or execution.
- **Not a general HTTP proxy.** It understands just enough JSON-RPC to log method
  and id and to synthesize well-formed error responses.
- **No premature abstraction.** One seam, one decision point. A second consumer
  (the chain-aware engine) will motivate abstraction — not speculation.
