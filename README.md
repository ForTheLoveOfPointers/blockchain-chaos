# chain-chaos

**Chaos testing for blockchain infrastructure.**

```
Application
    |
    v
chain-chaos
    |
    v
Ethereum
```

`chain-chaos` sits between an EVM application and a real RPC endpoint and lets you
reproduce realistic infrastructure failures — RPC latency, timeouts, dropped
connections, rate limits, WebSocket disconnects — **deterministically**, from a
scenario plus a seed. Instead of forking a chain and testing the fork, you inject
the failure directly on the path between your app and the chain.

The question it answers: *does my application stay correct when the blockchain and
its infrastructure behave badly?*

## Try it

Run a local node, and a small watcher application, through a chaos scenario:

```sh
# 1. a node with a moving head
anvil --block-time 1

# 2. the proxy, driven by a scenario: eth_blockNumber is rejected for 1s, then recovers
cargo run -p chain-chaos -- run scenarios/rpc-recovery.yaml --upstream http://127.0.0.1:8545 --listen 127.0.0.1:8546

# 3. an example app pointed at the proxy — watch it survive the outage and catch up
cargo run -p block-watcher -- --rpc-url http://127.0.0.1:8546 --interval 500ms
```

Every fault derives from the scenario's `seed`, so the run reproduces exactly.
Preview a scenario's timeline without running it:

```sh
cargo run -p chain-chaos -- inspect scenarios/rpc-recovery.yaml
```

```
scenario: rpc-recovery
seed: 12345
  @       0s  [1 active]  delay 3s
  @       5s  [2 active]  ws disconnect after 500ms
  @       8s  [3 active]  reject http=429 code=-32005
  @      15s  [0 active]  recover → pass-through
```

## What it can do today

- **Transparent proxy** (Phase 1): HTTP + WebSocket JSON-RPC pass-through. Point an
  existing app at it and it behaves identically to talking to the node directly,
  with every request logged. JSON-RPC ids and batch ordering are preserved exactly.
- **Transport faults** (Phase 2): fixed/random latency, timeout, reject (synthetic
  JSON-RPC error, e.g. HTTP 429 rate-limit), connection drop, WebSocket disconnect —
  matched by method and transport, rolled against a deterministic seed.
- **Scenario engine** (Phase 3): a YAML scenario turns individual faults into a
  time-ordered, reproducible experiment, with `recover` to return to healthy.

Chain-aware faults (stale heads, reorgs, missing logs), multi-provider chaos, and
built-in assertions are on the roadmap — see [`DESIGN.md`](DESIGN.md) and
[`blockchain-chaos-roadmap.txt`](blockchain-chaos-roadmap.txt).

## Two ways to inject faults

**Static rules** — a TOML config of always-on rules (see [`examples/faults.toml`](examples/faults.toml)):

```sh
cargo run -p chain-chaos -- proxy --config examples/faults.toml
```

**Scenarios** — faults that change over time (see [`scenarios/`](scenarios)):

```yaml
name: rpc-recovery
seed: 12345
events:
  - at: startup
    delay: { methods: [eth_getLogs], duration: 3s }
  - after: 5s
    disconnect: { after: 500ms }
  - after: 8s
    reject: { methods: [eth_sendRawTransaction], probability: 0.5, http_status: 429 }
  - after: 15s
    recover: true
```

## Layout

```
crates/
  proxy/     # HTTP + WS pass-through, fault engine, config, logging
  scenario/  # deterministic YAML scenario engine (timeline + driver)
  cli/       # `chain-chaos` binary: proxy / run / inspect
examples/
  faults.toml       # static fault config
  block-watcher/    # example app + end-to-end chaos test
scenarios/          # example scenarios
DESIGN.md           # problem statement, architecture, fault model, non-goals
```

## Development

```sh
cargo test --workspace     # unit + integration tests
cargo clippy --workspace --all-targets
```

Integration tests that need a node spawn `anvil` (from
[Foundry](https://book.getfoundry.sh/)) and **skip** — rather than fail — if it is
not on `PATH`, so they stay CI-friendly.

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE) at your option.
