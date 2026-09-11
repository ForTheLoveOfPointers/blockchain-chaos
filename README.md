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

`chain-chaos` sits between an EVM application and a real RPC endpoint and reproduces
realistic infrastructure failures: RPC latency, timeouts, dropped connections, rate
limits, WebSocket disconnects. Every run is deterministic, driven by a scenario plus
a seed. Instead of forking a chain and testing the fork, you inject the failure
directly on the path between your app and the chain.

The question it answers: *does my application stay correct when the blockchain and
its infrastructure behave badly?*

## Try it

Run a local node, and a small watcher application, through a chaos scenario:

```sh
# 1. a node with a moving head
anvil --block-time 1

# 2. the proxy, driven by a scenario: eth_blockNumber is rejected for 1s, then recovers
cargo run -p chain-chaos -- run scenarios/rpc-recovery.yaml --upstream http://127.0.0.1:8545 --listen 127.0.0.1:8546

# 3. an example app pointed at the proxy; watch it survive the outage and catch up
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

- **Transparent proxy** (Phase 1): HTTP and WebSocket JSON-RPC pass-through. Point an
  existing app at it and it behaves identically to talking to the node directly,
  with every request logged. JSON-RPC ids and batch ordering are preserved exactly.
- **Transport faults** (Phase 2): fixed and random latency, timeout, reject (a
  synthetic JSON-RPC error such as HTTP 429), connection drop, WebSocket disconnect.
  Rules match by method and transport and roll against a deterministic seed.
- **Scenario engine** (Phase 3): a YAML scenario turns individual faults into a
  time-ordered, reproducible experiment, with `recover` to return to healthy.
- **Chain-aware faults** (Phase 4): rewrite the upstream response. `stale_head`
  reports a head N blocks behind reality, `missing_logs` empties `eth_getLogs`, and
  `malformed` corrupts a method's result.
- **Multi-provider chaos** (Phase 5): run one independent proxy per RPC provider,
  each with its own upstream and fault set, so provider A can lag while B has an
  outage and C stays healthy. This is what exercises an application's failover and
  provider-disagreement logic (see [`examples/providers.toml`](examples/providers.toml)).
- **Reorgs** (Phase 6): a same-height reorg where the top N blocks keep their numbers
  but take new hashes and a re-linked parent chain, logs in the range are removed,
  and their transactions disappear, over HTTP and `newHeads`. An indexer that tracks
  block hashes detects a depth-N reorg; one that tracks only numbers silently
  corrupts. Driven by a scenario, it switches on and then converges on `recover`
  (see [`scenarios/reorg.yaml`](scenarios/reorg.yaml)).

Built-in assertions and CI integration are still on the roadmap. See
[`DESIGN.md`](DESIGN.md) and
[`blockchain-chaos-roadmap.txt`](blockchain-chaos-roadmap.txt).

## Ways to inject faults

**Static rules.** A TOML config of always-on rules (see [`examples/faults.toml`](examples/faults.toml)):

```sh
cargo run -p chain-chaos -- proxy --config examples/faults.toml
```

**Provider cluster.** One proxy per RPC provider, each independently faulted (see [`examples/providers.toml`](examples/providers.toml)):

```sh
cargo run -p chain-chaos -- cluster --config examples/providers.toml
```

**Scenarios.** Faults that change over time (see [`scenarios/`](scenarios)):

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
  scenario/  # deterministic YAML scenario engine
  cli/       # `chain-chaos` binary: proxy / run / cluster / inspect
examples/
  faults.toml       # static fault config
  providers.toml    # multi-provider cluster config
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
[Foundry](https://book.getfoundry.sh/)) and skip, rather than fail, if it is not on
`PATH`, so they stay CI-friendly.

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE) at your option.
