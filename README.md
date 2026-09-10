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

`chain-chaos` sits between an EVM application and a real RPC endpoint and lets
you reproduce realistic infrastructure failures — RPC latency, timeouts, dropped
connections, rate limits, stale responses, provider disagreement, and (later)
chain reorganizations — **deterministically**, from a scenario plus a seed.

The question it answers: *does my blockchain application stay correct when the
blockchain and its infrastructure behave badly?*

## Status

Early. **Phase 1** is a transparent HTTP + WebSocket JSON-RPC pass-through proxy
— no faults injected yet. Point an existing app at it and it should behave
identically to talking to the node directly. Fault injection arrives in Phase 2.

## Quickstart (Phase 1)

Run a local node:

```sh
anvil
```

Start the proxy in front of it:

```sh
cargo run -p chain-chaos -- proxy --upstream http://127.0.0.1:8545 --listen 127.0.0.1:8546
```

Point your application (or `cast`) at the proxy instead of the node:

```sh
cast block-number --rpc-url http://127.0.0.1:8546
```

Same result as talking to Anvil directly — now every request is logged, and
Phase 2 will let you make it fail on purpose.

## Layout

```
crates/
  proxy/   # HTTP + WS pass-through, config, logging, graceful shutdown
  cli/     # `chain-chaos` binary
```

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE) at your option.
