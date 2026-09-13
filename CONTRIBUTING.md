# Contributing

## Development

Requires Rust 1.80+. Integration tests that need a node spawn `anvil` (from
[Foundry](https://book.getfoundry.sh/)) and skip, rather than fail, when it is not
on `PATH`, so they stay CI-friendly.

```sh
cargo test --workspace
cargo clippy --workspace --all-targets
cargo fmt --all
```

## Scope

chain-chaos is deliberately minimal (see [DESIGN.md](DESIGN.md)): one rule set,
two phases, one seam. Before adding a fault, a CLI flag, or an abstraction, think if it even makes sense. If it doesn't need documenting in `DESIGN.md`, then it
probably doesn't belong.

## Pull requests

- One change per PR, with a scenario or test demonstrating it.
- Add or update a test for the behaviour you changed or fixed.
- Keep it plain and simple. This repo is ment for manual or agent-assisted development, so human-readibility is paramount. If you're using an LLM, instruct it to keep em-dashed out of it with a skill such as *unslop* or similar.

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE)
at your option.