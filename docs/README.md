# Documentation

This folder keeps durable reference material only. Temporary plans and handoff notes should live outside the repo or be deleted after the implementation lands.

## Product and architecture

- [`DESIGN.md`](DESIGN.md) — architecture, runtime modes, local tooling, and adapter boundaries.
- [`FOUNDRY_PARITY.md`](FOUNDRY_PARITY.md) — explicit behavior contract for the Foundry surfaces emulated locally.
- [`DIAGRAMS.md`](DIAGRAMS.md) — Mermaid diagrams for dataset, stream, keepalive, and local-emulated flows.

## Operating and release docs

- [`RELEASE.md`](RELEASE.md) — Foundry configuration, Sources/egress, environment variables, probes, and publish workflow notes.
- [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) — common deployment/local harness failures and how to diagnose them.

## DevX evidence

- [`devx/foundry-cmgo-cli-output-captures.md`](devx/foundry-cmgo-cli-output-captures.md) — normalized terminal captures from fresh generated dataset and stream starters.

## Use cases

- [`use-cases/milwaukee-resolution.md`](use-cases/milwaukee-resolution.md) — deterministic catalog-resolution dogfood case against real Milwaukee retailer parquet data.

## Visual references

- [`screenshots/`](screenshots/) — Foundry UI screenshots referenced from the root README and release/troubleshooting docs as needed.
