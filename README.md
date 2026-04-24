# Foundry Compute Module Go Template

[![Release](https://img.shields.io/github/v/tag/shpitdev/palantir-compute-module-pipeline-template?sort=semver&label=release)](https://github.com/shpitdev/palantir-compute-module-pipeline-template/releases)
[![CI](https://github.com/shpitdev/palantir-compute-module-pipeline-template/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/shpitdev/palantir-compute-module-pipeline-template/actions/workflows/ci.yml)
[![Release Automation](https://github.com/shpitdev/palantir-compute-module-pipeline-template/actions/workflows/release-version.yml/badge.svg?branch=main)](https://github.com/shpitdev/palantir-compute-module-pipeline-template/actions/workflows/release-version.yml)

A local-first Go template and toolkit for pipeline-mode Palantir Foundry Compute Modules.

The repo contains two things:

1. A reusable Go kit for reading Foundry inputs, processing rows, and writing dataset or stream outputs.
2. `foundry-cmgo`, a CLI that scaffolds new compute-module projects and runs a Foundry-like local loop with mock Foundry APIs plus a real Docker container parity check.

The included example enriches email rows with Gemini, but the durable product surface is the compute-module kit and CLI workflow.

## Quickstart: generate and validate a module

Install the CLI from this checkout:

```bash
./install.sh
```

Generate a starter and run the local Foundry loop:

```bash
foundry-cmgo new \
  --name my-module \
  --module github.com/acme/my-module \
  --dir /tmp/my-module \
  --example dataset

cd /tmp/my-module
go test ./...
foundry-cmgo preview --rows 20
foundry-cmgo build
foundry-cmgo inspect last
foundry-cmgo inspect config
foundry-cmgo inspect outputs
```

Command intent:

- `new` creates a minimal, dataset, or stream compute-module starter.
- `preview` samples configured local CSV input and runs the module as a host process against an in-process mock Foundry server.
- `build` builds and runs the generated Docker image by default, using full input, so container-only issues show up before publishing.
- `inspect` shows resolved config, last-run metadata, output artifacts, logs, row/record counts, and Docker network strategy.

Generated dataset/stream projects include `foundry-cmgo.yaml`:

```yaml
version: 1
module:
  command: ["go", "run", "./cmd/compute-module", "foundry"]
inputs:
  - alias: input
    path: data/input.csv
outputs:
  - alias: output
    mode: dataset
mockFoundry:
  root: .local/mock-foundry
  branch: master
preview:
  rows: 1000
  strategy: sampled
```

For deterministic terminal captures of the CLI happy path, see [`docs/devx/foundry-cmgo-cli-output-captures.md`](docs/devx/foundry-cmgo-cli-output-captures.md).

## Working in this repo

Canonical entrypoint:

```bash
./dev help
```

Core checks:

```bash
./dev verify                  # format, license, lint, tests, consumer checks, devx smoke checks
./dev verify foundry-cmgo     # fresh generated dataset + stream starter workflows
./dev test                    # Gemini + docker-compose E2E when secrets are configured
```

`./dev test` performs real Gemini calls and fails if committed output contains `status=error` rows. CI skips Gemini-dependent jobs when the required secrets are absent.

Local email-enricher paths:

```bash
export GEMINI_API_KEY=...
./dev run local -- --input /path/to/emails.csv --output /path/to/enriched.csv
./dev run foundry-emulated
./dev run foundry-emulated --watch
```

The watch loop starts mock Foundry plus a real container, reruns on input CSV edits, and writes committed local output under `.local/mock-foundry/uploads/...`.

Reset local compose/mock output state while preserving inputs:

```bash
./dev clean
```

## Repository layout

- `cmd/foundry-cmgo`: scaffold, preview, build, inspect, and mock-data seeding CLI.
- `cmd/enricher`: example compute-module binary that wires the kit to the email-enricher example.
- `pkg/pipeline/...`: reusable worker, schema, local IO, and Foundry IO primitives.
- `pkg/foundry/...`: Foundry environment parsing and HTTP client helpers.
- `pkg/mockfoundry/...`: local mock Foundry API used by preview/build/tests.
- `examples/email_enricher/...`: example domain logic and output mapping.
- `internal/devx/...`: generated-project templates and local preview/build orchestration.
- `test/consumer`: external package import contract tests.
- `test/template`: minimal generated-style module used for compatibility tests.

## Documentation

Start with [`docs/README.md`](docs/README.md) for the docs map.

High-signal docs:

- [`docs/DESIGN.md`](docs/DESIGN.md): architecture, runtime modes, and local tooling.
- [`docs/FOUNDRY_PARITY.md`](docs/FOUNDRY_PARITY.md): explicit local-vs-Foundry behavior contract.
- [`docs/RELEASE.md`](docs/RELEASE.md): Foundry configuration, Sources, egress, probes, and publishing notes.
- [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md): common deployment and local harness failures.
- [`docs/DIAGRAMS.md`](docs/DIAGRAMS.md): Mermaid diagrams for dataset and stream flows.
- [`docs/use-cases/milwaukee-resolution.md`](docs/use-cases/milwaukee-resolution.md): real-data deterministic catalog-resolution dogfood case.

## Foundry deployment note

This repo is public. Do not commit real Foundry URLs, dataset RIDs, tokens, Source credentials, or API keys.

Publishing to a Foundry registry requires repository secrets for the target stack. A failed `publish-foundry` run at registry login means credentials are missing or unauthorized; it is not evidence that the local template workflow is broken.

## Screenshots

Foundry UI screenshots live under [`docs/screenshots/`](docs/screenshots/).

Compute module configuration:

![Foundry compute module configuration](docs/screenshots/foundry-compute-module-configure-2026-02-15.png)

Lineage overview:

![Foundry data lineage view](docs/screenshots/foundry-data-lineage-2026-02-15.png)

Streaming transaction view:

![Foundry stream current transaction view](docs/screenshots/foundry-stream-transaction-view-2026-02-15.png)

Streaming metrics:

![Foundry stream metrics](docs/screenshots/foundry-stream-metrics-2026-02-15.png)
