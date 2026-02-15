# palantir-compute-module-pipeline-search

<!-- Release & CI -->
[![Release](https://img.shields.io/github/v/tag/anand-testcompare/palantir-compute-module-pipeline-search?sort=semver&label=release)](https://github.com/anand-testcompare/palantir-compute-module-pipeline-search/releases)
[![Foundry Publish](https://github.com/anand-testcompare/palantir-compute-module-pipeline-search/actions/workflows/publish-foundry.yml/badge.svg?branch=main)](https://github.com/anand-testcompare/palantir-compute-module-pipeline-search/actions/workflows/publish-foundry.yml)
[![Release Automation](https://github.com/anand-testcompare/palantir-compute-module-pipeline-search/actions/workflows/release-version.yml/badge.svg?branch=main)](https://github.com/anand-testcompare/palantir-compute-module-pipeline-search/actions/workflows/release-version.yml)

<!-- Core stack -->
[![Go](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go&logoColor=white)](https://go.dev/doc/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Palantir Foundry](https://img.shields.io/badge/Palantir_Foundry-Compute_Module-0B74DE)](https://www.palantir.com/platforms/foundry/)
[![Gemini](https://img.shields.io/badge/Google_Gemini-API-4285F4?logo=google&logoColor=white)](https://ai.google.dev/)
[![Mermaid](https://img.shields.io/badge/Mermaid-Diagrams-FF3670?logo=mermaid&logoColor=white)](https://mermaid.js.org/)

Pipeline-mode Foundry Compute Module (Go) that:

1. Reads a dataset of email addresses
2. Enriches each email via Gemini (grounding + URL context + structured output)
3. Writes enriched rows to either:
   - a snapshot dataset (transactions), or
   - a streaming dataset (stream-proxy)

Local-first workflow: iterate locally (mock Foundry APIs + real container) and deploy the same image into Foundry.

Note: compute modules run as long-lived containers. This module runs the pipeline once per container start and then keeps the process alive so the platform does not restart it (which would re-run the pipeline and can duplicate stream outputs).

## Repo Layout

This repo is split into reusable kit packages and an example module:

- `pkg/pipeline/...`: reusable pipeline primitives (worker, local/foundry IO adapters, schema contract)
- `pkg/foundry/...`: Foundry env parsing and HTTP client
- `pkg/mockfoundry/...`: emulated Foundry server used by local harnesses and tests
- `examples/email_enricher/...`: example email enrichment domain logic and output mapping
- `cmd/enricher`: example binary wiring the kit + example

External-consumer contracts are validated in:

- `test/consumer`: imports reusable packages directly
- `test/template`: minimal new-module skeleton using pipeline kit APIs

## Development

Canonical entrypoint:

```bash
./dev help
```

Verify (CI parity + external consumer checks):

```bash
./dev verify
```

Real e2e test run (Gemini + Foundry-emulated docker-compose):

```bash
./dev test
```

`./dev test` performs real Gemini calls and fails if committed output contains any `status=error` rows.

Preflight diagnostics:

```bash
./dev doctor
./dev doctor --json
```

Run locally (no Foundry required, Gemini required):

```bash
export GEMINI_API_KEY=...
./dev run local -- --input /path/to/emails.csv --output /path/to/enriched.csv
```

`GEMINI_MODEL` is optional; default is `gemini-2.5-flash`.

Run Foundry-like flow locally (mock dataset API + real Gemini + real container):

```bash
./dev run foundry-emulated
```

Run a long-lived local dev loop (watches input CSV and reruns automatically):

```bash
./dev run foundry-emulated --watch
```

`./dev run foundry-emulated --watch` starts a tight local loop:

- starts mock-foundry + a real container
- runs once immediately, then reruns on input CSV edits
- reuses prior `status=ok` rows by `email` (best-effort incremental cache)
- stops cleanly on `Ctrl+C`

### Local Watch Loop Quickstart

1. Set a valid Gemini key in `.env`:

```bash
GEMINI_API_KEY=...
# GEMINI_MODEL is optional (default: gemini-2.5-flash)
```

2. Edit input rows in:

```bash
.local/mock-foundry/inputs/ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111.csv
```

3. Start the local loop:

```bash
./dev run foundry-emulated --watch
```

4. Read latest committed output at:

```bash
.local/mock-foundry/uploads/ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222/_committed/readTable.csv
```

5. Change and save the input CSV again to trigger another run.

Reset local compose state and clear mock-foundry uploads (inputs are preserved):

```bash
./dev clean
```

See `docker-compose.local.yml` for fixture mounts and output paths.

Run CI-style docker-compose E2E (fixed fixtures + output validation):

```bash
export GEMINI_API_KEY=...
./dev test -v
```

Note: CI jobs that require Gemini secrets are skipped automatically if `GEMINI_API_KEY` / `GEMINI_MODEL` GitHub secrets are not configured.

## Docs

- `docs/DESIGN.md`: architecture, interfaces, local testing approach
- `docs/RELEASE.md`: Foundry configuration steps (Sources, egress, probes) and publishing guidance
- `docs/TROUBLESHOOTING.md`: common deployment failures and diagnosis
- `docs/DIAGRAMS.md`: Mermaid sequence diagrams + flowcharts for API usage scenarios

## Defaults (high-signal)

Defaults differ between:

- binary internal fallbacks (used when env vars are unset in Foundry)
- local docker-compose harness defaults in `docker-compose.local.yml`

Key ones:

- `REQUEST_TIMEOUT`: `30s` binary fallback; local compose sets `2m`
- `WORKERS`: `10`
- `MAX_RETRIES`: `3`
- `FAIL_FAST`: `false`

For the full set of options and Foundry configuration, see `docs/RELEASE.md`.

## Screenshots

Put Foundry UI screenshots in `docs/screenshots/` and reference them from this README.

- Convention: `docs/screenshots/<short-topic>-<yyyy-mm-dd>.png`

Current screenshots:

Compute module configuration (pipelines mode, sources + env vars):

![Foundry compute module configuration](docs/screenshots/foundry-compute-module-configure-2026-02-15.png)

Lineage overview (inputs, sources, egress, output):

![Foundry data lineage view](docs/screenshots/foundry-data-lineage-2026-02-15.png)

Streaming dataset current transaction view:

![Foundry stream current transaction view](docs/screenshots/foundry-stream-transaction-view-2026-02-15.png)

Streaming dataset metrics:

![Foundry stream metrics](docs/screenshots/foundry-stream-metrics-2026-02-15.png)
