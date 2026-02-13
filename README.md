# palantir-compute-module-pipeline-search

[![Release](https://img.shields.io/github/v/tag/anand-testcompare/palantir-compute-module-pipeline-search?sort=semver&label=release)](https://github.com/anand-testcompare/palantir-compute-module-pipeline-search/releases)
[![Foundry Publish](https://github.com/anand-testcompare/palantir-compute-module-pipeline-search/actions/workflows/publish-foundry.yml/badge.svg?branch=main)](https://github.com/anand-testcompare/palantir-compute-module-pipeline-search/actions/workflows/publish-foundry.yml)
[![Release Automation](https://github.com/anand-testcompare/palantir-compute-module-pipeline-search/actions/workflows/release-version.yml/badge.svg?branch=main)](https://github.com/anand-testcompare/palantir-compute-module-pipeline-search/actions/workflows/release-version.yml)

Pipeline-mode Foundry Compute Module (Go) that:

1. Reads a dataset of email addresses
2. Enriches each email via Gemini (Google Search grounding + URL context + structured output)
3. Writes an output dataset

This runs as a Foundry Compute Module and executes a pipeline job that:

- Reads an input dataset of email addresses
- Enriches each email via Gemini
- Writes enriched rows to either a snapshot dataset (transactions) or a streaming dataset (stream-proxy)

In Foundry, compute modules are deployed as long-running containers. This repo runs its pipeline logic once per module start and then keeps the process alive so the platform does not restart it (which would re-run the pipeline and can duplicate stream outputs).

It is also runnable locally (without Foundry) against local files.

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

Canonical developer entrypoint:

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

`./dev run foundry-emulated` now runs a preflight checklist before compose starts:
- verifies local fixture/config paths
- verifies local harness directories are writable
- attempts an automatic ownership fix for `.local/` when needed

`./dev run foundry-emulated --watch`:
- starts mock-foundry
- runs enricher once immediately
- watches the input CSV for changes (2s polling) and reruns automatically
- uses `REQUEST_TIMEOUT` per email (default `2m` in local compose)
- for dataset outputs, reuses previously committed `status=ok` rows by `email` and enriches only new/changed rows
- validates output after each rerun and prints failures
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
