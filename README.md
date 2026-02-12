# palantir-compute-module-pipeline-search

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

Targeted tests:

```bash
./dev test --scope unit
./dev test --scope integration
./dev test --scope e2e
```

Preflight diagnostics:

```bash
./dev doctor
./dev doctor --json
```

Run locally (no Foundry required, Gemini required):

```bash
export GEMINI_API_KEY=...
export GEMINI_MODEL=gemini-2.5-flash
./dev run local -- --input /path/to/emails.csv --output /path/to/enriched.csv
```

Run Foundry-like flow locally (mock dataset API + real Gemini + real container):

```bash
./dev run foundry-emulated
```

See `docker-compose.local.yml` for fixture mounts and output paths.

Run CI-style docker-compose E2E (fixed fixtures + output validation):

```bash
export GEMINI_API_KEY=...
export GEMINI_MODEL=gemini-2.5-flash
./dev e2e -v
```

Note: CI jobs that require Gemini secrets are skipped automatically if `GEMINI_API_KEY` / `GEMINI_MODEL` GitHub secrets are not configured.

## Docs

- `docs/DESIGN.md`: architecture, interfaces, local testing approach
- `docs/RELEASE.md`: Foundry configuration steps (Sources, egress, probes) and publishing guidance
- `docs/TROUBLESHOOTING.md`: common deployment failures and diagnosis
