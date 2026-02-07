# Design

## Overview

This project is a pipeline-mode Foundry Compute Module (Go) that runs as a one-shot batch container:

1. Read input dataset rows (email addresses)
2. Enrich each email via Gemini (search grounding + URL context + structured output)
3. Write an output dataset file and commit a transaction

This repo should also support a local, non-Foundry run mode for personal use and faster iteration:

- Read a local input file of emails
- Enrich via Gemini (or a stub)
- Write a local output file

Non-goals:

- Exposing an OpenAPI service
- Implementing the function-mode Jobs API contract

## Pipeline Mode vs Function Mode

Pipeline mode (this project):

- One-shot batch program executed by a pipeline build
- Reads file-based env vars (`BUILD2_TOKEN`, `RESOURCE_ALIAS_MAP`)
- Exits `0` on success, non-zero on failure

Function mode (not this project):

- Long-lived server that polls a Jobs API and posts results
- Different env vars and contract (module auth token, CA path, job URIs)

## Runtime Contract

Foundry pipeline-mode containers are provided file paths via environment variables:

- `BUILD2_TOKEN`: file path containing a bearer token
- `RESOURCE_ALIAS_MAP`: file path containing a JSON alias map that includes at least input/output dataset identifiers and branch identifiers

Additional configuration this module expects (not injected automatically):

- `FOUNDRY_URL`: Foundry base URL used to call dataset APIs, e.g. `https://<your-stack>.palantirfoundry.com`
- `GEMINI_API_KEY`: Gemini API key (inject as a secret)

Security notes:

- Treat the token file contents and all email addresses as sensitive
- Never log tokens or API keys
- Be deliberate about logging emails (prefer hashing/redaction)

## Local Mode (Non-Foundry)

The pipeline core should be runnable outside Foundry as a first-class workflow.

Desired local UX:

- `enricher local --input <csv> --output <csv>`
- Optional: `--format csv|jsonl`, `--workers N`, `--dry-run`, `--fail-fast`

Local mode should share the same pipeline core as Foundry mode; only I/O adapters differ.

## Dev Tooling

This repo should have a single local verification entrypoint that matches CI (format + lint + test).

If using Palantir godel, the common commands are:

| Task | Command |
| --- | --- |
| Format | `./godelw format` |
| Lint | `./godelw lint` |
| Test | `./godelw test` |
| Verify | `./godelw verify` |
| License headers | `./godelw license` |

Tools explicitly not required for this project:

- Conjure-generated clients (we are calling Foundry REST endpoints directly)
- OSDK / ontology SDKs (not relevant to pipeline-mode containers)

## Architecture (Adapters)

Keep the pipeline core independent of how inputs/outputs are provided.

Suggested boundaries:

- `InputSource`: yields `EmailRow` values
- `Enricher`: `Enrich(ctx, email) -> EnrichedRow`
- `OutputSink`: accepts `EnrichedRow` values and writes them somewhere

Adapters:

- Foundry input/output adapters (dataset APIs)
- Local file input/output adapters (CSV/JSONL on disk)

## Foundry I/O

### Read

Input is read via the Datasets `readTable` API (sufficient for small batches like ~500 rows). For larger inputs, plan for pagination/streaming.

### Write

Output is written using the standard dataset transaction flow:

1. Create transaction (output dataset + branch)
2. Upload file into the transaction (CSV initially; Parquet later if needed)
3. Commit transaction

## Foundry API Surface (Minimal)

The module can be implemented with a thin HTTP client hitting a small API surface:

- `GET  /api/v1/datasets/{rid}/readTable`
- `POST /api/v2/datasets/{rid}/transactions`
- `PUT  /api/v1/datasets/{rid}/transactions/{txn}/files/...`
- `POST /api/v2/datasets/{rid}/transactions/{txn}/commit`

## Schema Contract

Schemas should be treated as build-time contracts.

Recommended MVP output columns (joinable and debuggable):

- `email` (string, required)
- `linkedin_url` (string)
- `company` (string)
- `title` (string)
- `description` (string)
- `confidence` (string or float)
- `status` (string, e.g. `ok|not_found|error`)
- `error` (string, empty on success)

Optional audit fields (consider for later):

- `web_search_queries` (string, JSON-encoded)
- `sources` (string, JSON-encoded URLs)
- `model` (string)

## Enrichment (Gemini)

The enrichment step is a single function boundary (interface) so unit/integration tests can stub it.

Desired behavior:

- One request per email
- Uses Google Search grounding
- Uses URL context
- Structured JSON output constrained to the Go struct schema
- Conservative timeouts and retries (transient failures only)
- Rate limiting to respect quotas and avoid spiky egress

## Concurrency + Retry

Worker pool design:

- Fixed number of workers (configurable)
- Per-email retry with exponential backoff + jitter
- Hard timeout per email and overall build deadline
- Explicit failure policy (decide early)
- Fail-fast: any error exits non-zero (build fails)
- Partial output: write a row with `status=error` and continue

## Local Testing Strategy

Testing philosophy:

- Prefer end-to-end and integration tests that exercise realistic behavior
- Keep unit tests focused on sharp edges (parsing, backoff math, cancellation)

Layer 1: unit tests (no network, no Docker)

- Alias map parsing
- File-based env var loading (`BUILD2_TOKEN`, `RESOURCE_ALIAS_MAP`)
- Worker pool behavior (timeouts, cancellation, retries)
- CSV encode/decode

Layer 2: integration test using `httptest.Server`

- Mock the small Foundry API surface used by the module
- Run the orchestration end-to-end with a stubbed enricher
- Assert: correct API calls + output file schema/content

Layer 3: Docker Compose smoke test

- Run the real container image against a mock Foundry service
- Validate file mounts, env var loading, and end-to-end execution
- Treat the mock Foundry service as a reusable local harness, not test-only

Layer 4: Gemini integration tests (opt-in)

- Prefer early, realistic end-to-end runs against the real Gemini API using a tiny fixture
- Keep opt-in behind a build tag and a required env var so CI stays hermetic

## Repo Layout (Planned)

```
cmd/enricher/main.go
internal/
  foundry/
    env.go
    aliases.go
    datasets.go
  enrich/
    gemini.go
    worker.go
    types.go
    pipeline.go
  util/
    backoff.go
    csvio.go
test/
  fixtures/
  mock-foundry/
docker-compose.test.yml
Dockerfile
```

## Container Image

Prefer a static binary in a minimal base image (e.g. distroless).

Watch-outs:

- Ensure CA certificates are present in the runtime image so TLS calls to Foundry and Gemini succeed
- Keep the image amd64 unless you know Foundry will run arm64

## Publishing / Deployment

This repo is public; keep platform-specific publishing details and secrets out of version control.

See `docs/RELEASE.md` for the operational steps and required Foundry configuration (including egress).

## References

- Compute Modules: getting started (`https://palantir.com/docs/foundry/compute-modules/get-started/`)
- Container environment (`https://palantir.com/docs/foundry/compute-modules/containers/`)
- Execution modes (`https://palantir.com/docs/foundry/compute-modules/execution-modes/`)
- Custom client spec (function mode) (`https://palantir.com/docs/foundry/compute-modules/advanced-custom-client/`)
- Datasets readTable (`https://palantir.com/docs/foundry/api/datasets-resources/datasets/read-table/`)
- Transactions create (`https://palantir.com/docs/foundry/api/datasets-v2-resources/transactions/create-transaction/`)
- Upload file (`https://palantir.com/docs/foundry/api/datasets-resources/files/upload-file/`)
- Transactions commit (`https://palantir.com/docs/foundry/api/datasets-resources/transactions/commit-transaction/`)
- Gemini Google Search grounding (`https://ai.google.dev/gemini-api/docs/google-search`)
- Gemini URL context (`https://ai.google.dev/gemini-api/docs/url-context`)
- Gemini structured output (`https://ai.google.dev/gemini-api/docs/structured-output`)
- Palantir godel (`https://github.com/palantir/godel`)
