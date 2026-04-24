# Design

## Overview

This project is a pipeline-mode Foundry Compute Module (Go) that runs its pipeline logic once per module start:

1. Read input dataset rows (email addresses)
2. Enrich each email via Gemini (search grounding + URL context + structured output)
3. Write either:
   - a snapshot dataset output (output transaction + upload + optional commit), or
   - a stream output (stream-proxy JSON record publish)

This repo also supports a local, non-Foundry run mode for personal use and faster iteration:

- Read a local input file of emails
- Enrich via Gemini
- Write a local output file

Non-goals:

- Exposing an OpenAPI service
- Implementing the function-mode Jobs API contract

## Pipeline Mode vs Function Mode

Pipeline mode (this project):

- One-shot batch program executed by a pipeline build
- Reads file-based env vars (`BUILD2_TOKEN`, `RESOURCE_ALIAS_MAP`)
- Exits `0` on success, non-zero on failure in local/test harnesses
- In Foundry, the compute module container is typically expected to be long-running; this repo keeps the process alive after completing a run when compute-module internal endpoints are present to avoid restart/rerun loops

Function mode (not this project):

- Long-lived server that polls a Jobs API and posts results
- Different env vars and contract (module auth token, CA path, job URIs)

Note: some Foundry stacks inject internal module endpoints (e.g. `GET_JOB_URI`, `POST_RESULT_URI`) even for pipeline-style modules. This repo uses those endpoints only to keep the module responsive (acknowledge internal jobs); it does not expose interactive query handlers.

## Runtime Contract

Foundry pipeline-mode containers are provided file paths via environment variables:

- `BUILD2_TOKEN`: file path containing a bearer token
- `RESOURCE_ALIAS_MAP`: file path containing a JSON alias map that includes at least input/output dataset identifiers and branch identifiers

Service discovery:

- `FOUNDRY_SERVICE_DISCOVERY_V2`: file path containing Foundry service base URLs. The code reads `api_gateway` for dataset APIs and `stream_proxy` for stream APIs.
- `FOUNDRY_URL`: optional local/back-compat fallback when service discovery is not available.

Additional configuration this module expects:

- `GEMINI_API_KEY`: Gemini API key, or configure a Foundry Source and read it from `SOURCE_CREDENTIALS`
- `GEMINI_MODEL`: Gemini model name

Optional Gemini knobs:

- `GEMINI_BASE_URL`: override Gemini API base URL (useful for proxies/testing)
- `GEMINI_CAPTURE_AUDIT`: include sources/queries in output (`true|false`)

Security notes:

- Treat the token file contents and all email addresses as sensitive
- Never log tokens or API keys
- Be deliberate about logging emails (prefer hashing/redaction)

## Local Mode (Non-Foundry)

Local mode is a first-class workflow:

- `enricher local --input <csv> --output <csv>`
- Supports the same worker, retry, timeout, rate-limit, fail-fast, Gemini model/base URL, and audit-capture knobs as Foundry mode.

Local and Foundry modes share the email-enricher pipeline contract; only I/O adapters differ.

## Dev Tooling

This repo should have a single local verification entrypoint that matches CI (format + lint + test).

Primary commands:

| Task | Command |
| --- | --- |
| Verify (CI parity + public consumer checks) | `./dev verify` |
| Real Gemini + compose E2E | `./dev test` |
| Doctor diagnostics | `./dev doctor` |
| Local emulated flow (canonical) | `./dev run foundry-emulated` |
| Local emulated flow with auto-rerun loop | `./dev run foundry-emulated --watch` |
| Reset local emulated harness outputs | `./dev clean` |

Underlying godel tasks remain available (`./godelw format|lint|test|verify|license`).

Tools explicitly not required for this project:

- Conjure-generated clients (we are calling Foundry REST endpoints directly)
- OSDK / ontology SDKs (not relevant to pipeline-mode containers)

## Architecture (Adapters)

The code keeps domain processing separate from local/Foundry I/O:

- `examples/email_enricher/pipeline`: output row contract, CSV codec, legacy stream-record codec, and enrichment orchestration helpers
- `pkg/pipeline/io/local`: local CSV input helpers
- `pkg/pipeline/io/foundry`: Foundry dataset I/O, stream backend boundary, and retry policy
- `pkg/foundry`: environment parsing, service discovery, HTTP client, and internal keepalive support
- `pkg/mockfoundry`: local Foundry-like API harness

The current stream backend is `LegacyStreamProxyBackend`. It preserves the compute-module-compatible stream-proxy surface while leaving a seam for a future high-scale streams backend.

## Foundry I/O

### Read

Input is read via the Datasets `readTable` API (sufficient for small batches like ~500 rows). For larger inputs, plan for pagination/streaming.

### Write

Output can be written in one of two ways:

- Snapshot dataset output: dataset transactions + file upload
- Stream output: stream-proxy JSON record publish

The CLI defaults to `--output-write-mode=auto`, which probes stream-proxy to decide which write path to use.

#### Dataset Output (Transactions)

In Foundry pipeline mode, the build system may create the output transaction before starting the module (and creating a new transaction can conflict).

1. Create transaction (output dataset + branch). If this fails with `OpenTransactionAlreadyExists`, list transactions (preview) and use the latest `OPEN` transaction.
2. Upload file into the transaction (CSV initially; Parquet later if needed)
3. If the transaction was created by Foundry (the `OpenTransactionAlreadyExists` case), do not commit; Foundry will commit as part of the build.
   If the module created the transaction (local harness), commit after upload succeeds.

#### Stream Output (Stream-Proxy)

Write one JSON record per output row via the legacy stream-proxy API. App orchestration talks through `foundryio.StreamBackend`; the current implementation is `LegacyStreamProxyBackend`.

## Foundry API Surface (Minimal)

The module can be implemented with a thin HTTP client hitting a small API surface:

- `GET  /api/v2/datasets/{rid}/readTable`
- `POST /api/v2/datasets/{rid}/transactions`
- `GET  /api/v2/datasets/{rid}/transactions?preview=true` (preview; used to discover existing `OPEN` transactions)
- `POST /api/v2/datasets/{rid}/files/{filePath}/upload?transactionRid={txn}`
- `POST /api/v2/datasets/{rid}/transactions/{txn}/commit`
- `GET  /stream-proxy/api/streams/{rid}/branches/{branch}/records` (used for write-mode probing and best-effort incremental cache reads; response shape may be an array or an envelope depending on stack)
- `POST /stream-proxy/api/streams/{rid}/branches/{branch}/jsonRecord`

## Schema Contract

Schemas are treated as code-owned contracts. The email-enricher output columns live in `examples/email_enricher/pipeline.Header()`. Stream output uses the same logical field names through `RowToStreamRecord` / `RowFromStreamRecord`; local stream readTable projection adds metadata columns from `StreamMetadataHeader()`.

Current output columns:

- `email` (string, required)
- `linkedin_url` (string)
- `company` (string)
- `title` (string)
- `description` (string)
- `confidence` (string or float)
- `status` (string, e.g. `ok|not_found|error`)
- `error` (string, empty on success)
- `model` (string)
- `sources` (string, JSON-encoded URLs)
- `web_search_queries` (string, JSON-encoded)

## Enrichment (Gemini)

The enrichment step is a single function boundary (interface) so unit/integration tests can:

- use deterministic test fakes (no network)

Current behavior:

- One request per email
- Uses Google Search grounding
- Uses URL context
- Parses structured JSON into the Go result schema
- Applies per-email timeouts and retries for transient failures
- Supports optional global request rate limiting

## Concurrency + Retry

Worker pool design:

- Fixed number of workers (configurable)
- Per-email retry with exponential backoff + jitter
- Per-email request timeout
- `--fail-fast=true`: first enrichment error fails the run
- default partial-output mode: write a row with `status=error` and continue
- Foundry dataset/stream I/O retries use `foundryio.DefaultRetryPolicy`

## Local Testing Strategy

Testing philosophy:

- Prefer end-to-end and integration tests that exercise realistic behavior
- Keep unit tests focused on sharp edges (parsing, backoff math, cancellation)

Layer 1: unit tests (no network, no Docker)

- Alias map parsing
- File-based env var loading (`BUILD2_TOKEN`, `RESOURCE_ALIAS_MAP`)
- Worker pool behavior (timeouts, cancellation, retries)
- CSV encode/decode
- stream row encode/decode
- stream table projection

Layer 2: integration test using `httptest.Server`

- Mock the small Foundry API surface used by the module
- Run the orchestration end-to-end with a deterministic test enricher (no network)
- Assert: correct API calls + output file schema/content

Layer 3: Docker Compose smoke test

- Run the real container image against a mock Foundry service
- Requires real Gemini API access (set `GEMINI_API_KEY` and `GEMINI_MODEL`)
- Validate file mounts, env var loading, and end-to-end execution
- Treat the mock Foundry service as a reusable local harness, not test-only

Layer 4: Gemini integration tests (real network)

- `go test -tags=gemini_e2e ./...` runs the real Gemini package-level e2e when secrets are present
- Docker/Venom e2e runs the real container against the mock Foundry service with real Gemini
- CI skips Gemini-dependent jobs when secrets are absent

## Repo Layout

```
cmd/enricher/main.go
cmd/mock-foundry/main.go
examples/
  email_enricher/
    enrich/
      gemini/
      types.go
    pipeline/
      csv.go
      rows.go
      stream.go
internal/
  app/
    enricher.go
    incremental.go
pkg/
  foundry/
    client.go
    env.go
  mockfoundry/
    server.go
  pipeline/
    core/
    io/
      foundry/
      local/
    schema/
    worker/
test/
  consumer/
  fixtures/
  template/
docker-compose.local.yml
Dockerfile
Dockerfile.mock-foundry
```

## Container Image

Prefer a static binary in a minimal base image.

Watch-outs:

- Ensure CA certificates are present in the runtime image so TLS calls to Foundry and Gemini succeed
- Foundry's default stdout log capture path may require `/bin/sh` and `tee`; distroless images can make logs and some probes harder to debug
- Keep the image amd64 unless you know Foundry will run arm64

## Publishing / Deployment

This repo is public; keep platform-specific publishing details and secrets out of version control.

See `docs/RELEASE.md` for the operational steps and required Foundry configuration (including egress).

## References

- Compute Modules: getting started (`https://palantir.com/docs/foundry/compute-modules/get-started/`)
- Container environment (`https://palantir.com/docs/foundry/compute-modules/containers/`)
- Execution modes (`https://palantir.com/docs/foundry/compute-modules/execution-modes/`)
- Custom client spec (function mode) (`https://palantir.com/docs/foundry/compute-modules/advanced-custom-client/`)
- Datasets v2 readTable (`https://palantir.com/docs/foundry/api/datasets-v2-resources/datasets/read-table-dataset/`)
- Transactions v2 create (`https://palantir.com/docs/foundry/api/datasets-v2-resources/transactions/create-transaction/`)
- Datasets v2 list transactions (preview) (`https://palantir.com/docs/foundry/api/datasets-v2-resources/datasets/list-transactions-of-dataset/`)
- Upload file v2 (`https://palantir.com/docs/foundry/api/datasets-v2-resources/files/upload-file/`)
- Transactions v2 commit (`https://palantir.com/docs/foundry/api/datasets-v2-resources/transactions/commit-transaction/`)
- Gemini Google Search grounding (`https://ai.google.dev/gemini-api/docs/google-search`)
- Gemini URL context (`https://ai.google.dev/gemini-api/docs/url-context`)
- Gemini structured output (`https://ai.google.dev/gemini-api/docs/structured-output`)
- Palantir godel (`https://github.com/palantir/godel`)
