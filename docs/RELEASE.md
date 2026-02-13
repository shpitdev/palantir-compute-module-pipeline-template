# Release (Foundry)

This repo is public. Do not put real Foundry URLs, dataset RIDs, tokens, or API keys in git.

## Preconditions

- A pipeline-mode Compute Module is created in Foundry
- Input dataset and output dataset exist (and their schemas are decided)
- You know the alias names you will configure in the pipeline (e.g. `input`, `output`)

## Required Configuration

### Environment Variables

Foundry injects:

- `BUILD2_TOKEN` (file path)
- `RESOURCE_ALIAS_MAP` (file path)

You must provide (via compute module configuration):

- `FOUNDRY_URL`: Foundry base URL or hostname (example formats: `https://<your-stack>.palantirfoundry.com` or `<your-stack>.palantirfoundry.com`)
- `GEMINI_MODEL`: Gemini model name (do not hardcode in code; configure per environment)

Gemini API key (recommended: Foundry Sources):

- **Option A (Sources, recommended):** configure a Source that stores the Gemini key and injects egress policy.
  - Foundry will mount a credentials JSON file and set `SOURCE_CREDENTIALS` to its file path.
  - Configure:
    - `GEMINI_SOURCE_API_NAME`: the Source "API name" you configured in Data Connection
    - `GEMINI_SOURCE_SECRET_NAME`: the secret name inside that Source (optional if the key can be inferred)
- **Option B (env var / secret):** set `GEMINI_API_KEY` directly.
  - `GEMINI_API_KEY` may be the literal key or a **file path** containing the key.

Optional knobs:

- `WORKERS` (int)
- `MAX_RETRIES` (int)
- `REQUEST_TIMEOUT` (duration)
- `FAIL_FAST` (bool)
- `RATE_LIMIT_RPS` (float)
- `GEMINI_CAPTURE_AUDIT` (bool)
- `GEMINI_BASE_URL` (string; optional base URL override for proxies/testing, not recommended in Foundry)

### Output Write Semantics (Pipeline Mode)

This module supports two output types:

- Snapshot dataset output (dataset transactions + file upload)
- Stream output (stream-proxy JSON record publish)

By default, the binary runs in `--output-write-mode=auto`, which probes stream-proxy to decide which write path to use.

#### Dataset Output (Transactions)

When executing in pipeline mode with a snapshot dataset output, Foundry may open a transaction on the configured output dataset for the
duration of the build. During this time you may be unable to create a new transaction on that output dataset/branch.

This binary handles that by:

- Attempting to create a transaction
- If Foundry responds with `OpenTransactionAlreadyExists`, listing transactions (preview endpoint) and using the latest `OPEN` transaction
- Uploading the output file into that transaction
- If the transaction was created by Foundry (the `OpenTransactionAlreadyExists` case), **do not commit**; Foundry will commit as part of the build.
  If the binary created the transaction (local harness), it will commit after a successful upload.

#### Stream Output (Stream-Proxy)

If the configured output is a stream, this binary writes output rows by publishing JSON records via the stream-proxy API.

### Egress Policy

Compute modules run in a zero-trust network model: by default they have no external network access (including other Foundry services).
You must explicitly configure Sources (network policies) for any network access the module needs.

At minimum, expect to allowlist:

- `generativelanguage.googleapis.com` (Gemini API)

Confirm exact domains from the client library / runtime behavior before locking the policy.

In addition, if your module calls Foundry REST APIs (this repo does), you should plan to allow access to your Foundry stack host
(the same host used by `FOUNDRY_URL`) via a Source/network policy as well. This does not mean "leaving Foundry"; it is simply
allowing the container to make HTTPS requests to the stack's API gateway.

Practical pattern:

- Create a REST API Source pointing at your Foundry stack hostname (e.g. `<stack>.palantirfoundry.com:443`)
- Enable code import for compute modules on that Source
- Attach that Source to the compute module alongside the Gemini Source

Note: for Sources used in code alongside Foundry inputs/outputs, you may need to enable Source exports and allow the required markings/org.

### Compute Module Probes + Logs

Two common pitfalls when iterating on compute modules:

- Stdout log capture: Foundry's stdout log capture can require `/bin/sh` and `tee`. Very minimal images (e.g. distroless) may run fine but produce no logs.
- Readiness probe: avoid probes like `echo` (not present in distroless). Prefer an exec probe that calls your binary directly, for example `['/enricher','--help']`.

### Streaming Output Idempotency

Streaming outputs are append-only. If the compute module is restarted/redeployed, the container may re-run the pipeline and republish the same records.

Options:

- Write to a snapshot dataset instead of a stream for one-shot jobs.
- Add idempotency/deduplication in your stream sink (e.g. publish a stable key or filter against a checkpoint/state dataset).
- Keep the compute module process alive after a successful run so the platform doesn't restart it and duplicate writes.

## Image Publishing

This repo publishes compute-module images to Foundry via `.github/workflows/publish-foundry.yml`.

### GitHub Secrets Required

- `FOUNDRY_ARTIFACT_REPOSITORY_RID`: artifact repository RID used as the `docker login` username.
- `FOUNDRY_TOKEN`: token used as the `docker login` password.
- `FOUNDRY_REGISTRY_HOST`: Foundry container registry host (example: `<your-stack>-container-registry.palantirfoundry.com`).
- `FOUNDRY_DOCKER_IMAGE_NAME`: image name in the registry (example: `email-enrichment-google`).
- `FOUNDRY_URL`: stack URL (kept for related workflows and docs consistency).

### Tagging Behavior

- On `main` pushes: publish `sha-<short-gitsha>` and moving tag `main`.
- On release tags `v*`: publish both `sha-<short-gitsha>` and `<tag>`.
- On pull requests: build only (no push) to validate Docker buildability.

### Permissions

For push to succeed, the CI identity backing `FOUNDRY_TOKEN` needs **Edit** on the target Artifact repository.
`View` alone can pull but cannot push.

## Rollout Checklist

- First run against a tiny fixture (5-10 emails)
- Validate output schema matches the dataset schema exactly
- Validate logs contain no tokens/API keys and avoid raw emails
- Validate failure policy (fail-fast vs partial output) matches pipeline expectations
- Confirm egress policy is minimal and sufficient

## Rollback

- Revert compute module image tag to the previously known-good tag
- Re-run pipeline build
