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

## Image Publishing

There are typically two publishing targets:

1. GitHub artifacts (CI visibility and reproducibility)
2. Your org's Palantir registry / artifact store used by Foundry compute modules

Define and document the publishing mechanism used in your environment (exact commands and auth differ by org).

Recommended end state:

- GitHub Actions builds the Docker image and produces a versioned artifact
- A gated workflow publishes the same image tag to the Palantir registry consumed by Foundry

## Rollout Checklist

- First run against a tiny fixture (5-10 emails)
- Validate output schema matches the dataset schema exactly
- Validate logs contain no tokens/API keys and avoid raw emails
- Validate failure policy (fail-fast vs partial output) matches pipeline expectations
- Confirm egress policy is minimal and sufficient

## Rollback

- Revert compute module image tag to the previously known-good tag
- Re-run pipeline build
