# Troubleshooting

This repo is designed to be a reusable template for Foundry Compute Modules. The most common issues when first deploying are operational (Sources, networking, probes, logs) rather than application logic.

## No Logs / Empty Logs

Symptoms:

- The compute module is stuck "Initializing" / "Unresponsive" and the container shows no stdout logs.

Things to check:

- Foundry's stdout log capture path can require `/bin/sh` and `tee`. Very minimal images (e.g. distroless) may run but produce no logs.
- If you need logs quickly while iterating, use a base image that includes a shell.

## CrashLoopBackOff

Symptoms:

- Replica diagnostics show `CrashLoopBackOff`.

Things to check:

- Readiness/liveness probes: avoid probes like `echo` or `sh -c ...` if your image doesn't include them. Prefer an exec probe that calls your binary directly, e.g. `['/enricher','--help']`.
- File permissions for Foundry-mounted files: tokens and injected files are often readable by uid `5000`. Run the container as a numeric non-root user that can read those mounts.

## Network Errors Calling Foundry APIs

Symptoms:

- Errors like `dial tcp: i/o timeout` or `connection refused` when calling `/api/...` or `/stream-proxy/...`.

Things to check:

- Compute modules run in a zero-trust model and do not get outbound network by default.
- If your code calls back into Foundry REST APIs, you typically need a REST API Source that allowlists your stack hostname (the same host used in `FOUNDRY_URL`) and you must attach that Source to the compute module.

## Duplicate Records in Streaming Outputs

Symptoms:

- Output stream shows the same logical records many times.

Why it happens:

- Stream writes are append-only.
- If the compute module is restarted/redeployed, the container may re-run the pipeline and publish the same records again.

Mitigations:

- Use a snapshot dataset output for one-shot jobs.
- Add idempotency (e.g. checkpoint/state dataset) if you must write to a stream.
- Keep the compute module process alive after a successful run to avoid restart/rerun loops.

## Internal Module Endpoints Use IPv4

Symptoms:

- The compute module client loop logs `dial tcp [::1]:... connect: connection refused` against `GET_JOB_URI`.

Why it happens:

- `localhost` may resolve to IPv6 (`::1`) first, but the runtime sidecar may only be bound on IPv4 loopback.

Fix:

- Rewrite injected `localhost` URIs to `127.0.0.1` before dialing.

## Local Emulated Flow Fails Preflight

Symptoms:

- `./dev run foundry-emulated` fails before Docker Compose starts.
- Errors reference missing fixture CSV or unwritable `.local/mock-foundry/*`.

Why it happens:

- The local harness expects input fixture CSVs under `.local/mock-foundry/inputs`.
- Prior root-owned Docker runs can leave `.local/mock-foundry/uploads` unwritable for the host user.

Fix:

- Ensure input fixture exists at:
  - `.local/mock-foundry/inputs/<INPUT_RID>.csv`
- If ownership is broken, run the remediation command printed by preflight (example):
  - `docker run --rm -v "<repo>/.local:/work" alpine:3 sh -c "chown -R <uid>:<gid> /work"`
- Run `./dev clean` to reset compose resources and clear uploads while preserving inputs.
