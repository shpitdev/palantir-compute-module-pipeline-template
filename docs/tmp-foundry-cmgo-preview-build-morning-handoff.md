# Morning handoff: foundry-cmgo preview/build ergonomics

Date: 2026-04-24  
Local branch: `main`  
Remote synced: `origin/main` fast-forwarded after PR #63 merge

## Current state

The preview/build ergonomics slice is merged into `main`.

Implemented user path:

```bash
foundry-cmgo new --name my-module --module github.com/acme/my-module --example dataset
cd my-module
go test ./...
foundry-cmgo preview
foundry-cmgo build
foundry-cmgo inspect last
```

Important behavior:

- `preview` is intentionally fast and host-process based.
  - Starts an in-process mock Foundry server.
  - Samples configured CSV input; default preview rows: 1000.
  - Runs configured `module.command` from `foundry-cmgo.yaml`.
  - Prints a compact summary + table.
- `build` is intentionally Docker/container-backed by default.
  - Builds the generated Dockerfile with `--platform linux/amd64`.
  - Runs the generated image against the same in-process mock Foundry server.
  - Prints a short note explaining this default and the escape hatch.
  - Escape hatches: `foundry-cmgo build --container=false` or `foundry-cmgo build --local-process`.
- `inspect last` reads `.local/foundry-cmgo/last-run.json` and shows last run state/log/output paths.
- Generated dataset/stream starters now include `foundry-cmgo.yaml`.
- Generated Foundry Dockerfile is hardened:
  - Debian slim, not Alpine.
  - Explicit `linux/amd64`.
  - CA certificates installed.
  - Numeric non-root `USER 5000:5000`.

## Files most relevant for the next pass

- `cmd/foundry-cmgo/main.go`
  - CLI wiring and terminal rendering for `preview`, `build`, and `inspect last`.
- `internal/devx/project_config.go`
  - `foundry-cmgo.yaml` loading/defaulting/inference.
- `internal/devx/run.go`
  - Main preview/build orchestration.
  - Starts mock Foundry.
  - Materializes sampled/full inputs.
  - Runs local process or Docker container.
  - Reads dataset/stream output.
  - Writes last-run manifest.
- `internal/devx/run_test.go`
  - Host-process preview/build coverage for generated dataset/stream starters.
- `internal/devx/templates/foundry/*`
  - Generated Dockerfile, README, config, fixtures.
- `README.md`
  - Root-level DevX CLI docs.
- `docs/tmp-foundry-cmgo-preview-build-plan.md`
  - Original implementation/research plan and broader roadmap.

## Verification already run

After the PR was merged and `main` was pulled locally:

```bash
go test ./...
```

Passed on `main`.

Before merge, also verified:

```bash
go vet ./...
```

Fresh generated dataset starter:

```bash
go test ./...
foundry-cmgo preview --rows 1
foundry-cmgo build          # Docker/container default
foundry-cmgo inspect last
```

Fresh generated stream starter:

```bash
go test ./...
foundry-cmgo preview
foundry-cmgo build          # Docker/container default
foundry-cmgo inspect last
```

Both starter flows passed in this Linux/Docker environment.

## Known caveats / where I left off

1. `internal/devx/run.go` is now doing too much.
   - It works, but it owns config resolution, mock server lifecycle, Docker context prep, command execution, output parsing, and manifest writing.
   - Next cleanup should split it into smaller files or helpers without changing behavior.

2. Docker build emits warnings about constant `FROM --platform=linux/amd64`.
   - This is intentional for Foundry clarity, but Docker BuildKit warns against constant platform flags.
   - Do not remove blindly; the plan explicitly wanted visible amd64 intent.

3. Container build currently uses Docker host networking.
   - `docker run --network host ... FOUNDRY_URL=http://127.0.0.1:<port>`
   - This worked in the current Linux environment.
   - It may need a fallback for Docker Desktop / non-Linux environments, likely `host.docker.internal` with `--add-host host.docker.internal:host-gateway` or a small compatibility probe.

4. Local source-checkout generated starters use absolute `replace` directives.
   - Docker cannot access absolute paths outside its build context.
   - The implementation handles this by preparing a temporary Docker context under `.local/foundry-cmgo/builds/<run-id>/docker-context` and copying absolute local replacements into `_local_replaces/`.
   - This is practical for dev verification but should get a focused test or a cleaner abstraction.

5. Only the first input and first output are implemented.
   - The YAML schema uses arrays, but orchestration currently targets the single-transform happy path.

6. Stream build output is persisted as run-local JSONL.
   - Dataset build commits to `.local/mock-foundry/uploads/.../_committed/readTable.csv`.
   - Stream build writes records under `.local/foundry-cmgo/builds/<run-id>/streams/<rid>/<branch>/records.jsonl`.
   - Mock stream history is not durable across processes yet.

7. `doctor`, richer `inspect`, and `preview --watch` are not implemented.
   - Seed commands are still present as advanced/debug plumbing.

8. Prefer installed `foundry-cmgo` or `scripts/foundry-cmgo-dev` for outside-repo testing.
   - Running `go run /path/to/repo/cmd/foundry-cmgo` from inside a generated module can pull CLI dependencies into the generated module context and produce confusing `go.sum` noise.

## What I think is next

Highest-value next slice:

1. Add `foundry-cmgo doctor project` and `foundry-cmgo doctor docker`.
   - Check config exists and references real files.
   - Check Dockerfile has no Alpine runtime, has CA certs, has `linux/amd64`, and has numeric non-root user.
   - Check Docker availability and whether host networking works; if not, print the expected fallback/flag.

2. Split `internal/devx/run.go` after tests lock behavior.
   - Suggested files:
     - `run.go` for public orchestration entrypoints/types.
     - `runtime_mock.go` for mock Foundry server lifecycle.
     - `runtime_docker.go` for Docker build/run and local-replace context prep.
     - `output.go` for dataset/stream output reading and rendering data prep.
     - `manifest.go` for last-run persistence.

3. Add a Docker-network portability fallback.
   - Try host networking on Linux.
   - Fall back to `host.docker.internal` where supported.
   - Make failures actionable: name Docker/network issue and suggest `--local-process` only as a temporary escape hatch.

4. Add a scripted generated-starter verification target.
   - One command that generates dataset + stream starters and runs preview/build/inspect.
   - This should become the confidence check before touching DevX CLI behavior again.

5. Expand `inspect`.
   - `inspect outputs`: list known outputs and paths.
   - `inspect config`: show resolved config/defaults.
   - Keep this lightweight before any TUI work.

6. Only after the above, consider `preview --watch`.
   - Keep it simple: rerun on save, one process at a time, clear Ctrl-C cleanup.
   - Do not start with Bubble Tea.

## Last local sync actions

Performed after merge:

```bash
git checkout main
git pull --ff-only
go test ./...
```

Result: local `main` is up to date with `origin/main`, and tests passed.
