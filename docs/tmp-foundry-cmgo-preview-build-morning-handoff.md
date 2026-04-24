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

7. A broad `doctor` command is intentionally not the next recommendation.
   - The valuable part is command-scoped preflight: fail early inside `preview`/`build` when config, input files, Docker, or network reachability are missing.
   - Foundry-specific image policy checks are lower value here because this CLI creates the Dockerfile template. If those checks become durable policy, they likely belong in a generic Foundry CLI/linter rather than this generated-project helper.
   - Seed commands are still present as advanced/debug plumbing.

8. Prefer installed `foundry-cmgo` or `scripts/foundry-cmgo-dev` for outside-repo testing.
   - Running `go run /path/to/repo/cmd/foundry-cmgo` from inside a generated module can pull CLI dependencies into the generated module context and produce confusing `go.sum` noise.

## What I think is next (revised after grooming)

Updated stance: do **not** start with a broad `doctor` command. Doctors are usually low-value when the same command can validate its own prerequisites and fail clearly. They become more useful when there is real drift: old generated projects, external platform rules changing, or user-managed config that may be stale. For this repo, most checks should be early, explicit preflight inside the command that needs them.

Highest-value next slices:

1. Split `internal/devx/run.go` after tests lock behavior.
   - This is the clearest maintainability win.
   - Suggested files:
     - `run.go` for public orchestration entrypoints/types.
     - `runtime_mock.go` for mock Foundry server lifecycle.
     - `runtime_docker.go` for Docker build/run and local-replace context prep.
     - `output.go` for dataset/stream output reading and rendering data prep.
     - `manifest.go` for last-run persistence.
   - Keep behavior unchanged during this pass.

2. Add a scripted generated-starter verification target.
   - One command that generates dataset + stream starters and runs preview/build/inspect.
   - This should become the confidence check before touching DevX CLI behavior again.
   - This is more valuable than `doctor` because it proves the actual path users run.

3. Expand `inspect`.
   - `inspect outputs`: list known outputs and paths.
   - `inspect config`: show resolved config/defaults.
   - `inspect last --json` may be useful for agents/tests if not already enough.
   - Keep this lightweight before any TUI work.

4. Replace the broad doctor idea with command-scoped preflight and sharper errors.
   - In `preview`: check config parse, input CSV exists/readable, module command exists enough to execute, and output mode is valid before doing expensive work.
   - In `build`: check Docker CLI availability, Docker daemon availability, Dockerfile exists, required run state files are readable by the container user, and mock Foundry reachability from the chosen container network path.
   - These checks should happen as part of `preview`/`build`, not behind a separate command users must remember.
   - If a check fails, the error should name the missing requirement and the command/flag to move forward, e.g. `install Docker`, `start Docker`, `use --local-process temporarily`, or `fix foundry-cmgo.yaml inputs[0].path`.

5. Re-evaluate Docker networking portability with evidence before implementing a fallback.
   - Current Linux path uses `docker run --network host` and works here.
   - Unknown: Docker Desktop / macOS behavior for host networking and `host.docker.internal`.
   - Recommended next action is a small compatibility probe/design note, not a speculative abstraction.
   - If fallback is needed, prefer encapsulating it in `runtime_docker.go` and testing the selected mock Foundry URL before running the module.

6. Treat stream JSONL output as acceptable indefinitely unless a real use case says otherwise.
   - Dataset output has a natural committed CSV path in mock Foundry state.
   - Stream output is append/event-shaped; run-local JSONL is a reasonable inspectable artifact.
   - Do not force stream durability parity unless users need cross-run stream replay or inspect history.

7. Only after the above, consider `preview --watch`.
   - Keep it simple: rerun on save, one process at a time, clear Ctrl-C cleanup.
   - Do not start with Bubble Tea.

Deferred / likely not worth doing here:

- A standalone `doctor docker` that re-checks the Dockerfile policy this CLI generated itself.
- Foundry-policy linting that tries to become the source of truth for platform container rules. If that is needed, it should likely live in a generic Foundry CLI/linter rather than this project-specific DevX helper.
- A TUI before the CLI summary/table path proves insufficient.

## Last local sync actions

Performed after merge:

```bash
git checkout main
git pull --ff-only
go test ./...
```

Result: local `main` is up to date with `origin/main`, and tests passed.
