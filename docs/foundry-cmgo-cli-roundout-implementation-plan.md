# Implementation plan: round out `foundry-cmgo` local CLI

Date: 2026-04-24  
Branch to implement on: `feat/foundry-cmgo-cli-roundout`  
Status: ready-to-implement plan

## Current baseline

Merged behavior on `main`:

- `foundry-cmgo new` generates `minimal`, `dataset`, and `stream` starters.
- Generated dataset/stream starters include `foundry-cmgo.yaml`.
- `foundry-cmgo preview`:
  - host-process path by default;
  - samples configured CSV input, defaulting to 1000 rows;
  - starts in-process mock Foundry;
  - runs configured `module.command`;
  - prints summary + table.
- `foundry-cmgo build`:
  - Docker/container-backed by default;
  - full input, not sampled;
  - writes dataset output to local mock Foundry committed CSV path;
  - writes stream output as run-local JSONL;
  - supports `--container=false` / `--local-process` escape hatch.
- `foundry-cmgo inspect last` reads `.local/foundry-cmgo/last-run.json`.
- Generated Foundry Dockerfile is Debian slim, `linux/amd64`, CA-enabled, numeric non-root `USER 5000:5000`.
- Low-level `seed dataset` / `seed stream` remain advanced/debug plumbing.

## Product stance

The CLI should feel like a local Foundry Transforms loop:

```bash
foundry-cmgo new ...
foundry-cmgo preview
foundry-cmgo build
foundry-cmgo inspect last
```

The happy path should not require users to understand seed commands, Docker Compose, curl, token files, alias maps, mounted paths, or mock Foundry internals.

`preview` should optimize for fast feedback.  
`build` should optimize for confidence and therefore cross the Docker/container boundary by default.

## Explicit non-goals / revised decisions

### Do not start with a standalone `doctor docker`

A broad doctor command is not the next highest-value step.

Reasons:

- This CLI creates the generated Docker template, so re-checking our own template policy is lower leverage than making generated commands fail clearly.
- Standalone doctors are often a crutch for silent failures elsewhere. Prefer each command validating prerequisites at the point of use.
- Foundry-specific image policy linting should probably live in a generic Foundry CLI/linter if it becomes durable product surface.
- A doctor becomes more useful later for drift: old generated projects, old config schema, old Dockerfiles, or externally edited project layouts.

### What to do instead

Add command-scoped preflight and actionable errors inside `preview` and `build`.

Examples:

- Missing `foundry-cmgo.yaml` should either infer defaults or say exactly what file/field is missing.
- Missing input CSV should name `inputs[0].path` and the resolved filesystem path.
- Missing Docker / stopped Docker daemon should fail before doing build orchestration and mention `--local-process` as a temporary escape hatch.
- Container cannot reach mock Foundry should say it is a Docker networking issue, not a generic module failure.

## Implementation slices

### Slice 1: Lock generated-starter verification as a script/target

Why first: it proves the actual user workflow and gives a safety net before refactoring `internal/devx/run.go`.

Add a repo-local verification command, likely one of:

- `test/scripts/verify_foundry_cmgo_generated.sh`, then wire it into `./dev verify` if runtime cost is acceptable; or
- `./dev verify foundry-cmgo` / `./dev test foundry-cmgo-generated` if the `dev` script already has a good command structure.

Required generated-project checks:

1. Build the current `foundry-cmgo` binary once into a temp dir.
2. Generate a fresh dataset starter with `--local-replace <repo-root>`.
3. In the dataset starter:
   - `go test ./...`
   - `foundry-cmgo preview --rows 1`
   - `foundry-cmgo build` using default container path
   - `foundry-cmgo inspect last`
4. Generate a fresh stream starter with `--local-replace <repo-root>`.
5. In the stream starter:
   - `go test ./...`
   - `foundry-cmgo preview`
   - `foundry-cmgo build` using default container path
   - `foundry-cmgo inspect last`
6. Print short artifact paths for failed runs, especially `.local/foundry-cmgo/.../run.log`.

Acceptance:

- One command exercises dataset + stream generated starters end-to-end.
- It uses the installed/built CLI binary, not `go run /path/to/cmd/foundry-cmgo` from inside the generated module.
- It is documented in `README.md` or `docs/TROUBLESHOOTING.md` as the DevX regression check.

### Slice 2: Split `internal/devx/run.go` without behavior changes

Current issue: `internal/devx/run.go` owns too many concerns.

Target shape:

- `internal/devx/run.go`
  - public `Preview`, `Build`, `RunOptions`, `LocalRunResult`, high-level orchestration only.
- `internal/devx/run_config.go` or keep `project_config.go`
  - config loading/defaulting/validation.
- `internal/devx/runtime_mock.go`
  - mock Foundry server lifecycle and host/container URL selection.
- `internal/devx/runtime_docker.go`
  - Docker build/run command creation;
  - Docker context preparation for absolute local `replace` directives;
  - Docker availability checks.
- `internal/devx/runtime_process.go`
  - host-process module execution.
- `internal/devx/input.go`
  - sampled/full input materialization.
- `internal/devx/output.go`
  - dataset/stream output reading and preview row shaping.
- `internal/devx/manifest.go`
  - `last-run.json` persistence and `InspectLast` loading.

Rules:

- No behavior changes in this slice unless tests reveal an obvious bug.
- Keep public CLI output stable.
- Keep diffs reviewable; prefer move/extract over rewrite.

Acceptance:

- Existing tests pass.
- Generated-starter verification script passes.
- `git diff --color-moved` should show mostly moved code, not a broad rewrite.

### Slice 3: Command-scoped preflight and sharper errors

Do not add standalone `doctor` yet.

Add explicit preflight/check helpers that are called by `preview`/`build` before expensive work.

Preview preflight:

- Config parse/defaulting succeeded.
- Single input/output currently supported; if more are present, either ignore with clear messaging or fail with an explicit single-transform limitation.
- `inputs[0].path` resolves to an existing readable file.
- Output mode is one of `dataset|stream`.
- `module.command` is non-empty.
- If command executable cannot be found and is not a shell/builtin path, report clearly.

Build preflight:

- All preview preflight checks.
- If container mode:
  - `Dockerfile` exists.
  - Docker CLI is available.
  - Docker daemon is reachable.
  - run-state directory files that the container must read (`token.txt`, `alias-map.json`) have permissions compatible with numeric non-root user.
  - selected Docker networking strategy can reach the mock Foundry server, or failure clearly names Docker networking.
- If local-process mode:
  - no Docker checks.

Error style:

- Avoid stack/raw subprocess dumps for predictable preflight failures.
- Include exact path/config key.
- Include the next useful command/flag when known.

Examples:

```text
build failed: Docker is required for the default container build, but `docker info` failed: ...
Start Docker, or run `foundry-cmgo build --local-process` for a faster non-container check.
```

```text
preview failed: input alias "input" points to missing file data/input.csv (resolved: /tmp/my-module/data/input.csv)
Update foundry-cmgo.yaml inputs[0].path or pass --input.
```

Acceptance:

- Add focused unit tests for config/input errors.
- Add at least one integration-ish test or script assertion for `build --local-process` not requiring Docker.
- Existing generated-starter verification still passes.

### Slice 4: Expand `inspect`

Current `inspect last` is useful but minimal.

Add:

```bash
foundry-cmgo inspect last --json
foundry-cmgo inspect config
foundry-cmgo inspect outputs
```

`inspect config` should show resolved values, not just raw YAML:

- module command;
- input aliases and paths;
- output aliases and modes;
- mock root;
- preview rows/strategy;
- whether config came from file or inferred defaults.

`inspect outputs` should show what users normally hunt for:

- last dataset committed CSV path;
- last stream JSONL path;
- row/record count;
- state dir;
- run log path;
- run kind and timestamp.

Acceptance:

- Human-readable table output.
- JSON output for `inspect last --json` at minimum.
- Tests cover no-last-run and malformed-last-run behavior.

### Slice 5: Docker networking portability investigation + implementation only if evidence says so

Current implementation uses Linux-friendly host networking:

```bash
docker run --network host ... FOUNDRY_URL=http://127.0.0.1:<port>
```

This works in the current Linux/Docker environment.

Unknowns:

- Docker Desktop / macOS support for this exact path.
- Whether `host.docker.internal` plus `--add-host host.docker.internal:host-gateway` is more portable or only Linux-specific in different ways.

Recommended approach:

1. Encapsulate network strategy selection in `runtime_docker.go`.
2. Add a small reachability probe before running the module container.
   - Probe from a tiny container image if cheap/reliable; otherwise probe by running the generated container with a simple command only if the image supports it.
   - If probing is too heavy, improve failure classification around connection refused/timeouts.
3. Prefer evidence over speculative portability work.
4. Keep `--local-process` as the escape hatch when Docker networking is hostile.

Acceptance:

- The selected strategy is visible in run logs or JSON result.
- Failure message distinguishes Docker unavailable from Docker network cannot reach mock Foundry.
- Linux generated-starter verification still passes.

### Slice 6: Keep stream output as run-local JSONL unless a real use case demands more

Current behavior:

- Dataset build output is committed under `.local/mock-foundry/uploads/.../_committed/readTable.csv`.
- Stream build output is written under `.local/foundry-cmgo/builds/<run-id>/streams/<rid>/<branch>/records.jsonl`.

Recommendation:

- Keep this shape for now, potentially indefinitely.
- It is honest to stream semantics and easy to inspect.
- Do not invent durable cross-run stream state unless users need stream replay/history.

Possible small improvement:

- `inspect outputs` should make stream JSONL path first-class.

### Slice 7: Aesthetic pass, beautification, and screenshot capture

This is a dedicated final implementation step, not an afterthought.

Goals:

- The CLI should look intentionally designed, not like raw debug output.
- Output should be easy to screenshot and easy to paste into issues/docs.
- Do not add Bubble Tea/TUI yet.

Tasks:

1. Review `preview`, `build`, and `inspect` terminal output in real generated dataset and stream projects.
2. Polish labels, spacing, ordering, and wording.
   - Keep aliases/human names before RIDs.
   - Keep paths visible.
   - Keep next commands obvious.
   - Keep the Docker-default note concise and non-defensive.
3. Improve table rendering if needed using existing dependencies first.
   - Avoid new dependencies unless output is clearly poor and the dependency rationale is documented.
4. Add docs examples with captured output.
   - Prefer deterministic ANSI-stripped text captures in docs if screenshot tooling is unavailable.
   - If local screenshot tooling is available, capture terminal screenshots into `docs/screenshots/` using the repo convention: `docs/screenshots/<short-topic>-<yyyy-mm-dd>.png`.
5. Capture at least:
   - dataset preview;
   - dataset build;
   - stream preview or stream build;
   - inspect last/config/outputs after expansion.
6. Update README or a DevX doc to include the polished output examples/screenshots.

Acceptance:

- A reviewer can see what the CLI experience looks like without running it.
- Screenshots or deterministic output captures are committed.
- No TUI/Bubble Tea introduced.

## Final verification before PR

Run all of these before claiming done:

```bash
go test ./...
go vet ./...
# New generated-starter verification command/script from Slice 1
```

Also manually smoke a fresh generated project from outside the source repo using `scripts/foundry-cmgo-dev` or the built binary:

```bash
foundry-cmgo new --name starter --module example.com/acme/starter --example dataset
cd starter
go test ./...
foundry-cmgo preview --rows 1
foundry-cmgo build
foundry-cmgo inspect last
```

Repeat for `--example stream`.

## PR expectation

The implementation PR should include:

- behavior-preserving cleanup of `internal/devx/run.go`;
- generated-starter verification script/target;
- command-scoped preflight and sharper errors;
- expanded `inspect`;
- Docker networking handling evidence or a clear decision to leave Linux-only for now with documented error behavior;
- aesthetic/output polish;
- committed output examples or screenshots;
- verification evidence in the PR body.
