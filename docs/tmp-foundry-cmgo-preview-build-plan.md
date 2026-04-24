# Temporary plan: Foundry-like `foundry-cmgo preview` / `build` ergonomics

Date: 2026-04-24  
Branch: `plan/foundry-cmgo-preview-build-ergonomics`  
Status: research + implementation handoff plan only; do not treat as implemented.

## Summary recommendation

Add first-class `foundry-cmgo preview` and `foundry-cmgo build` commands that hide the current low-level mock Foundry plumbing from users. Keep `seed dataset` and `seed stream` as advanced/debug subcommands, but make the normal user path feel like Foundry Transforms: configure once, preview fast against bounded/sample input, then build full output.

Also harden the generated Docker template. The root project Dockerfile already matches the likely Foundry constraints much better than the generated template. The template currently uses Alpine and lacks an explicit numeric non-root user and explicit `linux/amd64` platform. Official Palantir docs require compute module images to run as a non-root numeric user and be built for `linux/amd64`; they show `USER 5001` as an example, not a hard-coded required UID. The current repo Dockerfile documents that many mounted secret/token files are readable by UID 5000. Treat UID 5000 as our template default for compatibility, while documenting that the official rule is numeric non-root.

## Research notes and constraints

### Official Palantir compute module/container constraints

From Palantir Compute Modules container docs:

- Images must run as a non-root numeric user.
- Images must be built for `linux/amd64`.
- Published image tags must not be `latest`; use a digest or non-`latest` tag.
- Exposed ports, if any, must be in 1024-65535 excluding 8945 and 8946.
- Palantir's Dockerfile example uses `RUN adduser --uid 5001 user` and `USER 5001`; this demonstrates numeric non-root, not necessarily UID 5001 as a fixed requirement.

Source: https://www.palantir.com/docs/foundry/compute-modules/containers

From Palantir pipeline execution mode docs:

- Pipeline mode is intentionally non-interactive due to provenance/security controls.
- The build system provides inputs and outputs; users cannot directly query the module in pipeline mode.
- `BUILD2_TOKEN` points to a mounted bearer-token file.
- `RESOURCE_ALIAS_MAP` points to a mounted JSON file mapping configured aliases to RID/branch tuples.
- Supported pipeline resources include datasets, streaming datasets, and media sets.
- Build-system transactions on output datasets are managed automatically and committed on success.

Source: https://www.palantir.com/docs/foundry/compute-modules/execution-modes

### Foundry preview/build ergonomics to emulate

From Palantir VS Code/Transforms docs:

- Preview is a fast feedback loop for transform logic before a full build.
- Preview runs against real input data and shows immediate results.
- Preview supports data loading strategies: sampled default, full dataset, and code-defined filters.
- The default sampled preview uses 1000 input rows.
- Active Preview re-runs on save and uses caching; only one active preview runs at a time.
- Build is the full production-path execution that commits output datasets and exposes status/logs.

Sources:

- https://www.palantir.com/docs/foundry/palantir-extension-for-visual-studio-code/transforms-preview/
- https://www.palantir.com/docs/foundry/palantir-extension-for-visual-studio-code/active-preview/
- https://www.palantir.com/docs/foundry/palantir-extension-for-visual-studio-code/transforms-build/
- https://www.palantir.com/docs/foundry/code-repositories/create-transforms/

## Current repo/template gap analysis

### Root project Dockerfile: mostly good

`Dockerfile` currently has:

```dockerfile
FROM --platform=linux/amd64 golang:1.25 AS builder
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ...
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates ...
USER 5000:5000
ENTRYPOINT ["/enricher", "foundry"]
```

This aligns with the known constraints:

- Explicit amd64 builder platform and GOARCH.
- Debian slim runtime rather than Alpine.
- CA certificates included for outbound HTTPS.
- Numeric non-root user.
- No exposed ports for pipeline mode.

Open question: runtime stage does not declare `--platform=linux/amd64`. Usually the build command should pass `--platform linux/amd64`; still, adding `FROM --platform=linux/amd64 debian:bookworm-slim` to the template would make the intent obvious.

### Generated template Dockerfile: needs hardening

`internal/devx/templates/foundry/Dockerfile.tmpl` currently has:

```dockerfile
FROM golang:1.25.8-alpine AS build
...
FROM alpine:3.22
COPY --from=build /out/compute-module /usr/local/bin/compute-module
ENTRYPOINT ["compute-module"]
```

Problems for a high-quality Foundry template:

- Alpine runtime is not forbidden by the official docs, but user experience indicates org/runtime limitations. Avoid it in the template.
- No explicit `linux/amd64` platform.
- No numeric non-root `USER`.
- No `ca-certificates`; future examples that call Foundry/external HTTPS may fail or behave differently than root project.
- No comments explaining Foundry constraints.

Recommended replacement shape:

```dockerfile
# syntax=docker/dockerfile:1

FROM --platform=linux/amd64 golang:1.25.8-bookworm AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/compute-module ./cmd/compute-module

FROM --platform=linux/amd64 debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/compute-module /usr/local/bin/compute-module
USER 5000:5000
ENTRYPOINT ["compute-module"]
```

Notes:

- Use `bookworm`/`bookworm-slim` instead of Alpine for compatibility, even though static Go binaries can run on Alpine.
- Use UID/GID `5000:5000` as our default because the root project already captured that as a Foundry-mounted-token compatibility convention. Mention official docs only require numeric non-root.
- Do not add `EXPOSE` for pipeline-mode starter templates. If function/server templates are added later, validate ports against Foundry constraints.

### Generated docker-compose.local.yml: works but is too implementation-heavy for average users

Current generated compose runs mock Foundry as `golang:1.25.8-alpine` and uses `go run github.com/.../cmd/mock-foundry@v...`. This can work but is slow/noisy and exposes too much infrastructure to users.

Recommended near-term stance:

- Keep compose for parity/debug mode.
- Introduce `foundry-cmgo preview` and `foundry-cmgo build` as the normal workflow.
- The CLI should start an in-process mock Foundry server by default instead of asking users to understand compose, ports, seed commands, and mounted paths.
- Add `--container` later for Docker parity checks.

## Proposed CLI UX

### Generated project config

Generate a small config file so average users do not memorize flags:

```yaml
# foundry-cmgo.yaml
version: 1
module:
  command: ["go", "run", "./cmd/compute-module", "foundry"]
inputs:
  - alias: input
    path: data/input.csv
outputs:
  - alias: output
    mode: dataset # dataset|stream
mockFoundry:
  root: .local/mock-foundry
  branch: master
preview:
  rows: 1000
  strategy: sampled # sampled|full|filter
```

Reasons:

- Mirrors Foundry's configured input/output aliases.
- Keeps `RESOURCE_ALIAS_MAP` as generated platform-like data, not a thing users need to remember.
- Creates a stable target for `preview`, `build`, `doctor`, and future VS Code tasks.

### `foundry-cmgo preview`

Primary fast feedback command.

Default:

```bash
foundry-cmgo preview
```

Useful flags:

```bash
foundry-cmgo preview --rows 20
foundry-cmgo preview --input data/input.csv --output-mode dataset
foundry-cmgo preview --output-mode stream --follow
foundry-cmgo preview --full
foundry-cmgo preview --filter 'email contains "@acme.com"'
foundry-cmgo preview --json
```

Behavior:

1. Load `foundry-cmgo.yaml` or infer defaults.
2. Validate project structure and Dockerfile constraints lightly.
3. Create isolated preview state under `.local/foundry-cmgo/previews/<run-id>/`.
4. Materialize a sampled/filtered input CSV into mock Foundry input layout.
5. Start in-process mock Foundry on a random localhost port.
6. Run configured module command with:
   - `FOUNDRY_URL=http://127.0.0.1:<port>`
   - `BUILD2_TOKEN=<preview-state>/token.txt`
   - `RESOURCE_ALIAS_MAP=<preview-state>/alias-map.json`
7. Read output dataset or stream records from mock Foundry.
8. Render a Foundry-like run summary and preview table.
9. Exit nonzero if the module fails or output cannot be read.

Output should look like a product, not a raw script:

```text
Foundry CMGO Preview  ✓

Transform       cmd/compute-module foundry
Input           input  data/input.csv  sampled 20/2 rows
Output          output dataset @ master
Runtime         local process  184ms

┌───────────────────┬───────────────────┬────────┐
│ email             │ value             │ status │
├───────────────────┼───────────────────┼────────┤
│ alice@example.com │ ALICE@EXAMPLE.COM │ ok     │
│ bob@example.com   │ BOB@EXAMPLE.COM   │ ok     │
└───────────────────┴───────────────────┴────────┘

2 rows previewed · 2 ok · 0 failed
State: .local/foundry-cmgo/previews/2026-04-24T...
Next: foundry-cmgo build
```

For stream preview with `--follow`, do not jump straight to Bubble Tea. Start with incremental append/log-table rendering:

```text
Foundry CMGO Stream Preview
output stream output @ master

[00:00.041] published alice@example.com  ok
[00:00.067] published bob@example.com    ok

┌────────────┬───────┬────────┐
│ published  │ ok    │ failed │
├────────────┼───────┼────────┤
│ 2          │ 2     │ 0      │
└────────────┴───────┴────────┘
```

If later the output is too noisy, add a real TUI. Do not start there.

### `foundry-cmgo build`

Full local build that writes/commits the output in local mock Foundry state.

```bash
foundry-cmgo build
foundry-cmgo build --output-mode stream
foundry-cmgo build --container
```

Behavior:

1. Load full configured input, not sampled.
2. Use durable state under `.local/mock-foundry/` or `.local/foundry-cmgo/builds/<run-id>/` plus a `last` pointer.
3. Run the configured module command.
4. Commit/read the output dataset or stream.
5. Print where the output is and how to inspect it.

Output:

```text
Foundry CMGO Build  ✓

Input     input   ri.foundry...111  data/input.csv  2 rows
Output    output  ri.foundry...222  dataset @ master
Duration  221ms

Committed dataset:
.local/mock-foundry/uploads/ri.foundry...222/_branches/master/_committed/readTable.csv

Next:
  foundry-cmgo inspect last
  foundry-cmgo preview --rows 20
```

### `foundry-cmgo inspect`

Small but important for “never hunt for what I expect” ergonomics.

```bash
foundry-cmgo inspect last
foundry-cmgo inspect outputs
foundry-cmgo inspect config
```

Should show:

- Last run status.
- Input/output aliases and RIDs.
- Output file path.
- Dataset/stream row count.
- Recent errors/log path.

### `foundry-cmgo doctor`

Add `doctor project` and `doctor docker`.

Checks:

- `foundry-cmgo.yaml` exists and references real files.
- `RESOURCE_ALIAS_MAP` aliases are unique and include input/output.
- Generated command can print help/version or `go test ./...` passes.
- Dockerfile has explicit `linux/amd64` build/runtime platform or docs build command enforces it.
- Runtime is not root and is numeric (`USER 5000:5000` or other numeric UID/GID).
- Avoids `latest` image tags in publish docs/config.
- Warns on Alpine runtime for Foundry template compatibility.
- Warns if HTTPS-capable examples lack CA certificates.
- Warns on `EXPOSE` outside allowed port range if present.

### Keep low-level commands

Do not remove:

```bash
foundry-cmgo seed dataset ...
foundry-cmgo seed stream ...
```

But move docs so they are “advanced/debug plumbing”. Normal users should not need them for the first happy path.

## Implementation plan for next agent

### Phase 1: Docker/template hardening

Files:

- `internal/devx/templates/foundry/Dockerfile.tmpl`
- possibly `Dockerfile` for comment/platform parity only
- `internal/devx/templates/foundry/README.md.tmpl`
- generated-project tests under `internal/devx/project_test.go`

Steps:

1. Replace generated Dockerfile template with Debian slim, `linux/amd64`, `ca-certificates`, and `USER 5000:5000`.
2. Add README explanation: official Foundry constraints are numeric non-root + amd64; this template defaults to UID 5000 for mounted-token compatibility.
3. Update/golden-check generated template tests if they assert files.
4. Verify `go test ./internal/devx ./cmd/foundry-cmgo` and generate a sample project.

Acceptance:

- Newly generated dataset/stream project Dockerfile no longer uses Alpine.
- Dockerfile has numeric non-root user.
- Dockerfile visibly targets amd64.

### Phase 2: Project config generation

Files:

- `internal/devx/templates/foundry/foundry-cmgo.yaml.tmpl` (new)
- `internal/devx/project.go`
- `internal/devx/project_test.go`
- README templates

Steps:

1. Generate `foundry-cmgo.yaml` for dataset and stream starters.
2. Include module command, input path, aliases, default output mode, mock root, preview rows.
3. Add config parser package, likely `internal/devx/config.go` or `internal/devx/project_config.go`.
4. Add unit tests for default inference and explicit config.

Acceptance:

- New projects can run `foundry-cmgo preview` with no flags.
- Existing projects without config get helpful inference/error text.

### Phase 3: Local preview command

Files:

- `cmd/foundry-cmgo/main.go`
- `internal/devx/preview.go` (new)
- `internal/devx/render.go` or `internal/devx/table.go` (new)
- `pkg/mockfoundry` only if needed for in-process orchestration hooks

Steps:

1. Add `preview` command parsing.
2. Implement sampled CSV writer. Default rows should be 1000 to match Foundry preview default semantics.
3. Start mock Foundry in-process on `127.0.0.1:0`.
4. Run configured command with env vars.
5. Capture stdout/stderr to run log file and concise terminal output.
6. Read output dataset/stream and render first N rows.
7. Add `--json` for machine-readable agent/test use.

Rendering recommendation:

- Prefer existing `lipgloss` + `text/tabwriter` initially to avoid dependency churn.
- If the result is not good enough, add `github.com/jedib0t/go-pretty/v6/table` as an explicit UX dependency in the implementation PR. The user specifically floated go-pretty, so this is acceptable if the implementer records the dependency rationale.

Acceptance:

- From a generated dataset project: `foundry-cmgo preview` shows a styled table and exits 0.
- From a generated stream project: `foundry-cmgo preview --output-mode stream` shows published records or a table of stream output.
- A missing input/config gives a Foundry-like actionable error, not a Go stack/raw failure.

### Phase 4: Local build command

Files:

- `cmd/foundry-cmgo/main.go`
- `internal/devx/build.go` (new)
- shared preview/build orchestration package

Steps:

1. Reuse preview orchestration but disable sampling.
2. Write durable local build state.
3. Add a `last` pointer or manifest under `.local/foundry-cmgo/last-run.json`.
4. Print output dataset/stream path and summary.

Acceptance:

- `foundry-cmgo build` runs full input and writes the final local output.
- `foundry-cmgo inspect last` can locate and display it.

### Phase 5: Watch/active-preview-lite

Do this after preview/build are stable.

```bash
foundry-cmgo preview --watch
```

Behavior:

- Watch relevant files and rerun on save.
- Abort/restart current preview when new changes arrive.
- Cache input sample between runs.
- Print only changed status/results unless `--verbose`.

Acceptance:

- Save processor code, preview re-runs automatically.
- Only one preview process runs at a time.
- Clear Ctrl-C cleanup.

## UX principles for average Palantir-first users

1. The happy path should be `new -> preview -> build`, not `seed -> docker compose -> curl -> inspect files`.
2. Always show aliases and human names before RIDs; keep RIDs visible but secondary.
3. Always show where output went and the next command.
4. Errors should name the missing Foundry-like concept: input alias, output alias, token, resource map, dataset output, stream output.
5. Preview should be safe/non-destructive and visibly sampled/filtered.
6. Build should be full and visibly committed.
7. Keep local process mode as default because it feels instant. Make Docker/container parity opt-in.
8. Use Foundry terminology consistently: Preview, Build, Input, Output, Dataset, Stream, Branch, Run logs, Commit.

## Verification checklist for implementation PR

- `go test ./...`
- Generate fresh `dataset` and `stream` starter projects into temp dirs.
- In each generated project:
  - `go test ./...`
  - `foundry-cmgo preview`
  - `foundry-cmgo build`
  - `foundry-cmgo inspect last`
- Verify `foundry-cmgo preview` from outside the source repo using both `foundry-cmgo` and `foundry-cmgo-dev`.
- Verify Dockerfile doctor catches Alpine/no-user/no-amd64 fixture.
- Optional Docker parity:
  - `docker build --platform linux/amd64 .`
  - `docker compose -f docker-compose.local.yml up --build --abort-on-container-exit`

## Open questions for later, not blockers

- Should stream preview show rows as they are published, or show a final table plus a compact event log? Recommendation: compact event log first.
- Should local preview use Docker by default for parity? Recommendation: no; keep local process default and add `--container`.
- Should config support multiple transforms/modules in one repo? Recommendation: design schema with arrays now, but implement single-transform happy path first.
- Should we commit `.local/foundry-cmgo/last-run.json`? Recommendation: no; add to generated `.gitignore`.
