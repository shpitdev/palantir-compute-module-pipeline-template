# Foundry Parity Cleanup Plan

## Purpose

This plan translates `docs/FOUNDRY_PARITY.md` into a small, reversible implementation sequence.

The goal is to mature the current Go template without changing its product behavior:

- keep local CSV, dataset-output, and stream-output flows working,
- make the Foundry parity contract explicit in code boundaries,
- remove schema/codec duplication,
- preserve the current legacy stream-proxy implementation while leaving room for a future high-scale streams backend,
- and grow tests around contract behavior before larger refactors.

## Guardrails

- No new dependencies.
- Prefer moving existing code over rewriting behavior.
- Lock behavior with focused tests before cleanup edits where coverage is missing.
- Keep Gemini/Exa provider choice out of this cleanup slice.
- Keep `pkg/mockfoundry` lightweight; do not attempt full Foundry service parity.
- Treat Tabex as a last-resort evidence tool for unresolved real-stack behavior only.

## Current duplication / risk map

| Area | Current files | Risk |
| --- | --- | --- |
| Output schema columns | `examples/email_enricher/pipeline/rows.go`, `pkg/mockfoundry/server.go` | CSV/stream/mock projections can drift. |
| Stream record codec | `internal/app/enricher.go` | App orchestration owns example-specific serialization. |
| Stream read normalization | `pkg/foundry/client.go`, `internal/app/enricher.go` | Transport envelope parsing and row wrapper parsing are split. |
| Incremental row selection | `internal/app/enricher.go` | Useful behavior is buried inside orchestration. |
| Retry classification | `pkg/pipeline/io/foundry/io.go`, `internal/app/enricher.go`, `pkg/pipeline/worker/worker.go` | Different layers can drift on retry/error semantics. |
| Stream API choice | `pkg/foundry/client.go`, `pkg/pipeline/io/foundry/io.go` | Legacy stream-proxy path is not isolated from future high-scale API support. |

## P0 — Contract and behavior locks

### P0.1 Add parity contract doc

Status: done in `docs/FOUNDRY_PARITY.md`.

### P0.2 Add a focused codec test before moving code

Add tests in `examples/email_enricher/pipeline` that prove:

- a `Row` encodes to the legacy stream JSON record shape,
- empty optional values become JSON `nil`,
- wrapper shapes like `{record:{...}}`, `{value:{...}}`, and `{data:{...}}` decode to the same `Row`,
- stream table CSV projection uses `Header()` plus stream metadata columns.

## P1 — Shared output codec ownership

Move example-specific stream/table encoding out of `internal/app/enricher.go` and into `examples/email_enricher/pipeline`.

Target API:

- `StreamMetadataHeader() []string`
- `StreamTableHeader() []string`
- `NormalizeStreamRecord(map[string]any) map[string]any`
- `RowFromStreamRecord(map[string]any) Row`
- `RowToStreamRecord(Row) map[string]any`
- `WriteStreamRecordsCSV(io.Writer, []map[string]any) error`

Expected simplification:

- `internal/app/enricher.go` calls the pipeline codec instead of hardcoding record fields.
- `pkg/mockfoundry/server.go` no longer hardcodes the email-enricher stream CSV header; the example binary/tests inject `pipeline.StreamTableHeader()`.

Risk:

- Stream table projection becomes configurable instead of fully schema-aware. That keeps `pkg/mockfoundry` generic enough for the template, but callers must provide a header when they need stable empty-table or downstream `ReadCSV` behavior.

## P2 — Dataset-view and incremental extraction

After P1 is green, extract incremental cache behavior from `internal/app/enricher.go` into a small package or file boundary.

Candidate boundary:

- `internal/app/incremental.go` initially, then promote only if it proves reusable.

Move:

- `incrementalPlan`
- `buildIncrementalPlan`
- `applyEnrichedRows`
- `chooseBestIncrementalRow`
- `emailKey`
- status counting if it remains app-local

Expected simplification:

- `RunFoundry` becomes a readable orchestration flow rather than a mixed orchestration/merge/codec file.

## P3 — Stream backend boundary

Introduce a legacy stream backend interface after schema/codec cleanup stabilizes.

Candidate boundary:

```go
type StreamBackend interface {
    Probe(ctx context.Context, ref foundry.DatasetRef) (bool, error)
    ReadRecords(ctx context.Context, ref foundry.DatasetRef) ([]map[string]any, error)
    PublishRecord(ctx context.Context, ref foundry.DatasetRef, record map[string]any) error
}
```

Initial implementation:

- `LegacyStreamProxyBackend` wrapping the current `foundry.Client` methods.

Deferred implementation:

- `HighScaleStreamsBackend` for the newer public platform SDK surface.

## P4 — Central retry/error policy

Centralize Foundry retry classification after the adapter boundaries exist.

Target:

- one retry policy helper for Foundry I/O calls,
- explicit named handling for `429`, `5xx`, connection reset/refused, and context deadlines,
- explicit non-retry handling for permission/not-found where those are contract-level outcomes.

## Immediate implementation slice

This branch should implement P0.2 + P1 only:

1. add stream codec tests,
2. move stream row codec/projection into `examples/email_enricher/pipeline`,
3. update app orchestration to call the shared codec,
4. update mock Foundry stream `readTable` projection to use an injected stream table header,
5. run `go test ./...`.

This is intentionally narrow: it removes the highest-risk schema duplication while avoiding broad runtime rewrites.
