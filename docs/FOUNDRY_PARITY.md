# Foundry Parity Contract

## Purpose

This document defines the explicit Foundry behavior contract targeted by this repo.

The goal is **not** to emulate all of Foundry.

The goal **is** to emulate the specific Foundry runtime, dataset, and stream behavior required so that:

- developers can iterate locally,
- run the same Go binary locally and in Foundry,
- and have high confidence that local behavior matches deployed behavior for the surfaces this template depends on.

This document is the source of truth for:

- emulator scope,
- runtime assumptions,
- integration tests,
- cleanup/de-duplication work,
- future parity investigations,
- future migration decisions.

---

## Status vocabulary

Each claim in this document should be treated as one of:

- **Confirmed** — supported by official Palantir docs, official Palantir SDK code, or direct captured evidence.
- **Inferred** — strongly suggested, but not yet fully proven on the exact target stack(s).
- **Deferred** — intentionally out of scope for parity v1.

---

## Source precedence

If code and docs disagree, use this order:

1. Official Palantir documentation
2. Palantir-owned public SDKs / official examples
3. Direct captured evidence (HAR / traces / responses)
4. Repo-local contract
5. Current implementation

---

# Scope

This repo’s parity target includes:

1. **Pipeline-mode compute module runtime**
2. **Dataset view / read semantics**
3. **Dataset transaction + file upload + commit semantics**
4. **Legacy compute-module stream-proxy semantics**
5. **Stream-backed dataset-view semantics**
6. **Minimal internal runtime keepalive semantics**

This repo’s parity target does **not** include:

- full Foundry UI behavior
- full GraphQL gateway behavior
- full catalog / metadata service parity
- full row-level policy enforcement behavior
- full authz parity
- exact parity for every stream API family
- exact retention / cursor / compaction behavior
- complete infra provisioning parity

---

# 1. Compute module runtime contract

## 1.1 Pipeline-mode environment

### Confirmed

Official compute-module SDKs and docs confirm that pipeline-mode modules use mounted/runtime-provided values including:

- `BUILD2_TOKEN`
- `RESOURCE_ALIAS_MAP`

Additional service discovery/runtime variables supported by official SDKs include:

- `FOUNDRY_SERVICE_DISCOVERY_V2`
- `DEFAULT_CA_PATH`

### Required behavior

For this repo:

- `BUILD2_TOKEN` is a **file path** to a bearer token
- `RESOURCE_ALIAS_MAP` is a **file path** to JSON alias metadata
- alias entries are shaped as:
  - `rid`
  - optional `branch`
- if `branch` is absent, default branch behavior is assumed

### Local implementation requirement

The local harness must provide file-backed equivalents of these vars exactly as Foundry does.

---

## 1.2 Resource alias contract

### Confirmed

Official TypeScript and Python compute-module SDKs both model pipeline resources as:

- alias -> `{ rid, branch? }`

### Required behavior

This repo assumes:

- aliases are the stable application-facing identifiers
- actual resource RIDs may vary by environment
- runtime code should resolve resources by alias, not hardcoded RID

### Cleanup implication

Alias parsing should remain centralized and schema-stable.

---

## 1.3 Pipeline token contract

### Confirmed

Official Python and TypeScript compute-module SDKs both read `BUILD2_TOKEN` from a mounted file.

### Required behavior

This repo assumes:

- pipeline-mode Foundry API calls use the mounted build token
- the token is read from disk, not directly from env value contents

### Cleanup implication

This should remain a file-backed invariant across runtime, tests, and emulator setup.

---

## 1.4 Foundry service discovery contract

### Confirmed

Official compute-module SDKs read `FOUNDRY_SERVICE_DISCOVERY_V2` and expose at least:

- `api_gateway`
- `stream_proxy`

Official Python platform SDK routing also distinguishes:

- generic API endpoint
- auth endpoint
- high-scale endpoint

### Required behavior

This repo’s runtime contract is:

- `api_gateway` resolves dataset/general Foundry API calls
- `stream_proxy` resolves stream/high-scale calls
- fallback `FOUNDRY_URL` may be supported locally, but is a convenience path, not the primary platform contract

### Cleanup implication

The parity doc and code should make clear that `FOUNDRY_URL` is a back-compat/local convenience, while service discovery is the primary Foundry-native contract.

---

## 1.5 Internal runtime endpoints

### Confirmed

Official TypeScript compute-module SDK uses:

- `GET_JOB_URI`
- `POST_RESULT_URI`
- `POST_SCHEMA_URI`
- `MODULE_AUTH_TOKEN`
- `DEFAULT_CA_PATH`

### Required behavior

For this repo:

- these endpoints are treated as **internal runtime infrastructure**
- not as the primary business/data API surface
- keepalive/query-ack behavior is allowed to exist even when the repo is operating in pipeline-oriented mode

### Inferred

Some pipeline-mode deployments may still expect these endpoints to be exercised so the container is considered responsive.

### Emulator requirement

Parity v1 only needs minimal support for:

- GET next job
- POST result
- auth via `Module-Auth-Token`
- optional CA-based HTTPS behavior if tested

---

# 2. Dataset view contract

This section is stronger than the earlier draft and should be treated as a first-class concept.

## 2.1 Dataset view model

### Confirmed

Official dataset SDK comments describe a **dataset view** as the effective file contents of a dataset for a branch and/or transaction context.

### Required behavior

This repo adopts the following dataset-view model:

A dataset view may be resolved from:

- a **branch**
- an **end transaction**
- a **transaction range**
- a **specific transaction** (by setting start and end transaction equal)

### Cleanup implication

Code should conceptually operate on **dataset views**, not just raw endpoint calls.

---

## 2.2 Branch view

### Confirmed

Branch-aware reads and file listings are officially supported.

### Required behavior

A branch view should represent:

- the latest resolved dataset state for that branch
- usually defaulting to `master` if no branch is provided

### Emulator requirement

The emulator must support branch-specific logical state.

---

## 2.3 Transaction-resolved view

### Confirmed

Official dataset SDKs support:

- `endTransactionRid`
- `startTransactionRid`
- transaction-equality for exact-transaction views

### Required behavior

For this repo:

- `endTransactionRid` may resolve the latest effective view at that transaction
- `startTransactionRid + endTransactionRid` may resolve a range-constrained view
- `start == end` should be interpreted as a specific transaction view

### Emulator requirement

Parity v1 does not need a perfect full historical engine, but it must preserve enough transaction/view semantics to support the repo’s real behaviors.

---

## 2.4 Snapshot reset semantics

### Confirmed

Official dataset SDK comments explicitly state that:

- an intermediate snapshot transaction resets/removes prior files from the resolved file view

### Required behavior

This repo’s parity model must explicitly acknowledge:

- dataset/file views are not simple append-only histories
- snapshot boundaries matter
- range semantics differ from naive cumulative append semantics

### Emulator requirement

Parity v1 may simplify this internally, but the contract must not pretend dataset history is append-only.

---

## 2.5 Dataset view not found

### Confirmed

Official dataset SDK error types include a `DatasetViewNotFound` concept.

### Required behavior

A dataset view may be absent if:

- the dataset has no transactions
- the dataset has no files in the resolved view
- the branch is invalid
- the caller lacks access

### Emulator requirement

The emulator should be able to represent “no dataset view available” distinctly from unrelated generic failures.

---

# 3. Dataset read contract

## 3.1 Read API surface

### Confirmed

The repo relies on `readTable`.

Official SDKs confirm parameters including:

- `branchName` / older `branchId`
- `startTransactionRid`
- `endTransactionRid`
- `format`
- `columns`
- `rowLimit`

### Required behavior

For this repo:

- reads are transaction/view-aware
- CSV is the primary format used
- branch defaults to `master` when omitted
- transaction pinning should be used where deterministic reads matter

---

## 3.2 Ordering

### Confirmed

Official docs note that row ordering should not be treated as deterministic.

### Required behavior

This repo must not rely on row order for correctness.

### Emulator requirement

The emulator does not need to randomize rows, but tests and merge logic must assume ordering instability.

---

## 3.3 Permission-denied and not-found reads

### Confirmed

Official SDKs distinguish errors such as:

- `ReadTablePermissionDenied`
- schema/view not found cases

### Required behavior

For this repo’s incremental logic:

- `404`/view-missing => treat as no prior readable output where appropriate
- `403` => treat as unreadable prior output where appropriate
- other failures => surface unless explicitly classified otherwise

---

# 4. Dataset transaction contract

## 4.1 Create transaction

### Confirmed

Official SDKs confirm:

- `POST /v2/datasets/{datasetRid}/transactions`
- optional `branchName`
- one open transaction per dataset branch
- `OpenTransactionAlreadyExists`

### Required behavior

This repo assumes:

- transaction creation is branch-scoped
- open-transaction conflicts are real platform behavior
- branch defaults to `master` for most enrollments when omitted

### Emulator requirement

Must support:

- create
- branch-scoped conflict
- `OpenTransactionAlreadyExists` structured error

---

## 4.2 List transactions

### Confirmed

Official docs and SDKs support listing transaction history and document reverse chronological behavior.

### Required behavior

This repo assumes:

- reverse chronological transaction history
- OPEN transactions are discoverable for fallback/reuse logic
- preview-gated/list-history behavior may exist depending on endpoint flavor

### Emulator requirement

Must support enough list behavior to find the most recent OPEN transaction.

---

## 4.3 Reuse existing open transaction

### Confirmed

This repo’s current fallback strategy aligns with official transaction constraints:

1. try create
2. on `OpenTransactionAlreadyExists`, list history
3. reuse latest OPEN transaction
4. upload to that transaction
5. avoid committing if ownership is external/Foundry-managed

### Status

- **Confirmed** that open transaction conflict exists
- **Inferred** that pipeline builds may pre-open and externally manage output transaction lifecycle in the relevant cases

### Emulator requirement

Must support this flow explicitly.

---

## 4.4 Commit transaction

### Confirmed

Official SDKs document that commit:

- preserves file modifications
- updates the branch to point to the transaction

### Required behavior

This repo assumes commit causes:

- logical branch-head advancement
- read-after-write visibility
- transaction status transition from OPEN to COMMITTED

### Emulator requirement

Must model branch head updates.

---

# 5. Dataset file contract

This section is important because the platform SDKs give stronger semantics than the earlier draft captured.

## 5.1 Upload file

### Confirmed

Official SDKs document:

- `POST /v2/datasets/{datasetRid}/files/{filePath}/upload`
- upload to:
  - a new branch-scoped transaction via `branchName`
  - or a manually opened transaction via `transactionRid`
- if `branchName` is used directly, transaction creation/commit may happen implicitly
- default transaction type for branch-based upload is `UPDATE`

### Required behavior

This repo currently uses the **manual transaction** path.

The parity contract should therefore distinguish:

#### Supported by this repo now

- upload into manually opened transaction via `transactionRid`

#### Supported by Foundry generally

- upload directly to branch with implicit transaction lifecycle

### Cleanup implication

The contract and code should not conflate these two modes.

---

## 5.2 File-view semantics

### Confirmed

Official SDKs document file listing/content/metadata resolution by:

- branch
- transaction
- transaction range
- exact transaction

### Required behavior

The parity contract should explicitly say:

- dataset file visibility is view-derived
- the repo’s dataset semantics are really dataset-view semantics

This makes the contract much more precise.

---

# 6. Stream contract

This section is the biggest place where the plan is now sharper.

## 6.1 Two stream API families exist

### Confirmed

From docs, HAR, and local platform SDK repos, there are at least two relevant stream surfaces:

## A. Legacy compute-module-oriented stream-proxy surface

Observed in compute-module docs and current repo:

- `GET /stream-proxy/api/streams/{rid}/branches/{branch}/records`
- `POST /stream-proxy/api/streams/{rid}/branches/{branch}/jsonRecord`

## B. Newer public high-scale streams API

Observed in official platform SDKs:

- `POST /v2/highScale/streams/datasets/{datasetRid}/streams/{streamBranchName}/publishRecord`
- `POST /v2/highScale/streams/datasets/{datasetRid}/streams/{streamBranchName}/publishRecords`
- `GET /v2/highScale/streams/datasets/{datasetRid}/streams/{streamBranchName}/getRecords`
- `GET /v2/highScale/streams/datasets/{datasetRid}/streams/{streamBranchName}/getEndOffsets`

These high-scale calls are routed through the stream/high-scale endpoint.

---

## 6.2 Current repo stance

### Confirmed

This repo currently targets the **legacy compute-module-compatible stream-proxy surface**.

### Required behavior

Parity v1 for this repo should remain defined in terms of the legacy surface:

- probe by reading `records`
- publish one row at a time via `jsonRecord`
- use stream records as best-effort incremental cache input

### Design note

The parity doc should explicitly state that this is a deliberate implementation stance, not a claim that it is the only Foundry stream API.

---

## 6.3 High-scale streams surface is observed but deferred

### Confirmed

Official platform SDKs show the newer streams model includes:

- schema-validated record publishing
- partitions
- sparse offsets
- `viewRid`
- end offsets
- subscriber APIs
- record size limits

### Deferred for parity v1

This repo does **not** currently model the full newer high-scale streams contract.

### Cleanup implication

Stream I/O should be isolated behind an adapter so future support for the newer surface does not require another orchestration rewrite.

---

## 6.4 Legacy stream-proxy response shape variability

### Confirmed

Docs and current repo tests already reflect that `records` responses may vary in shape.

### Required behavior

This repo should tolerate at least:

- top-level array of records
- object-wrapped list responses
- per-record wrappers such as:
  - `record`
  - `value`
  - `data`

### Emulator requirement

The emulator may return one canonical shape, but tests should preserve parser tolerance.

---

# 7. Stream-backed dataset-view contract

This remains central.

## 7.1 Streams also have dataset-view semantics

### Confirmed

Official stream docs plus HAR evidence indicate:

- stream data has low-latency streaming behavior
- and also dataset-style archived/view behavior

### Required behavior

This repo’s parity contract is:

- stream-backed outputs are not only “push endpoints”
- they also eventually participate in dataset-view semantics

---

## 7.2 Archived stream data appears as transaction/file history

### Confirmed

HAR evidence shows stream-backed datasets with:

- transaction history
- file listings
- archived files like `part=0_start=...avro`
- stream metadata referencing views and start transaction RIDs

### Required behavior

The parity model should explicitly include:

- append-style transactional history for archived stream data
- dataset-view readability of archived stream data
- schema continuity between streaming and dataset-view representations

---

## 7.3 Stream views

### Confirmed

Official platform SDKs for newer stream APIs expose `viewRid`.

HAR evidence also shows stream latest-view metadata.

### Required behavior

Parity v1 does not need to fully implement view RID control, but the contract should acknowledge that:

- streams have view identity
- the “latest stream on a branch” is a real concept
- stream reset can create a new view

---

# 8. Error contract

## 8.1 Structured Foundry errors

### Confirmed

Official SDK error types and observed behavior support structured errors with:

- `errorCode`
- `errorName`
- `errorInstanceId`
- typed parameters

### Required behavior

The emulator should return structured errors for key paths, including:

- `OpenTransactionAlreadyExists`
- permission denied cases
- transaction not found / not open
- invalid dataset/file path cases
- stream permission/validation failures where modeled

---

## 8.2 Important named errors for this repo

### Confirmed useful names

Dataset side:

- `OpenTransactionAlreadyExists`
- `TransactionNotFound`
- `TransactionNotOpen`
- `ReadTablePermissionDenied`
- `DatasetViewNotFound`
- `FileAlreadyExists`

Stream side:

- `PublishRecordToStreamPermissionDenied`
- `PublishRecordsToStreamPermissionDenied`
- `RecordDoesNotMatchStreamSchema`
- `RecordTooLarge`

### Cleanup implication

Error handling in the repo should become more explicit and centralized around these named classes of failure.

---

# 9. Authentication and authorization contract

## 9.1 Authentication

### Confirmed

There are distinct auth/header modes for:

- pipeline build token / bearer-style calls
- internal runtime `Module-Auth-Token`

### Required behavior

Parity v1 must support:

- bearer auth for dataset/stream-facing calls
- module auth for internal runtime endpoints

---

## 9.2 Authorization

### Deferred / minimal v1 model

Full Foundry authz is out of scope.

Parity v1 only needs to support meaningful distinctions between:

- success
- not found
- permission denied

for the endpoints this repo depends on.

---

# 10. Retry contract

## 10.1 Current repo behavior

The repo currently retries some transient failures such as:

- network timeouts
- connection resets/refusals
- `429`
- `5xx`

### Status

- **Confirmed** as repo behavior
- **Reasonable** relative to platform semantics
- **Not yet exhaustively validated** endpoint by endpoint

## 10.2 Contract requirement

Retryability must become explicit and centralized.

This should be modeled separately for:

- dataset reads/writes
- stream writes
- internal runtime polling

---

# 11. Deferred parity

The following are intentionally out of scope for parity v1:

- full public high-scale streams implementation
- subscriber APIs
- exact partition offset semantics
- exact `viewRid` lifecycle semantics
- exact stream retention/compaction behavior
- full row-level policy service behavior
- full UI service graph parity
- exact catalog/metadata service parity
- full Terraform/provider parity

---

# 12. Cleanup implications

## 12.1 Introduce a single canonical row/schema contract

Unify ownership of:

- field list
- CSV header
- CSV encode/decode
- stream-record encode/decode
- nullability behavior
- metadata fields like `run_id`, `written_at`

This should eliminate the current duplication between:

- example pipeline code
- app orchestration
- mock Foundry server

---

## 12.2 Introduce explicit adapter boundaries

### Dataset side

Introduce a dataset-view-oriented boundary, not just raw endpoint helpers.

### Stream side

Introduce an adapter such as:

- `StreamBackend`
  - `LegacyStreamProxyBackend`
  - future `HighScaleStreamsBackend`

This is now justified by the evidence from official SDK repos.

---

## 12.3 Split orchestration from contract codecs

`internal/app/enricher.go` should stop owning:

- stream record shape logic
- row encoding/decoding logic
- dataset-view merge semantics
- retry classification helpers

Those should move to reusable contract-driven packages.

---

## 12.4 Align tests to the contract

Test groups should be organized around:

### Runtime contract

- file-backed token
- file-backed aliases
- service discovery
- internal runtime env handling

### Dataset-view contract

- branch views
- transaction views
- transaction-range semantics
- open transaction reuse
- commit visibility
- permission/not-found handling

### Legacy stream contract

- probe behavior
- publish behavior
- parser tolerance for response envelopes
- append semantics

### Stream-backed dataset-view contract

- archived stream output visible as dataset-view state
- schema continuity
- transaction/file-history visibility at the level the repo depends on

---

# 13. Open questions

These are now narrower than before.

## Open question 1

Should this template remain on the legacy stream-proxy surface, or eventually add support for the newer high-scale streams API?

## Open question 2

How much of `viewRid` behavior matters for this repo’s actual use case?

## Open question 3

How closely should the emulator model the lag between stream publication and archived dataset-view visibility?

## Open question 4

Which stack-specific response shapes still need verification via **Tabex**?

---

# 14. Evidence used

## Official docs

- Compute modules getting started
- Compute modules containers/env
- dataset read/transaction/file docs
- streams concepts docs

## Official Palantir compute-module SDK repos

- `typescript-compute-module`
- `python-compute-module`

## Official Palantir platform SDK repos

- `foundry-platform-typescript`
- `foundry-platform-python`

## Local captured evidence

- `.memory/data-tmp/23dimethyl.usw-3.palantirfoundry.com-logs-0.0.18-clean-stream.har`
- `.memory/data-tmp/part=0_start=8.avro`

---

# 15. Immediate next steps

## P0

- add this file as `docs/FOUNDRY_PARITY.md`
- explicitly document **legacy vs high-scale stream APIs**
- explicitly document **dataset view semantics**

## P1

- extract unified row/schema/CSV/stream codec ownership
- introduce a `StreamBackend` abstraction
- move dataset-view merge logic out of app orchestration

## P2

- add contract-focused tests
- add emulator support for the remaining required dataset-view and legacy-stream behaviors

## P3

- decide whether to keep legacy stream-proxy only or add an optional high-scale stream backend
- use **Tabex** only for the remaining unresolved stack-specific questions

---

# Recommended note to include in the repo

> This repo currently targets the legacy compute-module-compatible stream-proxy surface for stream outputs. Official platform SDKs also expose a newer high-scale streams API. The codebase should isolate stream I/O behind an adapter boundary so future support for either or both surfaces does not require another orchestration rewrite.
