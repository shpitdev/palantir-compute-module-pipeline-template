# Diagrams

This document contains Mermaid diagrams for how the module uses Foundry-like APIs in different scenarios.

Notes:

- The code in this repo uses a deliberately minimal HTTP surface area.
- `pkg/mockfoundry/...` is "Foundry-like" for local harnesses and tests; it is not a full-fidelity emulator.
- Stream-proxy response shapes can vary by stack/version; the client parses a few common envelope shapes best-effort.

## Output Mode Resolution (auto|dataset|stream)

```mermaid
flowchart TD
    Start([RunFoundry start]) --> Mode{outputWriteMode}

    Mode -->|dataset| Dataset[Use dataset transaction output]
    Mode -->|stream| Stream[Use stream-proxy output]
    Mode -->|auto| Probe[GET stream-proxy /records]

    Probe -->|2xx| Stream
    Probe -->|404| Dataset
    Probe -->|other non-2xx| Fail[Fail run]
```

## Scenario: Foundry Run (Stream Output, Incremental)

This is the most common "streaming" scenario:

- Determine output is a stream via stream-proxy probing
- Read prior stream records to build an incremental cache keyed by `email`
- Enrich only the emails not already present with `status=ok`
- Publish one JSON record per enriched row

```mermaid
sequenceDiagram
    autonumber
    participant CM as Compute Module (container)
    participant API as Foundry API Gateway
    participant SP as Stream-Proxy
    participant G as Gemini API

    CM->>API: GET /api/v2/datasets/{inputRid}/branches/{branch}
    CM->>API: GET /api/v2/datasets/{inputRid}/readTable?format=CSV&branchName={branch}&startTransactionRid={txn}&endTransactionRid={txn}
    API-->>CM: CSV bytes

    CM->>SP: GET /stream-proxy/api/streams/{outputRid}/branches/{branch}/records (probe)
    SP-->>CM: 200 OK

    CM->>SP: GET /stream-proxy/api/streams/{outputRid}/branches/{branch}/records (incremental cache)
    SP-->>CM: records (array or envelope)

    loop For each pending email
        CM->>G: Enrich(email)
        G-->>CM: Result (company/title/etc)
        CM->>SP: POST /stream-proxy/api/streams/{outputRid}/branches/{branch}/jsonRecord
        SP-->>CM: 200 OK
    end

    Note over CM: If compute-module keepalive is enabled,
    Note over CM: container stays running after the run completes.
```

## Scenario: Foundry Run (Dataset Output, Transaction Flow)

Dataset mode writes a full snapshot file into a dataset transaction.

```mermaid
sequenceDiagram
    autonumber
    participant CM as Compute Module (container)
    participant API as Foundry API Gateway
    participant G as Gemini API

    CM->>API: GET /api/v2/datasets/{inputRid}/branches/{branch}
    CM->>API: GET /api/v2/datasets/{inputRid}/readTable?format=CSV&branchName={branch}&startTransactionRid={txn}&endTransactionRid={txn}
    API-->>CM: CSV bytes

    Note over CM: Incremental cache (best-effort)
    CM->>API: GET /api/v2/datasets/{outputRid}/branches/{branch}
    CM->>API: GET /api/v2/datasets/{outputRid}/readTable?format=CSV&branchName={branch}&startTransactionRid={txn}&endTransactionRid={txn}
    API-->>CM: CSV bytes (or 404 / 403)

    loop For each pending email
        CM->>G: Enrich(email)
        G-->>CM: Result
    end

    CM->>API: POST /api/v2/datasets/{outputRid}/transactions?branchName={branch}
    API-->>CM: { rid: {txnRid} }
    CM->>API: POST /api/v2/datasets/{outputRid}/files/{filePath}/upload?transactionRid={txnRid}
    API-->>CM: 200 OK
    CM->>API: POST /api/v2/datasets/{outputRid}/transactions/{txnRid}/commit
    API-->>CM: 200 OK
```

## Scenario: Dataset Output with Pre-existing OPEN Transaction

In Foundry pipeline mode, the platform may create the output transaction before starting the module.
In that case, creating a new transaction can conflict.

```mermaid
sequenceDiagram
    autonumber
    participant CM as Compute Module (container)
    participant API as Foundry API Gateway

    CM->>API: POST /api/v2/datasets/OUTPUT_RID/transactions?branchName=BRANCH
    API-->>CM: 409 OpenTransactionAlreadyExists

    CM->>API: GET /api/v2/datasets/OUTPUT_RID/transactions?preview=true
    API-->>CM: list transactions (includes OPEN)

    CM->>API: POST /api/v2/datasets/OUTPUT_RID/files/FILE_PATH/upload?transactionRid=OPEN_TXN_RID
    API-->>CM: 200 OK

    Note over CM,API: In pipeline mode, Foundry will commit the OPEN transaction.
```

## Scenario: Stream Incremental Cache Read Failures

```mermaid
flowchart TD
    S([Need incremental cache]) --> R[GET stream-proxy /records]
    R -->|200 OK| P[Parse response]
    P -->|Array| Use[Use records]
    P -->|Envelope| Unwrap[Extract values/records + unwrap record/value/data]
    Unwrap --> Use

    R -->|403 Forbidden| NoCache[Proceed without cache]
    R -->|404 Not Found| NoCache
    R -->|Other error| Fail[Fail run]
```

## Mock Foundry API Surface (Local Harness)

The "mock Foundry" server is implemented in:

- `cmd/mock-foundry/main.go`
- `pkg/mockfoundry/server.go`

It supports:

- Debug endpoints (dev/test visibility)
  - `GET /__debug/health`
  - `GET /__debug/calls` (records every request path/method)
  - `GET /__debug/uploads` (what files were uploaded into txns)
  - `GET /__debug/streams` (current in-memory stream records)

- Dataset API (v2)
  - `GET  /api/v2/datasets/{rid}/branches/{branchName}`
  - `GET  /api/v2/datasets/{rid}/readTable` (serves CSV)
  - `GET  /api/v2/datasets/{rid}/transactions?preview=true` (list txns; preview-gated)
  - `POST /api/v2/datasets/{rid}/transactions` (create txn)
  - `POST /api/v2/datasets/{rid}/files/{filePath...}/upload?transactionRid={txn}` (upload file into txn)
  - `POST /api/v2/datasets/{rid}/transactions/{txn}/commit` (commit txn)

- Dataset API (v1, minimal/back-compat)
  - `GET /api/v1/datasets/{rid}/readTable`
  - `PUT /api/v1/datasets/{rid}/transactions/{txn}/files/{path...}`

- Stream-proxy API (optional; only for stream RIDs registered via `MOCK_FOUNDRY_STREAM_RIDS`)
  - `GET  /stream-proxy/api/streams/{rid}/branches/{branch}/records`
  - `POST /stream-proxy/api/streams/{rid}/branches/{branch}/jsonRecord`

Authorization:

- The mock server can enforce `Authorization: Bearer <token>` when configured.

### Are these identical to real Foundry APIs?

No.

They are "Foundry-like" in path + method so the module can be tested end-to-end locally.
Behavior and response schemas are intentionally simplified.

Examples of differences:

- `readTable` is just "fixture CSV or last committed bytes"; it does not implement real Foundry table semantics.
- `branches/{branch}` returns a best-effort transaction RID (or a stable dummy value).
- Transaction listing uses simplified preview gating and transaction selection.
- Stream-proxy is an in-memory append/list implementation; it is not a real stream service (no cursors, limits, retention, authz model, etc.).
