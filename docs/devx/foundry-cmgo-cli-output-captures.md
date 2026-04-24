# `foundry-cmgo` CLI output captures

Date captured: 2026-04-24  
Capture type: deterministic ANSI-stripped text from freshly generated dataset and stream starters. Runtime-specific temp paths, run IDs, timestamps, ports, and durations are normalized.

These captures are the screenshot-equivalent regression artifact for the rounded-out CLI experience: labels are stable, aliases appear before RIDs, artifact paths stay visible, and the next useful command is explicit.

## Dataset starter

```text
$ foundry-cmgo preview --rows 1
Foundry CMGO Preview ✓

Transform          go run ./cmd/compute-module foundry --input-alias input --output-alias output --output-mode dataset
Input              input          data/input.csv    sampled 1/2 rows
Output             output         dataset @ master  1 rows
Runtime            local process  <duration>
Committed dataset  <tmp>/dataset-demo/.local/foundry-cmgo/previews/<run-id>/uploads/ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222/_branches/master/_committed/readTable.csv

email              value              status
alice@example.com  ALICE@EXAMPLE.COM  ok

1 rows output · state: <tmp>/dataset-demo/.local/foundry-cmgo/previews/<run-id> · log: <tmp>/dataset-demo/.local/foundry-cmgo/previews/<run-id>/run.log
next: foundry-cmgo build

$ foundry-cmgo build
Foundry CMGO Build ✓

Transform          Docker container -> foundry --input-alias input --output-alias output --output-mode dataset
Input              input                data/input.csv    2 rows
Output             output               dataset @ master  2 rows
Runtime            container (default)  <duration>
Docker network     host
Committed dataset  <tmp>/dataset-demo/.local/mock-foundry/uploads/ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222/_branches/master/_committed/readTable.csv

note: Docker build is the default parity check; use --local-process for faster host-process debugging.

email              value              status
alice@example.com  ALICE@EXAMPLE.COM  ok
bob@example.com    BOB@EXAMPLE.COM    ok

2 rows output · state: <tmp>/dataset-demo/.local/foundry-cmgo/builds/<run-id> · log: <tmp>/dataset-demo/.local/foundry-cmgo/builds/<run-id>/run.log
next: foundry-cmgo inspect last

$ foundry-cmgo inspect config
Foundry CMGO Config ✓

Source     <tmp>/dataset-demo/foundry-cmgo.yaml
Workdir    <tmp>/dataset-demo
Transform  go run ./cmd/compute-module foundry
Input      input   <tmp>/dataset-demo/data/input.csv
Output     output  dataset @ master
Mock root  <tmp>/dataset-demo/.local/mock-foundry
Preview    1000 rows  sampled
next: foundry-cmgo preview --rows 1

$ foundry-cmgo inspect outputs
Foundry CMGO Outputs ✓

Last run        build   <timestamp>
Output          output  dataset @ master
Rows            2
Artifact        <tmp>/dataset-demo/.local/mock-foundry/uploads/ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222/_branches/master/_committed/readTable.csv
Docker network  host
State           <tmp>/dataset-demo/.local/foundry-cmgo/builds/<run-id>
Log             <tmp>/dataset-demo/.local/foundry-cmgo/builds/<run-id>/run.log
next: inspect the artifact path above or rerun foundry-cmgo build

```

## Stream starter

```text
$ foundry-cmgo preview   # stream starter
Foundry CMGO Preview ✓

Transform       go run ./cmd/compute-module foundry --input-alias input --output-alias output --output-mode stream
Input           input          data/input.csv   2 rows
Output          output         stream @ master  2 rows
Runtime         local process  <duration>
Output records  <tmp>/stream-demo/.local/foundry-cmgo/previews/<run-id>/streams/ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222/master/records.jsonl

email              value              status
alice@example.com  ALICE@EXAMPLE.COM  ok
bob@example.com    BOB@EXAMPLE.COM    ok

2 rows output · state: <tmp>/stream-demo/.local/foundry-cmgo/previews/<run-id> · log: <tmp>/stream-demo/.local/foundry-cmgo/previews/<run-id>/run.log
next: foundry-cmgo build

$ foundry-cmgo build   # stream starter
Foundry CMGO Build ✓

Transform       Docker container -> foundry --input-alias input --output-alias output --output-mode stream
Input           input                data/input.csv   2 rows
Output          output               stream @ master  2 rows
Runtime         container (default)  <duration>
Docker network  host
Output records  <tmp>/stream-demo/.local/foundry-cmgo/builds/<run-id>/streams/ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222/master/records.jsonl

note: Docker build is the default parity check; use --local-process for faster host-process debugging.

email              value              status
alice@example.com  ALICE@EXAMPLE.COM  ok
bob@example.com    BOB@EXAMPLE.COM    ok

2 rows output · state: <tmp>/stream-demo/.local/foundry-cmgo/builds/<run-id> · log: <tmp>/stream-demo/.local/foundry-cmgo/builds/<run-id>/run.log
next: foundry-cmgo inspect last

$ foundry-cmgo inspect outputs   # stream starter
Foundry CMGO Outputs ✓

Last run        build   <timestamp>
Output          output  stream @ master
Records         2
Artifact        <tmp>/stream-demo/.local/foundry-cmgo/builds/<run-id>/streams/ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222/master/records.jsonl
Docker network  host
State           <tmp>/stream-demo/.local/foundry-cmgo/builds/<run-id>
Log             <tmp>/stream-demo/.local/foundry-cmgo/builds/<run-id>/run.log
next: inspect the artifact path above or rerun foundry-cmgo build
```
