# Use Case: Milwaukee Catalog Resolution Across Non-eBay Retailers

Status: design, pre-implementation.

## Goal

For every Milwaukee-branded product listing on non-eBay tool retailers, resolve it to a canonical Milwaukee MPN (the manufacturer's own part number) and emit a structured match record.

Input scale: ~30K retailer listings to resolve against ~10K canonical Milwaukee SKUs.
Compute: deterministic regex + normalization + in-memory lookup. No LLM required.

## Why this use case

- **Real enterprise pattern at a demo-friendly scale.** Brand-catalog → retailer-listing resolution is the canonical entity-resolution problem that powers Google Shopping, Amazon ASIN matching, and every MAP / competitive-intelligence platform. At production scale it runs over billions of pairs/day. The slice below is structurally identical, just small enough to verify end-to-end.
- **Grounded in real data we already have.** No synthesis, no HTTP scraping, no external dependencies — the parquets in `silkie-tools/get-products-monorepo-go` already cover both sides.
- **Exercises pipeline-mode compute correctly.** Per-row work joined against a small read-only reference dataset is the single most common real-world pipeline shape. The existing email-enricher demo is stateless per-row; this use case forces the kit to grow a legitimate "reference preload" primitive.
- **Deterministic first, LLM optional later.** MVP is pure regex + hash lookup — cheap, fast, testable, offline. An LLM-as-judge layer can extend the unmatched tail later without re-architecting.

## Inputs

### Canonical catalog (reference, loaded once at pipeline start)

- **File**: `brands/milwaukee/product-details.parquet`
- **Rows**: 10,316
- **Key fields**:
  - `mpn` — Milwaukee manufacturer part number, populated on 100% of rows. Format is `48-22-9316`, `2732-20`, `48-11-1812`, `0880-20` — two or more hyphen-joined digit groups.
  - `upc` — UPC-A / EAN-13 barcode. Mostly 13-digit zero-padded (e.g. `0045242519224`). Some rows have empty string.
  - `title`, `brand`, `description`, `features[]`, `specs[]`, `product_hierarchy[]`, `image_urls[]` — for eyeball review and future extensions.

### Retailer listings (per-row input stream)

| Retailer | File | Total rows | Milwaukee rows | Brand filter | Primary signal | Secondary |
|---|---|---:|---:|---|---|---|
| Ohio Power Tool | `retailers/ohiopowertool/ohiopowertool_products.parquet` | 22,156 | ~5,142* | `upper(brand) = 'MILWAUKEE'` | `product_sku` (*see caveat*) | regex over `path` |
| ToolNut | `retailers/toolnut/product_urls.parquet` | 33,569 | 6,595 | `lower(brand) = 'milwaukee'` | `model_number` | regex over `path` |
| RedToolStore | `retailers/redtoolstore/product_details.parquet` | 7,214 | 7,165 | `upper(vendor) = 'MILWAUKEE'` | `barcode` (UPC-A) | regex over `title` |
| Home Depot | `retailers/homedepot/homedepot_tools.parquet` | 219,637 | ~11,165 | `lower(url) LIKE '%milwaukee%'` | regex over URL path | — |
| Clark's Tool | `retailers/clarkstool/products.parquet` | 2,806 | ~669 | `lower(brand_hint) = 'milwaukee'` | regex over `product_slug` | — |

*Ohio Power Tool caveat: the initial Milwaukee-filtered sample showed `product_sku` empty for every row because those rows were category landing pages, not product pages. A secondary filter (likely `entity_type='product'` or a path-depth check) is needed before the 5,142 number can be trusted.

**Total candidate listings to resolve: ~30K** across five retailers.

## Output schema

One output row per `(retailer, listing_url)` input, plus fan-out rows for bundle listings that resolve to multiple canonical MPNs.

| Column | Type | Notes |
|---|---|---|
| `retailer` | string | one of `ohiopowertool`, `toolnut`, `redtoolstore`, `homedepot`, `clarkstool` |
| `listing_url` | string | full retailer URL |
| `listing_title` | string? | where available (RedToolStore has it; Home Depot title is reconstructible from URL path) |
| `extracted_mpn_candidates` | list<string> | all MPN-like tokens found in the row's extractable fields, pre-normalization |
| `extracted_upc_candidate` | string? | normalized 12-digit UPC-A if retailer supplied a barcode |
| `matched_canonical_mpn` | string? | NULL if no match |
| `match_type` | enum | `upc_exact` \| `mpn_exact` \| `mpn_token` \| `no_match` |
| `match_confidence` | enum | `high` \| `medium` \| `low` |
| `canonical_title` | string? | from canonical, for eyeball review |
| `canonical_category` | string? | first element of canonical `product_hierarchy` |
| `multi_match_note` | string? | set when listing contains >1 matched MPN ("bundle/kit: N MPNs") |
| `run_timestamp` | string | ISO-8601; supports incremental re-runs |

Expected output volume: ~30K rows plus a few thousand bundle fan-outs (Home Depot 2-MPN / 3-MPN bundles are common).

## Matching logic

### Step 1 — Brand filter per retailer

See table above. One SQL/DataFrame predicate per retailer; applied upstream so the pipeline input is already Milwaukee-only.

### Step 2 — Extract MPN candidate tokens

Per-retailer extractors feed a shared normalizer.

| Retailer | Extractor |
|---|---|
| Ohio Power Tool | If `product_sku` non-empty, use it; else regex the path for Milwaukee MPN pattern |
| ToolNut | Use `model_number` directly, trim trailing non-MPN slug fragments (e.g. `0240-20-3` → `0240-20`) |
| RedToolStore | Regex over `title` (e.g. `Milwaukee 0820-20 M12 Cordless 400 CFM…` → `0820-20`); also emit `barcode` as UPC candidate |
| Home Depot | Regex over URL path; can yield 2+ MPNs for bundles (`2465-20-48-11-2450` → `[2465-20, 48-11-2450]`) |
| Clark's Tool | Strip known slug prefixes (`add-`, `copy-of-`, `promo-`), then regex over remainder |

**Milwaukee MPN regex (starting point):** `\b\d{2,4}-\d{2}(?:-\d{2,4})?\b`

Matches observed patterns: `0240-20`, `2867-22`, `48-22-1921`, `48-11-1812`, `48-22-9316`. Needs tuning against full dataset; expect a handful of false positives (generic model-like numbers in descriptions).

### Step 3 — Normalize

- **MPN**: uppercase; strip leading/trailing whitespace; collapse interior whitespace; preserve hyphens.
- **UPC**: strip leading zeros and re-pad to 12 digits (UPC-A). Milwaukee canonical's 13-digit codes and RedToolStore's 12-digit codes agree on the `045242…` manufacturer prefix, so normalization is a pure-string operation.

### Step 4 — Lookup against canonical indexes

Reference data loaded once at pipeline start (~10K rows, ~few MB in memory):

```
mpn_index : map[normalized_mpn] -> CanonicalRow
upc_index : map[normalized_upc] -> CanonicalRow
```

Per-row resolution order (first hit wins):

1. `extracted_upc_candidate` hits `upc_index` → `upc_exact` / `high`.
2. Any token in `extracted_mpn_candidates` hits `mpn_index` → `mpn_exact` / `high`. (If >1 distinct token matches, emit one output row per match and set `multi_match_note`.)
3. Reserved for future: partial-MPN token appears in canonical title but MPN isn't a direct key match → `mpn_token` / `medium`. Not in MVP.
4. No signal → `no_match` / `low`.

### Step 5 — Emit

One output row per resolution. Unmatched listings are emitted with `match_type=no_match` rather than dropped — the unmatched tail is itself useful output (candidates for manual review, discontinued-SKU detection, or future LLM-assisted resolution).

## How this maps onto the compute module

### Pipeline shape

1. **Preload** *(new kit primitive)*: read `milwaukee/product-details.parquet` once, build `mpn_index` + `upc_index`, pass as shared read-only context into the worker pool.
2. **Input adapter**: union of per-retailer Milwaukee-filtered listings, projected to a common row shape `(retailer, listing_url, listing_title?, extractable_fields…)`.
3. **Worker** (per-row, concurrent): extract → normalize → lookup → emit.
4. **Output adapter**: snapshot dataset (Foundry transactions) or CSV (local).

### What this intentionally stretches in the current kit

- **Reference-data preload.** The kit's current `Enricher` is stateless per row. Resolution needs a small reference dataset shared across workers — exactly the shape that `pkg/pipeline/core/contracts.go`'s currently-unused `Processor` / `InputAdapter` interfaces were presumably designed for. This use case gives those abstractions a reason to exist.
- **One-to-many row fan-out.** Bundle listings resolve to multiple canonical MPNs. The current worker assumes 1-to-1. MVP options: (a) concatenate matches into one row with a delimiter; (b) fan out to N output rows. Recommend (b) — closer to how downstream analytics would consume it, and forces the kit to handle fan-out cleanly once.

### What this deliberately does NOT need

- **Gemini / Google Search / URL Context.** Not called anywhere in the MVP resolution path. If we add the `mpn_token` fuzzy tier later, LLM-as-judge would live there — strictly opt-in for ambiguous cases.
- **Network I/O in the hot path.** All lookups are in-process. Expected throughput: tens of thousands of rows per second, single-node.
- **Secrets / redaction.** No sensitive input data. Redaction is a no-op here, which is useful — simplifies the MVP and lets us focus on the pipeline primitives.

## Running locally (planned)

The current local mode reads CSV. Parquet input is not yet supported in the kit. MVP approach: convert parquets to CSV once with a DuckDB one-liner, feed into the existing local runner.

```bash
# Produce the per-retailer inputs
duckdb -c "COPY (SELECT 'ohiopowertool' AS retailer, url, path AS listing_title, product_sku, path AS extract_source FROM '…/ohiopowertool_products.parquet' WHERE upper(brand)='MILWAUKEE' AND entity_type='product') TO 'milwaukee-ohiopowertool.csv' (HEADER)"
# …one per retailer, then cat them together with a consistent schema…

# Canonical reference
duckdb -c "COPY (SELECT mpn, upc, title, product_hierarchy[1] AS category FROM '…/milwaukee/product-details.parquet') TO 'milwaukee-canonical.csv' (HEADER)"

# Run the resolver
./dev run local -- \
  --input milwaukee-listings.csv \
  --reference milwaukee-canonical.csv \
  --output milwaukee-matches.csv
```

The `--reference` flag is new and will need to land in `cmd/enricher/main.go` alongside a new `Resolver` kit primitive. Parquet-native input is a separate follow-up.

## Test / validation plan

- **Sanity counts**: output rows = input rows + bundle fan-outs. Log per-retailer breakdown.
- **Per-retailer match rates** (target, not SLA): ToolNut and RedToolStore >90% `mpn_exact`+`upc_exact` because both have explicit model/barcode fields. Ohio Power Tool and Clark's Tool 60–80% depending on slug noise. Home Depot depends heavily on the URL regex — track as a separate number.
- **Canary SKUs**: pin a list of 5 popular Milwaukee MPNs that must resolve across every retailer that carries them (candidates: `2767-22`, `2853-22`, `48-22-1921`, `0880-20`, `2465-20`). Test fails if any canary drops out of `mpn_exact`/`upc_exact`.
- **Spot-check**: sample 20 matched and 20 unmatched rows per retailer each run; review by eye until confidence plateaus.
- **Golden file**: freeze a small subset of retailer inputs (`test/fixtures/milwaukee-resolution-input.csv`) and commit the expected output (`test/fixtures/milwaukee-resolution-expected.csv`). CI diffs on every run.
- **Backwards compatibility**: existing email-enricher demo continues to pass. The resolution pipeline is a new domain alongside the example, not a replacement.

## Known gaps / open questions

1. **Ohio Power Tool coverage.** The 5,142 figure includes category landing pages. Confirm the correct "product row" filter (probably `entity_type='product'`) before finalizing expected match counts.
2. **Bundle fan-out.** Confirm downstream wants one row per matched MPN (recommended) vs. one row per listing with a joined MPN list.
3. **UPC format edge cases.** Spot-check a few Milwaukee 13-digit UPCs vs. RedToolStore 12-digit barcodes to confirm the zero-strip normalization is lossless (no collisions).
4. **Slug artifact dedup.** Clark's Tool has `copy-of-` and `add-` prefixed duplicates of the same product. Emit all as separate matches, or dedup on `(retailer, matched_canonical_mpn)`? MVP: emit all, let downstream dedup.
5. **MPN regex false positives.** The `\d{2,4}-\d{2}(?:-\d{2,4})?\b` pattern will hit generic numbers in descriptions. Mitigation: only extract from identified-MPN-field columns (URL path, slug, model_number, title) — not from free-text description.
6. **Refresh cadence.** Parquets are a Jan 2026 snapshot. For a live pipeline, assume daily retailer refresh. Out of scope for MVP; flag for v2.
7. **Parquet input support.** The kit's local mode is CSV-only today. MVP uses a one-time CSV export; parquet-native input is a separate kit task.

## Not in MVP (explicit non-goals)

- eBay (22.5M URLs) — separate scale test after the resolver is proven on the 30K slice.
- Dewalt / Makita canonicals — cross-brand expansion follows after single-brand shakedown.
- Pricing / MAP compliance — requires retailer price scraping, out of scope here.
- Image similarity for unmatched rows.
- LLM-assisted fuzzy resolution for the `mpn_token` tier.
- Live refresh / streaming mode — MVP is snapshot-only.

## Decision record

- 2026-04-23 — scoped to non-eBay retailers, Milwaukee-only, deterministic (no LLM). Rationale: produce a shippable reference implementation against real data; eBay scale and LLM ambiguity resolution are follow-ups once the core pipeline is validated.
