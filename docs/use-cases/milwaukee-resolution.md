# Use Case: Milwaukee Catalog Resolution Across Non-eBay Retailers

Status: dogfooded reference case; business accuracy review pending.

## Goal

Resolve Milwaukee-branded product listings from non-eBay tool retailers to canonical Milwaukee manufacturer part numbers (MPNs), then emit one structured match row per retailer listing.

This is a high-volume, local-first entity-resolution shape:

- canonical catalog: ~10K Milwaukee products,
- retailer listings: ~30K Milwaukee candidate rows across five retailers,
- compute: deterministic regex, UPC normalization, and in-memory lookup,
- no LLM calls, search calls, or network calls in the matching path.

## Why this belongs in this repo

The included email-enricher example proves per-row external API enrichment. Milwaukee resolution proves a different, common compute-module shape: preload a small reference dataset, process many input rows locally, and emit auditable evidence for every match/no-match decision.

It is useful dogfood because it exercises:

- generated project scaffolding,
- local preview,
- default Docker build parity,
- inspect commands,
- real data volume beyond toy examples,
- deterministic outputs that can be reviewed by a human.

## Source datasets

The source parquets live outside this repo in `silkie-tools/get-products-monorepo-go`; the matching logic treats those files as black-box data inputs.

| Role | Dataset | Notes |
|---|---|---|
| Canonical catalog | `brands/milwaukee/product-details.parquet` | canonical MPN, UPC, title, hierarchy, descriptions |
| Retailer | `retailers/toolnut/product_urls.parquet` | strong `model_number` and slug signal |
| Retailer | `retailers/ohiopowertool/ohiopowertool_products.parquet` | product SKU when populated; path fallback |
| Retailer | `retailers/redtoolstore/product_details.parquet` | title/handle MPNs plus UPC barcode signal |
| Retailer | `retailers/homedepot/homedepot_tools.parquet` | MPNs embedded in URL/category paths |
| Retailer | `retailers/clarkstool/products.parquet` | slug-derived MPN signal |

## Dogfood result snapshot

A fresh generated project was built outside this repo with `foundry-cmgo-dev new`, then implemented through a Claude/acpx dogfood loop using only the CLI workflow for scaffold, preview, build, and inspect.

The resulting module emitted 30,777 rows:

| Metric | Count | Rate |
|---|---:|---:|
| Exact matches | 25,183 | 81.8% |
| Probable matches | 550 | 1.8% |
| No match | 5,044 | 16.4% |
| Total matched | 25,733 | 83.6% |

By retailer:

| Retailer | Rows | Match rate | Distinct matched MPNs |
|---|---:|---:|---:|
| RedToolStore | 7,165 | 92.6% | 4,689 |
| ToolNut | 6,636 | 88.4% | 5,421 |
| Clark's Tool | 669 | 80.4% | 526 |
| Ohio Power Tool | 5,142 | 78.4% | 3,677 |
| Home Depot | 11,165 | 77.6% | 3,781 |

CLI validation completed in the generated project:

```bash
go test ./...
foundry-cmgo-dev preview
foundry-cmgo-dev build
foundry-cmgo-dev inspect last
foundry-cmgo-dev inspect config
foundry-cmgo-dev inspect outputs
```

The default Docker build path produced the same 30,777-row output as preview.

## Output shape

One row is emitted per retailer listing:

| Column | Meaning |
|---|---|
| `canonical_mpn` | matched Milwaukee MPN; empty for no-match |
| `canonical_title` | canonical product title; empty for no-match |
| `retailer` | retailer identifier |
| `retailer_url` | source listing URL |
| `retailer_listing_key` | slug, SKU, product ID, or other retailer key |
| `match_status` | `exact`, `probable`, or `no_match` |
| `match_method` | extraction/match strategy used |
| `match_confidence` | `high`, `medium`, or `low` |
| `evidence_json` | extracted candidates and chosen match evidence |

## Matching strategy

1. Load the canonical catalog once and build normalized MPN and UPC indexes.
2. Filter each retailer to Milwaukee candidate rows.
3. Extract candidate MPNs using retailer-specific fields: model numbers, SKUs, titles, handles, URL paths, and category paths.
4. Normalize MPNs and UPCs.
5. Resolve in this order:
   - exact MPN match,
   - suffix-stripped MPN match (probable),
   - UPC barcode match,
   - prefix MPN match (probable),
   - no match.
6. Preserve evidence for review rather than silently dropping unmatched rows.

## Accuracy risks to review before productizing

- Bundle listings can contain multiple valid MPNs; the current simple resolver chooses a match rather than modeling all bundle components.
- Some no-match rows include valid-looking MPNs that are absent from the canonical catalog snapshot.
- Prefix/suffix probable matches are useful but require manual spot-checking before treating them as exact truth.
- Retailer category pages and product pages are mixed in some sources, especially Ohio Power Tool.
- The January 2026 parquet snapshot may lag live retailer/catalog state.

## Repo decision

Keep this as a documented dogfood/use-case artifact for now, not as a committed example module. The generated implementation and final review output remain outside this repo until the business matching accuracy is reviewed.
