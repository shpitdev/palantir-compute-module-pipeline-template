# palantir-compute-module-pipeline-search

Pipeline-mode Foundry Compute Module (Go) that:

1. Reads a dataset of email addresses
2. Enriches each email via Gemini (Google Search grounding + URL context + structured output)
3. Writes an output dataset

This is a one-shot batch container triggered by Foundry pipeline builds (not a long-lived service).

It should also be runnable locally (without Foundry) against a local input file for faster iteration and personal one-off batches.

## Docs

- `docs/DESIGN.md`: architecture, interfaces, local testing approach
- `docs/RELEASE.md`: Foundry/pipeline configuration and publishing steps (incl. egress policy)
