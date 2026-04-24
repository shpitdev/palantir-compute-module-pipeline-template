# Superseded plan: Foundry-like `foundry-cmgo preview` / `build` ergonomics

Date: 2026-04-24  
Status: superseded by the merged preview/build implementation and the roundout plan below.

The initial preview/build plan was implemented and merged in PR #63. The follow-up handoff was merged in PR #64.

Use this current implementation plan for the next work session:

- `docs/foundry-cmgo-cli-roundout-implementation-plan.md`

Summary of what is already included on `main`:

- `foundry-cmgo preview`
- `foundry-cmgo build` with Docker/container default and `--container=false` / `--local-process` escape hatches
- `foundry-cmgo inspect last`
- generated `foundry-cmgo.yaml`
- hardened generated Foundry Dockerfile
- generated starter docs for `new -> preview -> build -> inspect last`

Do not continue from the old temporary recommendations in this file. The current plan intentionally de-prioritizes standalone `doctor docker` work and instead recommends command-scoped preflight, behavior-preserving cleanup, generated-starter verification, expanded inspect, Docker networking evidence, and a final CLI aesthetic/screenshot pass.
