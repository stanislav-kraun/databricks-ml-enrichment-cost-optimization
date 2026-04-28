# Optimized Pipeline Module

This package contains the optimized enrichment flow used in this business case.

## What is implemented

- `pipeline.py`: orchestration only (load -> lookup build -> enrich -> write -> guard).
  - production path in this case uses full lookup rebuild each run.
  - this public repo keeps direct dual-write publish for readability.
  - original production run used staged publish boundary to reduce partial-update windows.
- `transform.py`: core Spark logic:
  - pre-aggregate historical events to campaign-grain lookup;
  - join feature batch with compact lookup on `campaign_id`.
- `guardrails.py`: post-run lookup-size check against configured broadcast threshold.
- `config.py`: environment-aware config load/merge + required-key validation.
- `schemas.py`: explicit input schemas for batch and historical sources.
- `runtime.py`: Spark session and logger bootstrap.

## Why this is intentionally compact

Code is limited to the structural optimization path and minimal guardrails so interview discussion stays focused on:
- root cause (join path),
- design change (pre-aggregation + compact join side),
- measurable impact.

## Not implemented in code (by design)

- staged/atomic multi-dataset publish boundary implementation (used in production, omitted here);
- run-level replay/lock protocol for backfills;
- CDF/streaming-based incremental lookup maintenance;
- enterprise alert routing and on-call ownership wiring.

## Next step for production hardening

If runtime degrades in future, add incremental lookup maintenance as a targeted extension.
