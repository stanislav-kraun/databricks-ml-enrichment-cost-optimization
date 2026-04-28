# Tests

This folder contains a compact, high-signal test set for the public optimization case.
The goal is to validate core semantics and key failure behavior without pretending this is a full enterprise test matrix.

## Covered

- `test_transform.py`
  - validates lookup semantics on real Spark DataFrames:
  - null-key filtering, campaign-level aggregation, timestamp max behavior, and enrichment inner-join semantics.
- `test_pipeline_failure_recovery.py`
  - validates direct dual-write risk window with Delta-backed induced failure between writes;
  - asserts partial publish state explicitly (`output` updated, `lookup` stale).
- `test_config_and_guardrails.py`
  - validates threshold sync contract between pipeline config and Databricks job config;
  - validates guard input range checks.

## Out of scope

- Full integration testing against real Databricks workspace resources and IAM/network policies.
- End-to-end publish orchestration tests for staged/atomic multi-table boundaries.
- Long-run performance regression suites on production-like data volumes.
- Comprehensive upstream contract validation (schema evolution, source DQ, late-arrival replay behavior).
- Cost and runtime variance analysis across cluster shapes and autoscaling policies.

## Next step

For this public repo, the tests document engineering approach and critical trade-offs;
complete production validation requires real datasets and a managed Databricks environment.
