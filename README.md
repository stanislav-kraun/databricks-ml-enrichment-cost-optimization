# Business Case: Runtime and Compute Cost Reduction on Databricks

[![CI](https://github.com/stanislav-kraun/databricks-ml-feature-enrichment-optimization/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/stanislav-kraun/databricks-ml-feature-enrichment-optimization/actions/workflows/ci.yml)

Databricks pipeline that runs every 4 hours to enrich a refreshed ML feature batch against a historical events table.

This repository reproduces a real production fintech optimization case in a public-safe format. It shows how a slow enrichment pipeline is diagnosed and improved with measurable impact. Reproduction runs in local/CI (dev/test), while production execution runs in a separate managed Databricks environment under NDA.

## Scope: in-repo vs outside

| Topic | In this repo | In production (NDA analogue) |
|---|---|---|
| Pipeline transform logic | Implemented end-to-end | Same business logic in managed repository |
| Tests and CI | Compact, high-signal test set + CI | Full CI/CD with deploy and environment promotion |
| Upstream ingestion / lineage | Reads prepared Delta inputs | Source ingestion (CDC/streaming/DLT) handled upstream |
| Data quality ownership | Enforces transform-level contract and fail-fast checks | Source-level DQ contract enforced in ingestion layer |
| Publish boundary | Direct dual-write (public-case simplification) | Staged publish boundary to reduce partial-update windows |
| Observability | Public-safe guardrails and checks | Full metrics, alerting, and runbook ownership |
| IAM / secrets / network | Intentionally omitted | Managed via Databricks identity and network controls |
| Data / PII | Not included | Sanitized internal datasets only |

## Stack

- Databricks Runtime 14.3 LTS
- Delta Lake 3.1
- PySpark 3.5.0
- Python 3.11
- AWS EC2 `m5d.2xlarge` (8 vCPU, 32 GB RAM), job tier, on-demand, Photon enabled, autoscaling enabled

## Local development

**Prerequisites:** Python **3.11**

```bash
python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
python -m ruff check .
python -m pytest -q
```

## Roadmap (commit sequence)

1. **Bootstrap:** repo skeleton, dependency setup, CI, and engineering contract.
2. **Baseline pipeline:** initial working enrichment implementation and baseline tests.
3. **Baseline analysis:** profiling notes and evidence of bottlenecks before optimization.
4. **Optimization:** code changes to remove bottlenecks while preserving semantics.
5. **Final docs:** consolidated before/after outcomes, trade-offs, and operational notes.