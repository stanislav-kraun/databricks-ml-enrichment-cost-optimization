# Outcomes

Local checks: root `README.md` (`pytest`, scope).

## Prod: before vs after

Full calendar month **before** vs **after** cutover. Same job (4h), **autoscale 8–12 both months**. Typical **avg workers** still dropped (~**11.5** -> ~**8**) because autoscale used less headroom on the lighter plan. **6–8** policy came **after** that window; `conf/job_config.json` is today’s cluster.

**Sources:** Jobs run export -> mean / P95 wall time (success only). Query Profile -> read / shuffle / spill / Photon (few runs per month, rounded). Billing -> DBU for the same job + months. **$** ~$0.35/DBU is illustrative.

| | Month before cutover | Month after cutover |
|--|-------:|------:|
| Mean wall time / run | ~2.3h–2.7h | ~13m–18m |
| P95 wall time / run | ~2.8h–3.2h | ~22m–29m |
| Bytes read (profile sample) | ~900–980 GB | ~380–450 GB |
| Shuffle on big join | ~800+ GB, dominant | small / not the story |
| Spill | ~100+ GB | ~0 typical |
| Photon (% task time) | ~15%–25% | ~95%–99% |
| Cluster policy (min–max) | 8–12 | 8–12 |
| Avg workers / run (typical) | ~11.5 | ~8 |
| DBU / month (rough) | ~21.4k | ~1.9k |

Post-cutover example plan: `optimized-query-profile.png` (broadcast join on `campaign_id`, small RHS; prod table names ≠ repo paths).

## Semantic equivalence

Same frozen inputs: baseline path vs optimized path. Same grain: one row per `campaign_id` from `feature_batch`. Compare `campaign_id`, `historical_event_count`, `last_event_ts`.

```sql
SELECT
  (SELECT COUNT(*) FROM baseline_output) AS baseline_cnt,
  (SELECT COUNT(*) FROM optimized_output) AS optimized_cnt;

SELECT COUNT(*) FROM (
  SELECT campaign_id FROM baseline_output
  EXCEPT SELECT campaign_id FROM optimized_output
);

SELECT
  SUM(CASE WHEN b.historical_event_count <> o.historical_event_count THEN 1 ELSE 0 END),
  SUM(CASE WHEN b.last_event_ts <> o.last_event_ts THEN 1 ELSE 0 END)
FROM baseline_output b
JOIN optimized_output o ON b.campaign_id = o.campaign_id;
```

Prod snapshot: row counts match, keys match, field mismatches 0.

Local: Spark semantics for lookup + inner join live in `tests/test_transform.py` (`pytest` after `pip install -e ".[dev]"`).

Contract in short: lookup = `sum(event_count)`, `max(event_ts)` per campaign, null keys dropped, join stays `inner` on `campaign_id`.
