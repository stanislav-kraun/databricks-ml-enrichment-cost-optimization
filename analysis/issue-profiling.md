# Baseline profiling (issue)

About six months of the same 4h scheduled job in prod. 

I used Databricks Jobs -> Runs with a dated export on a cadence and computed mean and p95 on wall time from the full run list (hundreds of clocks), not from scrolling the grid. 

When durations looked wrong I opened Query Profile or Spark UI on the worst runs: more pulls early, then about one slow run per week, on the order of a few dozen profiles total. I copied read, shuffle, spill, and Photon from the profile summary and checked the DAG for SortMergeJoin on the fat join. That matched the clock trend in the export to the same shuffle-heavy plan across weeks. DBU only entered when billing gave usage by job id and day. 

The table is **~6 months of production rolled up** in two columns: **mean** and **p95** over that window. Job wall time uses every run in the Jobs export; bytes read, shuffle, spill, Photon, and the `ColumnarToRow` task-time share use the slow-run Query Profile sample. Values are rounded for readability.

| Metric | ~6 mo mean | ~6 mo p95 |
|--------|-----------:|----------:|
| Job wall time | ~2.5 h | ~3 h 45 m |
| Bytes read (job window) | ~800 GB | ~950 GB |
| Shuffle bytes (heavy join path, est.) | ~820 GB | ~900 GB |
| Disk spill | ~140 GB | ~218 GB |
| Task time in ColumnarToRow (% of aggregated task time) | ~81% | ~93% |
| Photon (% of task time) | ~20% | ~13% |

Conclusion: SortMergeJoin under skew, I/O-bound—not sizing. The fix was to pre-aggregate history to campaign grain so the RHS could broadcast and drop shuffle-merge on that join. Screenshot `baseline-query-profile.png`: prod “before” plan; names differ from this repo.
