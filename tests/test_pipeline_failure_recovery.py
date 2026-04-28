import pytest
from datetime import datetime

from ml_feature_enrichment import pipeline as pipeline_mod


def test_direct_publish_partial_failure_exposes_split_brain_window(spark, tmp_path, monkeypatch) -> None:
    probe_path = str(tmp_path / "delta_probe")
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe_path)
    except Exception as exc:
        pytest.skip(f"Delta is not available in local Spark test runtime: {exc}")

    feature_path = str(tmp_path / "feature_batch")
    history_path = str(tmp_path / "historical_events")
    output_path = str(tmp_path / "output")
    lookup_path = str(tmp_path / "lookup")

    spark.createDataFrame(
        [
            ("A", datetime(2026, 1, 1, 0, 0, 0), 1.0, 2.0, 3, "s1"),
            ("B", datetime(2026, 1, 1, 0, 0, 0), 2.0, 3.0, 4, "s2"),
        ],
        "campaign_id string, batch_ts timestamp, feature_a double, feature_b double, feature_c long, segment string",
    ).write.format("delta").mode("overwrite").save(feature_path)
    spark.createDataFrame(
        [
            ("A", "sess1", "click", datetime(2026, 1, 1, 0, 0, 0), 5),
            ("B", "sess2", "click", datetime(2026, 1, 1, 0, 0, 0), 7),
        ],
        "campaign_id string, session_id string, event_type string, event_ts timestamp, event_count long",
    ).write.format("delta").mode("overwrite").save(history_path)
    spark.createDataFrame(
        [("A", 1, datetime(2025, 12, 31, 0, 0, 0))],
        "campaign_id string, historical_event_count long, last_event_ts timestamp",
    ).write.format("delta").mode("overwrite").save(lookup_path)

    cfg = {
        "lookup_grain": "campaign",
        "feature_batch_path": feature_path,
        "historical_events_path": history_path,
        "output_path": output_path,
        "spark": {
            "broadcast_lookup_threshold_bytes": 268435456,
            "broadcast_guard_warning_ratio": 0.8,
            "lookup_materialized_overwrite_schema": False,
            "lookup_materialized_path": lookup_path,
        },
    }

    original_overwrite = pipeline_mod._overwrite_delta
    state = {"calls": 0}

    def _overwrite_with_failure(df, path: str, *, overwrite_schema: bool) -> None:
        state["calls"] += 1
        if state["calls"] == 2:
            raise RuntimeError("simulated lookup write failure")
        original_overwrite(df, path, overwrite_schema=overwrite_schema)

    monkeypatch.setattr(pipeline_mod, "_overwrite_delta", _overwrite_with_failure)
    monkeypatch.setattr(pipeline_mod, "check_lookup_size_postrun", lambda *_args, **_kwargs: None)

    with pytest.raises(RuntimeError, match="simulated lookup write failure"):
        pipeline_mod.run(spark, cfg, run_env="dev")

    output_rows = spark.read.format("delta").load(output_path).count()
    lookup_rows = spark.read.format("delta").load(lookup_path).count()
    assert output_rows == 2
    assert lookup_rows == 1
