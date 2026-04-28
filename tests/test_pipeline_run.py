from unittest.mock import MagicMock

from ml_feature_enrichment import pipeline as pipeline_mod


class _Writer:
    def __init__(self) -> None:
        self.saved_paths: list[str] = []

    def format(self, _fmt: str):
        return self

    def mode(self, _mode: str):
        return self

    def option(self, _key: str, _value: str):
        return self

    def save(self, path: str) -> None:
        self.saved_paths.append(path)


class _Frame:
    def __init__(self) -> None:
        self.write = _Writer()
        self.persisted = False
        self.unpersisted = False

    def persist(self, _storage_level=None):
        self.persisted = True
        return self

    def unpersist(self):
        self.unpersisted = True
        return self


def test_run_reads_sources_and_writes_output(monkeypatch) -> None:
    spark = MagicMock()
    feature_batch = MagicMock()
    historical_events = MagicMock()
    spark.read.format.return_value.schema.return_value.load.side_effect = [
        feature_batch,
        historical_events,
    ]

    call_order: list[str] = []

    lookup = _Frame()
    enriched = _Frame()

    def _build_lookup(_events):
        call_order.append("build_lookup")
        return lookup

    def _enrich(_batch, _lookup):
        call_order.append("enrich")
        return enriched

    def _check_guard(*_args, **_kwargs):
        call_order.append("guard")
        return None

    monkeypatch.setattr(pipeline_mod, "build_historical_lookup", _build_lookup)
    monkeypatch.setattr(pipeline_mod, "enrich_features", _enrich)
    monkeypatch.setattr(pipeline_mod, "check_lookup_size_postrun", _check_guard)

    original_overwrite = pipeline_mod._overwrite_delta

    def _overwrite(df, path: str, *, overwrite_schema: bool):
        call_order.append(f"write:{path}")
        return original_overwrite(df, path, overwrite_schema=overwrite_schema)

    monkeypatch.setattr(pipeline_mod, "_overwrite_delta", _overwrite)

    config = {
        "lookup_grain": "campaign",
        "feature_batch_path": "/tmp/feature_batch",
        "historical_events_path": "/tmp/historical_events",
        "output_path": "/tmp/enriched",
        "spark": {
            "broadcast_lookup_threshold_bytes": 268435456,
            "broadcast_guard_warning_ratio": 0.8,
            "lookup_materialized_overwrite_schema": False,
            "lookup_materialized_path": "/tmp/lookup",
        },
    }

    pipeline_mod.run(spark, config)

    assert spark.read.format.return_value.schema.return_value.load.call_count == 2
    assert enriched.write.saved_paths == ["/tmp/enriched"]
    assert lookup.write.saved_paths == ["/tmp/lookup"]
    assert lookup.persisted is True
    assert lookup.unpersisted is True
    assert call_order == [
        "build_lookup",
        "enrich",
        "write:/tmp/enriched",
        "write:/tmp/lookup",
        "guard",
    ]
