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


def test_run_reads_sources_and_writes_output(monkeypatch) -> None:
    spark = MagicMock()
    feature_batch = MagicMock()
    historical_events = MagicMock()
    spark.read.format.return_value.schema.return_value.load.side_effect = [
        feature_batch,
        historical_events,
    ]

    enriched = _Frame()
    monkeypatch.setattr(pipeline_mod, "enrich_features", lambda _a, _b: enriched)

    config = {
        "feature_batch_path": "/tmp/feature_batch",
        "historical_events_path": "/tmp/historical_events",
        "output_path": "/tmp/enriched",
    }

    pipeline_mod.run(spark, config)

    assert spark.read.format.return_value.schema.return_value.load.call_count == 2
    assert enriched.write.saved_paths == ["/tmp/enriched"]
