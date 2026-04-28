import json
from pathlib import Path

import pytest

from ml_feature_enrichment.guardrails import check_lookup_size_postrun


def test_auto_broadcast_threshold_sync_contract() -> None:
    root = Path(__file__).resolve().parent.parent
    raw_yaml = (root / "conf" / "pipeline_config.yaml").read_text(encoding="utf-8")
    job_raw = (root / "conf" / "job_config.json").read_text(encoding="utf-8")

    threshold_line = [ln for ln in raw_yaml.splitlines() if "broadcast_lookup_threshold_bytes:" in ln][0]
    guard_bytes = int(threshold_line.split(":")[1].strip())
    job = json.loads(job_raw)
    cluster_bytes = int(job["job_clusters"][0]["new_cluster"]["spark_conf"]["spark.sql.autoBroadcastJoinThreshold"])
    assert cluster_bytes == guard_bytes


def test_guardrail_validates_input_ranges() -> None:
    with pytest.raises(ValueError, match="threshold_bytes must be positive"):
        check_lookup_size_postrun(None, "/tmp/lookup", threshold_bytes=0, warning_ratio=0.8)
    with pytest.raises(ValueError, match="warning_ratio"):
        check_lookup_size_postrun(None, "/tmp/lookup", threshold_bytes=100, warning_ratio=2.0)
