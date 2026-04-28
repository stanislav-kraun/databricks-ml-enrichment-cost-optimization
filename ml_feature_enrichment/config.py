from typing import Any

import yaml


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError("Config file must contain a mapping at top level")
    return raw


def merge_pipeline_config(raw: dict[str, Any], env: str) -> dict[str, Any]:
    if "environments" not in raw:
        raise ValueError("pipeline config must define 'environments'")
    overlays: dict[str, Any] = raw["environments"]
    if env not in overlays:
        raise ValueError(f"Unknown environment {env!r}; expected one of: {sorted(overlays)!r}")
    overlay = overlays[env] or {}

    # Top-level keys represent default (prod-like) values.
    base = {k: v for k, v in raw.items() if k != "environments"}
    merged = dict(base)
    for key, value in overlay.items():
        # Merge nested spark overrides without losing default keys.
        if key == "spark" and isinstance(value, dict):
            merged["spark"] = {**(merged.get("spark") or {}), **value}
        else:
            merged[key] = value
    _validate_required_keys(merged)
    return merged


def _validate_required_keys(config: dict[str, Any]) -> None:
    required_top = ["feature_batch_path", "historical_events_path", "output_path", "lookup_grain", "spark"]
    missing_top = [k for k in required_top if k not in config]
    if missing_top:
        raise ValueError(f"Missing required config keys: {missing_top!r}")

    spark = config["spark"]
    if not isinstance(spark, dict):
        raise ValueError("config.spark must be a mapping")
    required_spark = [
        "app_name",
        "shuffle_partitions",
        "broadcast_lookup_threshold_bytes",
        "broadcast_guard_warning_ratio",
        "lookup_materialized_path",
    ]
    missing_spark = [k for k in required_spark if k not in spark]
    if missing_spark:
        raise ValueError(f"Missing required spark config keys: {missing_spark!r}")
