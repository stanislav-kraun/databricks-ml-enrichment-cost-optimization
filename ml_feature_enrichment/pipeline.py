import argparse

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

from ml_feature_enrichment.config import load_config, merge_pipeline_config
from ml_feature_enrichment.guardrails import check_lookup_size_postrun
from ml_feature_enrichment.runtime import get_logger, get_spark_session
from ml_feature_enrichment.schemas import FEATURE_BATCH_SCHEMA, HISTORICAL_EVENTS_SCHEMA
from ml_feature_enrichment.transform import (
    build_historical_lookup,
    enrich_features,
)

logger = get_logger(__name__)


def _overwrite_delta(df, path: str, *, overwrite_schema: bool) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true" if overwrite_schema else "false")
        .save(path)
    )


def run(spark: SparkSession, config: dict, *, run_env: str) -> None:
    logger.info(
        "Starting ml feature enrichment pipeline (env=%s, output_path=%s)",
        run_env,
        config.get("output_path"),
    )
    if config.get("lookup_grain") != "campaign":
        raise ValueError("Unsupported lookup_grain; expected 'campaign' for this pipeline")

    # Read batch and history with explicit schemas to keep input contract stable.
    feature_batch = (
        spark.read.format("delta")
        .schema(FEATURE_BATCH_SCHEMA)
        .load(config["feature_batch_path"])
    )
    historical_events = (
        spark.read.format("delta")
        .schema(HISTORICAL_EVENTS_SCHEMA)
        .load(config["historical_events_path"])
    )

    # Production trade-off in this case: full lookup rebuild each run for predictable semantics
    # and lower operational risk versus stateful incremental logic.
    historical_lookup = build_historical_lookup(historical_events).persist(StorageLevel.MEMORY_AND_DISK)
    lookup_path = config["spark"]["lookup_materialized_path"]
    lookup_overwrite_schema = config["spark"].get("lookup_materialized_overwrite_schema", False)
    if not isinstance(lookup_overwrite_schema, bool):
        raise TypeError("spark.lookup_materialized_overwrite_schema must be a bool")

    try:
        enriched = enrich_features(feature_batch, historical_lookup)
        # Public-case simplification: direct dual write keeps flow compact.
        # Original production pipeline used a staged publish boundary.
        _overwrite_delta(enriched, config["output_path"], overwrite_schema=False)
        _overwrite_delta(historical_lookup, lookup_path, overwrite_schema=lookup_overwrite_schema)
        # Post-run guard is visibility-first: warn when lookup grows toward threshold.
        check_lookup_size_postrun(
            spark,
            lookup_delta_path=lookup_path,
            threshold_bytes=config["spark"]["broadcast_lookup_threshold_bytes"],
            warning_ratio=config["spark"]["broadcast_guard_warning_ratio"],
        )
    finally:
        # Never leak persisted DataFrame on failures.
        historical_lookup.unpersist()

    logger.info("Pipeline complete")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--env", required=True, choices=["dev", "prod"])
    args = parser.parse_args()

    raw_config = load_config(args.config)
    config = merge_pipeline_config(raw_config, args.env)
    spark = get_spark_session(
        app_name=config["spark"]["app_name"],
        shuffle_partitions=config["spark"]["shuffle_partitions"],
    )
    run(spark, config, run_env=args.env)


if __name__ == "__main__":
    main()
