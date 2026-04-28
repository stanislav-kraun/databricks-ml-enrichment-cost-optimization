import argparse

import yaml
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

from ml_feature_enrichment.guardrails import check_lookup_size_postrun
from ml_feature_enrichment.schemas import FEATURE_BATCH_SCHEMA, HISTORICAL_EVENTS_SCHEMA
from ml_feature_enrichment.transform import build_historical_lookup, enrich_features
from ml_feature_enrichment.utils import get_logger, get_spark_session

logger = get_logger(__name__)


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def _overwrite_delta(df, path: str, *, overwrite_schema: bool) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true" if overwrite_schema else "false")
        .save(path)
    )


def run(spark: SparkSession, config: dict) -> None:
    logger.info("Starting ml feature enrichment pipeline")

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

    historical_lookup = build_historical_lookup(historical_events).persist(StorageLevel.MEMORY_AND_DISK)
    lookup_path = config["spark"]["lookup_materialized_path"]
    lookup_overwrite_schema = config["spark"].get("lookup_materialized_overwrite_schema", False)
    if not isinstance(lookup_overwrite_schema, bool):
        raise TypeError("spark.lookup_materialized_overwrite_schema must be a bool")

    try:
        enriched = enrich_features(feature_batch, historical_lookup)
        _overwrite_delta(enriched, config["output_path"], overwrite_schema=False)
        _overwrite_delta(historical_lookup, lookup_path, overwrite_schema=lookup_overwrite_schema)
        check_lookup_size_postrun(
            spark,
            lookup_delta_path=lookup_path,
            threshold_bytes=config["spark"]["broadcast_lookup_threshold_bytes"],
            warning_ratio=config["spark"]["broadcast_guard_warning_ratio"],
        )
    finally:
        historical_lookup.unpersist()

    logger.info("Pipeline complete")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config = load_config(args.config)
    spark = get_spark_session(
        app_name=config["spark"]["app_name"],
        shuffle_partitions=config["spark"]["shuffle_partitions"],
    )
    run(spark, config)


if __name__ == "__main__":
    main()
