import argparse

import yaml
from pyspark.sql import SparkSession

from ml_feature_enrichment.schemas import FEATURE_BATCH_SCHEMA, HISTORICAL_EVENTS_SCHEMA
from ml_feature_enrichment.transform import build_historical_lookup, enrich_features
from ml_feature_enrichment.utils import get_logger, get_spark_session

logger = get_logger(__name__)


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


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

    historical_lookup = build_historical_lookup(historical_events)
    enriched = enrich_features(feature_batch, historical_lookup)

    (
        enriched.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "false")
        .save(config["output_path"])
    )

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
