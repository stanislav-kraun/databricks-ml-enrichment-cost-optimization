from pyspark.sql import DataFrame, functions as F

from ml_feature_enrichment.utils import get_logger

logger = get_logger(__name__)


def build_historical_lookup(historical_events: DataFrame) -> DataFrame:
    logger.info("Building compact historical lookup by campaign_id")
    return (
        historical_events.filter(F.col("campaign_id").isNotNull())
        .groupBy("campaign_id")
        .agg(
            F.sum("event_count").alias("historical_event_count"),
            F.max("event_ts").alias("last_event_ts"),
        )
    )


def enrich_features(feature_batch: DataFrame, historical_lookup: DataFrame) -> DataFrame:
    logger.info("Join on campaign_id using compact lookup")
    return feature_batch.join(historical_lookup, on="campaign_id", how="inner")
