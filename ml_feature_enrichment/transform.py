from pyspark.sql import DataFrame

from ml_feature_enrichment.utils import get_logger

logger = get_logger(__name__)


def enrich_features_baseline_raw_join(
    feature_batch: DataFrame,
    historical_events: DataFrame,
) -> DataFrame:
    """Pre-optimization path: inner join to raw event grain (shuffle-heavy under skew)."""
    logger.info("Baseline raw join on campaign_id (event-level RHS)")
    return feature_batch.join(historical_events, on="campaign_id", how="inner")


def enrich_features(feature_batch: DataFrame, historical_events: DataFrame) -> DataFrame:
    """Baseline pipeline: raw join on campaign_id."""
    return enrich_features_baseline_raw_join(feature_batch, historical_events)
