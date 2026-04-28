from pyspark.sql import SparkSession

from ml_feature_enrichment.utils import get_logger

logger = get_logger(__name__)


def _read_delta_size_bytes(spark: SparkSession, lookup_delta_path: str) -> int:
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{lookup_delta_path}`")
    row = detail.select("sizeInBytes").collect()[0]
    return int(row["sizeInBytes"])


def check_lookup_size_postrun(
    spark: SparkSession,
    lookup_delta_path: str,
    threshold_bytes: int,
    warning_ratio: float,
) -> dict:
    if threshold_bytes <= 0:
        raise ValueError("threshold_bytes must be positive")
    if warning_ratio <= 0 or warning_ratio > 1:
        raise ValueError("warning_ratio must be in (0, 1]")

    exact_size_bytes = _read_delta_size_bytes(spark, lookup_delta_path)
    utilization = exact_size_bytes / float(threshold_bytes)
    result = {
        "lookup_delta_path": lookup_delta_path,
        "exact_size_bytes": exact_size_bytes,
        "threshold_bytes": threshold_bytes,
        "utilization": utilization,
        "estimate_source": "delta_describe_detail",
    }

    if utilization >= 1.0:
        logger.warning(
            "BROADCAST_GUARD_ALERT_POSTRUN: lookup exceeds threshold: exact_size_bytes=%s, threshold_bytes=%s",
            exact_size_bytes,
            threshold_bytes,
        )
    elif utilization >= warning_ratio:
        logger.warning(
            "Lookup size close to threshold: exact_size_bytes=%s, threshold_bytes=%s, utilization=%.2f",
            exact_size_bytes,
            threshold_bytes,
            utilization,
        )
    else:
        logger.info(
            "Lookup size check passed: exact_size_bytes=%s, threshold_bytes=%s",
            exact_size_bytes,
            threshold_bytes,
        )
    return result
