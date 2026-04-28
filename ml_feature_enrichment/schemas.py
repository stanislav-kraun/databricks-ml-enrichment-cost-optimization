from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

FEATURE_BATCH_SCHEMA = StructType([
    StructField("campaign_id", StringType(), nullable=False),
    StructField("batch_ts", TimestampType(), nullable=False),
    StructField("feature_a", DoubleType(), nullable=True),
    StructField("feature_b", DoubleType(), nullable=True),
    StructField("feature_c", LongType(), nullable=True),
    StructField("segment", StringType(), nullable=True),
])

HISTORICAL_EVENTS_SCHEMA = StructType([
    StructField("campaign_id", StringType(), nullable=False),
    StructField("session_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_ts", TimestampType(), nullable=False),
    StructField("event_count", LongType(), nullable=False),
])
