"""StructTypes for Delta loads in the enrichment pipeline.

This pipeline only reads the fields declared below. Additional upstream columns are tolerated and ignored by this
job (they remain in upstream storage). Breaking upstream changes to required fields (rename/removal/type drift) must
be handled by updating this contract and shipping a new pipeline version.

`nullable=False` documents business intent; Spark does not strictly enforce nullability on read.
"""

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
