from ml_feature_enrichment.transform import build_historical_lookup, enrich_features


def test_build_historical_lookup_semantics_with_spark(spark) -> None:
    historical_events = spark.sql(
        """
        SELECT * FROM VALUES
            ('A', 2L, timestamp('2026-01-01 00:00:00')),
            ('A', 3L, timestamp('2026-01-02 00:00:00')),
            ('B', 5L, timestamp('2026-01-03 00:00:00')),
            (NULL, 9L, timestamp('2026-01-04 00:00:00'))
        AS t(campaign_id, event_count, event_ts)
        """
    )

    result = build_historical_lookup(historical_events)
    rows = {
        row["campaign_id"]: (row["historical_event_count"], str(row["last_event_ts"]))
        for row in result.collect()
    }

    assert rows == {
        "A": (5, "2026-01-02 00:00:00"),
        "B": (5, "2026-01-03 00:00:00"),
    }


def test_enrich_features_inner_join_semantics_with_spark(spark) -> None:
    feature_batch = spark.sql(
        """
        SELECT * FROM VALUES
            ('A', 10.0),
            ('B', 20.0),
            ('C', 30.0)
        AS t(campaign_id, feature_value)
        """
    )
    historical_lookup = spark.sql(
        """
        SELECT * FROM VALUES
            ('A', 5L),
            ('C', 7L)
        AS t(campaign_id, historical_event_count)
        """
    )

    result = enrich_features(feature_batch, historical_lookup)
    rows = {(row["campaign_id"], row["historical_event_count"]) for row in result.collect()}

    assert rows == {("A", 5), ("C", 7)}
