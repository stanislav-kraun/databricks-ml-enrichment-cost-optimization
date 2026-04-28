from unittest.mock import MagicMock

from ml_feature_enrichment.transform import enrich_features


def test_enrich_features_uses_inner_join_on_campaign_id() -> None:
    feature_batch = MagicMock()
    historical_events = MagicMock()
    joined_df = MagicMock()
    feature_batch.join.return_value = joined_df

    result = enrich_features(feature_batch, historical_events)

    feature_batch.join.assert_called_once_with(
        historical_events,
        on="campaign_id",
        how="inner",
    )
    assert result is joined_df
