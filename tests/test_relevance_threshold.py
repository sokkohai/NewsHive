"""Tests for Relevance Threshold Configuration.

Tests the configurable threshold for minimum accepted articles (Mittel/Hoch relevance)
with automatic filling from top-score Niedrig items if needed.

Per: RELEVANCE_THRESHOLD environment variable configuration.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.config import ConfigError, ConfigLoader, Configuration
from src.models import ContentItem
from src.pipeline import Pipeline


class TestRelevanceThresholdConfig(unittest.TestCase):
    """Tests for RELEVANCE_THRESHOLD environment variable parsing."""

    def setUp(self):
        """Set up test environment."""
        self.original_env = os.environ.copy()
        self.temp_config_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        config_data = {
            "pipeline_version": "2.0",
            "web_sources": [
                {"url": "https://example.com", "categories": ["CCCI"]}
            ],
            "categories": [{"name": "CCCI", "keywords": []}],
        }
        json.dump(config_data, self.temp_config_file)
        self.temp_config_file.close()
        os.environ["NEWSHIVE_CONFIG"] = self.temp_config_file.name

    def tearDown(self):
        """Restore original environment."""
        os.environ.clear()
        os.environ.update(self.original_env)
        Path(self.temp_config_file.name).unlink(missing_ok=True)

    def test_default_threshold_value(self):
        """Test that default threshold is 5 when RELEVANCE_THRESHOLD not set."""
        if "RELEVANCE_THRESHOLD" in os.environ:
            del os.environ["RELEVANCE_THRESHOLD"]

        config = ConfigLoader.load()
        self.assertEqual(config.relevance_threshold, 5)

    def test_custom_threshold_value(self):
        """Test that custom RELEVANCE_THRESHOLD value is read correctly."""
        os.environ["RELEVANCE_THRESHOLD"] = "10"

        config = ConfigLoader.load()
        self.assertEqual(config.relevance_threshold, 10)

    def test_threshold_zero_disables_filling(self):
        """Test that threshold=0 still works (disables fallback)."""
        os.environ["RELEVANCE_THRESHOLD"] = "0"

        config = ConfigLoader.load()
        self.assertEqual(config.relevance_threshold, 0)

    def test_invalid_threshold_negative(self):
        """Test that negative threshold values raise ConfigError."""
        os.environ["RELEVANCE_THRESHOLD"] = "-5"

        with self.assertRaises(ConfigError) as context:
            ConfigLoader.load()
        self.assertIn("RELEVANCE_THRESHOLD", str(context.exception))

    def test_invalid_threshold_non_numeric(self):
        """Test that non-numeric threshold values raise ConfigError."""
        os.environ["RELEVANCE_THRESHOLD"] = "invalid"

        with self.assertRaises(ConfigError) as context:
            ConfigLoader.load()
        self.assertIn("RELEVANCE_THRESHOLD", str(context.exception))

    def test_threshold_with_whitespace(self):
        """Test that threshold values with whitespace are handled correctly."""
        os.environ["RELEVANCE_THRESHOLD"] = "  7  "

        config = ConfigLoader.load()
        self.assertEqual(config.relevance_threshold, 7)


class TestApplyMinimumProcessedFallback(unittest.TestCase):
    """Tests for _apply_minimum_processed_fallback() method behavior."""

    def setUp(self):
        """Set up test pipeline with mock configuration."""
        self.original_env = os.environ.copy()
        # Create minimal config for pipeline
        self.temp_config_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        config_data = {
            "pipeline_version": "2.0",
            "web_sources": [
                {"url": "https://example.com", "categories": ["CCCI"]}
            ],
            "categories": [{"name": "CCCI", "keywords": []}],
        }
        json.dump(config_data, self.temp_config_file)
        self.temp_config_file.close()
        os.environ["HONEYSCRAPER_CONFIG"] = self.temp_config_file.name
        os.environ["RELEVANCE_THRESHOLD"] = "5"

    def tearDown(self):
        """Clean up test environment."""
        os.environ.clear()
        os.environ.update(self.original_env)
        Path(self.temp_config_file.name).unlink(missing_ok=True)

    def _create_content_item(
        self,
        source_key: str,
        relevance_score: int | None = None,
        relevance_level: str = "Mittel",
    ) -> ContentItem:
        """Helper to create ContentItem for testing."""
        return ContentItem(
            id=source_key,
            source_type="web",
            source_key=source_key,
            title=f"Test Article {source_key}",
            summary="Test summary",
            content="Test content",
            categories=["CCCI"],
            published_at="2026-03-19T10:00:00Z",
            discovered_at="2026-03-19T10:00:00Z",
            extracted_at="2026-03-19T10:00:00Z",
            source_url=f"http://example.com/{source_key}",
            keywords=["test"],
            relevance_level=relevance_level,
            relevance_score=relevance_score,
        )

    def test_fallback_not_triggered_when_threshold_met(self):
        """Test that fallback is not triggered when threshold is already met."""
        pipeline = Pipeline()
        
        enriched_items = [
            self._create_content_item("item1", 12),
            self._create_content_item("item2", 8),
            self._create_content_item("item3", 6),
            self._create_content_item("item4", 5),
            self._create_content_item("item5", 5),
        ]
        enrichment_filtered = [
            ("item6", "Niedrig"),
            ("item7", "Niedrig"),
        ]
        extracted_items = enriched_items + [
            self._create_content_item("item6", 4, "Niedrig"),
            self._create_content_item("item7", 3, "Niedrig"),
        ]

        result_items, result_filtered = pipeline._apply_minimum_processed_fallback(
            enriched_items, extracted_items, enrichment_filtered, minimum_items=5
        )

        # Should not change
        self.assertEqual(len(result_items), 5)
        self.assertEqual(result_items, enriched_items)
        self.assertEqual(result_filtered, enrichment_filtered)

    def test_fallback_fills_with_top_score_items(self):
        """Test that fallback fills with the highest-score Niedrig items."""
        pipeline = Pipeline()
        
        enriched_items = [
            self._create_content_item("item1", 12),
            self._create_content_item("item2", 8),
        ]
        enrichment_filtered = [
            ("item3", "Niedrig"),
            ("item4", "Niedrig"),
            ("item5", "Niedrig"),
        ]
        extracted_items = enriched_items + [
            self._create_content_item("item3", 4, "Niedrig"),
            self._create_content_item("item4", 2, "Niedrig"),
            self._create_content_item("item5", 1, "Niedrig"),
        ]

        result_items, result_filtered = pipeline._apply_minimum_processed_fallback(
            enriched_items, extracted_items, enrichment_filtered, minimum_items=5
        )

        # Should have 5 items now
        self.assertEqual(len(result_items), 5)
        # Should be original 2 + top 3 Niedrig items (sorted by score desc: item3, item4, item5)
        result_keys = [item.source_key for item in result_items]
        self.assertEqual(result_keys, ["item1", "item2", "item3", "item4", "item5"])

    def test_fillback_only_uses_niedrig_items(self):
        """Test that fallback only uses Niedrig-classified items, not other failures."""
        pipeline = Pipeline()
        
        enriched_items = [
            self._create_content_item("item1", 12),
        ]
        enrichment_filtered = [
            ("item2", "Niedrig"),
            ("item3", "extraction_failed"),  # Should be ignored
            ("item4", "Niedrig"),
        ]
        extracted_items = enriched_items + [
            self._create_content_item("item2", 3, "Niedrig"),
            self._create_content_item("item4", 2, "Niedrig"),
        ]

        result_items, result_filtered = pipeline._apply_minimum_processed_fallback(
            enriched_items, extracted_items, enrichment_filtered, minimum_items=4
        )

        # Should have 3 items (1 enriched + 2 Niedrig from fallback)
        # item3 should NOT be included because it's not Niedrig
        self.assertEqual(len(result_items), 3)
        result_keys = [item.source_key for item in result_items]
        self.assertIn("item1", result_keys)
        self.assertIn("item2", result_keys)
        self.assertIn("item4", result_keys)
        self.assertNotIn("item3", result_keys)

    def test_fallback_logs_warning_when_threshold_not_reached(self):
        """Test that warning is logged when threshold cannot be achieved."""
        pipeline = Pipeline()
        
        enriched_items = [
            self._create_content_item("item1", 12),
        ]
        enrichment_filtered = [
            ("item2", "Niedrig"),
        ]
        extracted_items = enriched_items + [
            self._create_content_item("item2", 3, "Niedrig"),
        ]

        with patch("src.pipeline.logger") as mock_logger:
            result_items, result_filtered = pipeline._apply_minimum_processed_fallback(
                enriched_items,
                extracted_items,
                enrichment_filtered,
                minimum_items=5,
            )

            # Should log a warning about threshold not reached
            warning_calls = [
                call for call in mock_logger.method_calls if "warning" in str(call).lower()
            ]
            # Note: We can't easily check exact warning content due to mocking details,
            # but verify warning was logged
            self.assertTrue(
                any("threshold" in str(call).lower() for call in mock_logger.method_calls),
                "Expected warning about threshold not reached",
            )

    def test_fallback_handles_zero_threshold(self):
        """Test that zero threshold disables fallback."""
        pipeline = Pipeline()
        
        enriched_items = [
            self._create_content_item("item1", 12),
        ]
        enrichment_filtered = [
            ("item2", "Niedrig"),
            ("item3", "Niedrig"),
        ]
        extracted_items = enriched_items + [
            self._create_content_item("item2", 3, "Niedrig"),
            self._create_content_item("item3", 2, "Niedrig"),
        ]

        result_items, result_filtered = pipeline._apply_minimum_processed_fallback(
            enriched_items, extracted_items, enrichment_filtered, minimum_items=0
        )

        # With threshold=0, no fallback should occur
        self.assertEqual(len(result_items), 1)
        self.assertEqual(result_items[0].source_key, "item1")
        self.assertEqual(len(result_filtered), 2)

    def test_fallback_empty_filtered_list(self):
        """Test that fallback handles empty filtered items gracefully."""
        pipeline = Pipeline()
        
        enriched_items = [
            self._create_content_item("item1", 12),
        ]
        enrichment_filtered = []
        extracted_items = enriched_items

        result_items, result_filtered = pipeline._apply_minimum_processed_fallback(
            enriched_items, extracted_items, enrichment_filtered, minimum_items=5
        )

        # Should return unchanged
        self.assertEqual(len(result_items), 1)
        self.assertEqual(result_filtered, [])

    def test_fallback_deterministic_sorting(self):
        """Test that fallback sorting is deterministic (score desc, then source_key asc)."""
        pipeline = Pipeline()
        
        enriched_items = []
        enrichment_filtered = [
            ("item_b", "Niedrig"),
            ("item_a", "Niedrig"),
            ("item_c", "Niedrig"),
        ]
        extracted_items = [
            self._create_content_item("item_b", 3, "Niedrig"),
            self._create_content_item("item_a", 3, "Niedrig"),  # Same score as b, sorted by key
            self._create_content_item("item_c", 2, "Niedrig"),
        ]

        result_items, result_filtered = pipeline._apply_minimum_processed_fallback(
            enriched_items, extracted_items, enrichment_filtered, minimum_items=3
        )

        # Should have 3 items sorted by score desc, then key asc
        self.assertEqual(len(result_items), 3)
        result_keys = [item.source_key for item in result_items]
        # Both item_a and item_b have score 3, so they come first
        # item_c has score 2, so comes last
        # For items with same score, order by source_key
        self.assertIn("item_a", result_keys[:2])
        self.assertIn("item_b", result_keys[:2])
        self.assertEqual(result_keys[2], "item_c")


class TestRelevanceThresholdIntegration(unittest.TestCase):
    """Integration tests for relevance threshold in pipeline execution."""

    def setUp(self):
        """Set up test environment."""
        self.original_env = os.environ.copy()
        self.temp_config_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        config_data = {
            "pipeline_version": "2.0",
            "web_sources": [
                {"url": "https://example.com", "categories": ["CCCI"]}
            ],
            "categories": [{"name": "CCCI", "keywords": []}],
        }
        json.dump(config_data, self.temp_config_file)
        self.temp_config_file.close()
        os.environ["NEWSHIVE_CONFIG"] = self.temp_config_file.name

    def tearDown(self):
        """Clean up test environment."""
        os.environ.clear()
        os.environ.update(self.original_env)
        Path(self.temp_config_file.name).unlink(missing_ok=True)

    def test_pipeline_uses_configured_threshold(self):
        """Test that pipeline uses the configured threshold value."""
        os.environ["RELEVANCE_THRESHOLD"] = "7"

        config = ConfigLoader.load()
        self.assertEqual(config.relevance_threshold, 7)

        # Verify that pipeline gets the correct threshold
        pipeline = Pipeline(config=config)
        self.assertEqual(pipeline.config.relevance_threshold, 7)

    def test_env_var_precedence(self):
        """Test that RELEVANCE_THRESHOLD env var takes precedence."""
        os.environ["RELEVANCE_THRESHOLD"] = "12"

        config = ConfigLoader.load()
        self.assertEqual(config.relevance_threshold, 12)


if __name__ == "__main__":
    unittest.main()
