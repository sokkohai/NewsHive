import os
from unittest.mock import MagicMock, patch

import pytest
from src.models import ContentItem
from src.pipeline import Pipeline
from src.config import Configuration, WebSource, Category, ArticlePreparationConfig


@pytest.fixture
def real_webhook_url():
    """Load actual webhook URL from WEBHOOK_URL env var or fall back to mock."""
    return os.getenv("WEBHOOK_URL", "http://mock-webhook.com")


@pytest.fixture
def mock_config(real_webhook_url):
    return Configuration(
        pipeline_version="1.0",
        web_sources=[WebSource(url="http://example.com", categories=["test_topic"])],
        email_folders=[],
        categories=[Category("test_topic", ["test"])],
        webhook_url=real_webhook_url,
        article_text_preparation=ArticlePreparationConfig(enabled=False),
    )


@pytest.fixture
def sample_items():
    items = [
        ContentItem(
            id="1",
            source_type="web",
            source_key="http://example.com/1",
            title="Test Title 1",
            summary="Test Summary 1",
            content="Test Content 1",
            categories=["test_topic"],
            published_at="unknown",
            discovered_at="2023-01-01T00:00:00Z",
            extracted_at="2023-01-01T00:01:00Z",
            source_url="http://example.com/1",
        ),
        ContentItem(
            id="2",
            source_type="web",
            source_key="http://example.com/2",
            title="Test Title 2",
            summary="Test Summary 2",
            content="Test Content 2",
            categories=["test_topic"],
            published_at="unknown",
            discovered_at="2023-01-01T00:00:00Z",
            extracted_at="2023-01-01T00:01:00Z",
            source_url="http://example.com/2",
        ),
    ]
    return items


def test_webhook_integration(mock_config, sample_items):
    """Test that pipeline sends data to webhook correctly."""

    # Mock pipeline components to bypass actual processing
    with patch("src.pipeline.StateStoreManager"), patch(
        "src.pipeline.Discoverer"
    ), patch("src.pipeline.Extractor"), patch(
        "src.pipeline.UnifiedEnricher"
    ) as mock_unified_enricher_class, patch(
        "requests.post"
    ) as mock_post:

        # Setup mocks
        mock_enricher_instance = MagicMock()
        mock_enricher_instance.process.return_value = (sample_items, [])
        mock_unified_enricher_class.return_value = mock_enricher_instance

        pipeline = Pipeline(config=mock_config)
        pipeline.unified_enricher = mock_enricher_instance

        # Mock the response object with status_code
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"
        mock_post.return_value = mock_response

        # Mock pipeline flow
        pipeline._stage_discovery = MagicMock(return_value=sample_items)
        pipeline._stage_deduplication = MagicMock(return_value=sample_items)
        pipeline._stage_extraction = MagicMock(return_value=(sample_items, [], []))
        pipeline._stage_categorization = MagicMock(return_value=sample_items)

        # Mock State Store behavior
        pipeline.state_store.has_processed.return_value = False

        # Run pipeline
        pipeline.run()

        # Wait a moment for async webhook to execute
        import time
        time.sleep(0.5)

        # Verify webhook was called
        assert mock_post.call_count >= 1
        
        # Verify payload structure
        args, kwargs = mock_post.call_args
        payload = kwargs.get("json")
        assert payload is not None
        assert isinstance(payload, list)
        assert len(payload) == len(sample_items)
        assert "practice_areas" in payload[0]
        assert "practice_area" not in payload[0]


def test_webhook_skipped_when_no_url(mock_config, sample_items):
    """Test that webhook is skipped when not configured."""
    mock_config.webhook_url = None
    
    with patch("src.pipeline.StateStoreManager"), \
         patch("src.pipeline.Discoverer"), \
         patch("src.pipeline.Extractor"), \
         patch("src.pipeline.UnifiedEnricher") as mock_unified_enricher, \
         patch("requests.post") as mock_post:
        
        mock_unified_enricher.return_value.process.return_value = (
            sample_items,
            [],
        )

        pipeline = Pipeline(config=mock_config)
        
        # Mock pipeline flow
        pipeline._stage_discovery = MagicMock(return_value=sample_items)
        pipeline._stage_deduplication = MagicMock(return_value=sample_items)
        pipeline._stage_extraction = MagicMock(return_value=(sample_items, [], []))  # Changed: 3 return values
