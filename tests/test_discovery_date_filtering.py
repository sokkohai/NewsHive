"""Tests for Discovery stage date filtering (3-day window).

Validates that Discovery filters articles older than 3 days.
"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from src.discovery import Discoverer
from src.config import Configuration, WebSource
from src.models import ContentItem


def create_mock_config():
    """Create mock configuration."""
    config = Mock(spec=Configuration)
    config.web_sources = [
        WebSource(
            url="https://example.com/news",
            listings_type="linked",
            categories=["Test"]
        )
    ]
    config.email_folders = []
    config.categories = []  # Add categories for Discoverer init
    config.firecrawl_enabled = False
    config.jina_enabled = False
    config.article_max_age_days = 3
    config.is_keyword_filtering_enabled = Mock(return_value=True)
    return config


def test_discovery_filters_old_articles():
    """Test that Discovery filters out articles older than 3 days."""
    config = create_mock_config()
    keywords = ["test", "news"]
    discoverer = Discoverer(config)
    
    # Mock web discoverer to return articles with different ages
    current_time = datetime.now(timezone.utc)
    fresh_date = (current_time - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    old_date = (current_time - timedelta(days=5)).isoformat().replace("+00:00", "Z")
    
    with patch.object(discoverer.web_discoverer, 'discover') as mock_discover:
        mock_discover.return_value = [
            ("https://example.com/article1", "Test Article 1", fresh_date),
            ("https://example.com/article2", "Test Article 2", old_date),
            ("https://example.com/article3", "Test Article 3", "unknown"),
        ]
        
        candidates = discoverer.discover()
        
        # Should only include fresh article and unknown date article
        assert len(candidates) == 2
        assert any(c.source_key == "web:https://example.com/article1" for c in candidates)
        assert any(c.source_key == "web:https://example.com/article3" for c in candidates)
        assert not any(c.source_key == "web:https://example.com/article2" for c in candidates)


def test_discovery_passes_unknown_dates():
    """Test that articles with unknown dates pass through Discovery."""
    config = create_mock_config()
    keywords = ["test"]
    discoverer = Discoverer(config)
    
    with patch.object(discoverer.web_discoverer, 'discover') as mock_discover:
        mock_discover.return_value = [
            ("https://example.com/article1", "Test Article", "unknown"),
        ]
        
        candidates = discoverer.discover()
        
        assert len(candidates) == 1
        assert candidates[0].published_at == "unknown"


def test_discovery_boundary_3_days():
    """Test boundary: 2.9 days is fresh, 3.1 days is filtered."""
    config = create_mock_config()
    keywords = ["test", "article"]  # Added "article" to match titles
    discoverer = Discoverer(config)
    
    current_time = datetime.now(timezone.utc)
    # 2.9 days = fresh
    fresh = (current_time - timedelta(days=2, hours=22)).isoformat().replace("+00:00", "Z")
    # 3.1 days = too old
    old = (current_time - timedelta(days=3, hours=3)).isoformat().replace("+00:00", "Z")
    
    with patch.object(discoverer.web_discoverer, 'discover') as mock_discover:
        mock_discover.return_value = [
            ("https://example.com/fresh", "Fresh Article", fresh),
            ("https://example.com/old", "Old Article", old),
        ]
        
        candidates = discoverer.discover()
        
        # Should only include fresh article (2.9 days < 3 days)
        assert len(candidates) == 1
        assert candidates[0].source_key == "web:https://example.com/fresh"


def test_discovery_handles_unparseable_dates():
    """Test that unparseable dates are treated as unknown and passed through."""
    config = create_mock_config()
    keywords = ["test"]
    discoverer = Discoverer(config)
    
    with patch.object(discoverer.web_discoverer, 'discover') as mock_discover:
        mock_discover.return_value = [
            ("https://example.com/article1", "Test Article", "invalid-date-format"),
        ]
        
        candidates = discoverer.discover()
        
        # Should pass through with unknown date
        assert len(candidates) == 1
        assert candidates[0].published_at == "unknown"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
