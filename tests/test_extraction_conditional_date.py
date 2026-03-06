"""Tests for conditional date filtering in Extraction stage.

Validates that Extraction only extracts dates when Discovery returned 'unknown'.
"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from src.extraction import Extractor
from src.models import ContentItem


def create_test_item(source_key: str, published_at: str) -> ContentItem:
    """Helper to create test ContentItem."""
    return ContentItem(
        id=source_key,
        source_type="web",
        source_key=source_key,
        title="Test Article",
        summary="",
        content="",
        categories=[],
        published_at=published_at,
        discovered_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        extracted_at="",
        source_url=source_key,
    )


def test_extraction_skips_date_check_when_known():
    """Test that Extraction skips date extraction if published_at is already set."""
    extractor = Extractor()
    
    current_time = datetime.now(timezone.utc)
    fresh_date = (current_time - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    
    item = create_test_item("web:https://example.com/article", fresh_date)
    
    with patch.object(extractor.web_extractor, 'extract') as mock_extract, \
         patch.object(extractor, '_get_html_for_date_extraction') as mock_get_html:
        
        mock_extract.return_value = ("Article test content", "local")
        
        result, status = extractor.extract(item)
        
        # Should NOT attempt to extract date
        mock_get_html.assert_not_called()
        assert result is not None
        assert result.published_at == fresh_date  # Unchanged


def test_extraction_extracts_date_when_unknown():
    """Test that Extraction extracts date only when published_at is 'unknown'."""
    extractor = Extractor()
    
    item = create_test_item("web:https://example.com/article", "unknown")
    
    current_time = datetime.now(timezone.utc)
    extracted_date = (current_time - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    
    with patch.object(extractor.web_extractor, 'extract') as mock_extract, \
         patch.object(extractor, '_get_html_for_date_extraction') as mock_get_html, \
         patch.object(extractor.web_extractor, 'extract_published_date') as mock_extract_date:
        
        mock_extract.return_value = ("Article test content", "local")
        mock_get_html.return_value = "<html>...</html>"
        mock_extract_date.return_value = extracted_date
        
        result, status = extractor.extract(item)
        
        # Should attempt to extract date
        mock_get_html.assert_called_once()
        mock_extract_date.assert_called_once()
        assert result is not None
        assert result.published_at == extracted_date  # Updated


def test_extraction_filters_old_article_after_date_extraction():
    """Test that old articles are filtered after date extraction (when unknown initially)."""
    extractor = Extractor()
    
    item = create_test_item("web:https://example.com/article", "unknown")
    
    current_time = datetime.now(timezone.utc)
    old_date = (current_time - timedelta(days=5)).isoformat().replace("+00:00", "Z")
    
    with patch.object(extractor.web_extractor, 'extract') as mock_extract, \
         patch.object(extractor, '_get_html_for_date_extraction') as mock_get_html, \
         patch.object(extractor.web_extractor, 'extract_published_date') as mock_extract_date:
        
        mock_extract.return_value = ("Article test content", "local")
        mock_get_html.return_value = "<html>...</html>"
        mock_extract_date.return_value = old_date
        
        result, status = extractor.extract(item)
        
        # Should be filtered as old (status now contains descriptive reason)
        assert result is None
        assert "too old" in status.lower()


def test_extraction_allows_unknown_date_after_extraction():
    """Test that articles with still-unknown dates after extraction are allowed through."""
    extractor = Extractor()
    
    item = create_test_item("web:https://example.com/article", "unknown")
    
    with patch.object(extractor.web_extractor, 'extract') as mock_extract, \
         patch.object(extractor, '_get_html_for_date_extraction') as mock_get_html, \
         patch.object(extractor.web_extractor, 'extract_published_date') as mock_extract_date:
        
        mock_extract.return_value = ("Article test content", "local")
        mock_get_html.return_value = "<html>...</html>"
        mock_extract_date.return_value = "unknown"  # Still unknown after extraction
        
        result, status = extractor.extract(item)
        
        # Should be allowed through (Option A)
        assert result is not None
        assert result.published_at == "unknown"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
