"""Tests for listings_type configuration field."""

import pytest
from src.config import WebSource


def test_listings_type_default_is_linked():
    """Test that listings_type defaults to 'linked'."""
    source = WebSource(url="https://example.com/news", categories=["test"])
    assert source.listings_type == "linked"


def test_listings_type_inline_from_dict():
    """Test loading inline listings_type from dict."""
    data = {
        "url": "https://example.com/news",
        "categories": ["test"],
        "listings_type": "inline"
    }
    source = WebSource.from_dict(data)
    assert source.listings_type == "inline"


def test_listings_type_linked_from_dict():
    """Test loading linked listings_type from dict."""
    data = {
        "url": "https://example.com/news",
        "categories": ["test"],
        "listings_type": "linked"
    }
    source = WebSource.from_dict(data)
    assert source.listings_type == "linked"


def test_listings_type_backward_compatibility():
    """Test that categories are required even for simple web sources."""
    data = {
        "url": "https://example.com/news",
        "categories": ["test"]
    }
    source = WebSource.from_dict(data)
    assert source.listings_type == "linked"


def test_listings_type_to_dict_default():
    """Test that default listings_type is not included in dict."""
    source = WebSource(url="https://example.com/news", categories=["test"], listings_type="linked")
    result = source.to_dict()
    assert "listings_type" not in result  # Default not serialized


def test_listings_type_to_dict_inline():
    """Test that non-default listings_type is included in dict."""
    source = WebSource(url="https://example.com/news", categories=["test"], listings_type="inline")
    result = source.to_dict()
    assert result["listings_type"] == "inline"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
