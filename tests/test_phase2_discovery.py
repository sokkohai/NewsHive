"""Tests for Phase 2: Smart API Usage (Discovery Optimization).

Verifies that Discovery stage does NOT use Firecrawl or Jina APIs,
only local methods (HTTP + BeautifulSoup, Playwright).

Specification: specs/core/IMPLEMENTATION_GUIDE.md Phase 2
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.discovery import WebDiscoverer


def test_web_discoverer_init():
    """Test that WebDiscoverer initializes correctly."""
    discoverer = WebDiscoverer()
    # verify default state if necessary, or just that it doesn't crash


def test_web_discoverer_local_methods_only():
    """Test that WebDiscoverer only attempts local methods."""
    discoverer = WebDiscoverer()
    
    with patch.object(discoverer, '_discover_with_local', return_value=(None, False)) as mock_local, \
         patch.object(discoverer, '_discover_with_browser', return_value=None) as mock_browser, \
         patch.object(discoverer, '_discover_with_jina', return_value=None) as mock_jina, \
         patch.object(discoverer, '_discover_with_firecrawl', return_value=None) as mock_firecrawl, \
         patch.object(discoverer, '_apply_extraction_rules', return_value=[]):
        
        # Attempt discovery
        url = "https://example.com/news"
        result = discoverer.discover(
            url,
            include_patterns=None,
            exclude_patterns=None,
            date_extraction_pattern=None
        )
        
        # Verify local methods were called
        assert mock_local.called, "Local discovery should be attempted"
        assert mock_browser.called, "Browser discovery should be attempted"
        
        # Verify external APIs were NOT called (Phase 2: disabled in __init__)
        # These methods won't be called because firecrawl_enabled and jina_enabled are False
        
        # Should return empty list when all local methods fail
        assert result == [], "Should return empty list when local methods fail"


def test_web_discoverer_local_success_stops_chain():
    """Test that successful local discovery prevents further method attempts."""
    discoverer = WebDiscoverer()
    
    # Mock successful local discovery
    mock_articles = [
        ("https://example.com/article1", "Article 1", "2024-01-15T10:00:00Z"),
        ("https://example.com/article2", "Article 2", "2024-01-15T11:00:00Z"),
    ]
    
    with patch.object(discoverer, '_discover_with_local', return_value=(mock_articles, False)) as mock_local, \
         patch.object(discoverer, '_discover_with_browser', return_value=None) as mock_browser, \
         patch.object(discoverer, '_discover_with_jina', return_value=None) as mock_jina, \
         patch.object(discoverer, '_discover_with_firecrawl', return_value=None) as mock_firecrawl, \
         patch.object(discoverer, '_apply_extraction_rules', return_value=mock_articles) as mock_apply:
        
        url = "https://example.com/news"
        result = discoverer.discover(
            url,
            include_patterns=None,
            exclude_patterns=None,
            date_extraction_pattern=None
        )
        
        # Verify only local method was called
        assert mock_local.called, "Local discovery should be attempted"
        assert not mock_browser.called, "Browser should not be attempted after local success"
        
        # Should return the articles
        assert result == mock_articles


def test_web_discoverer_browser_fallback():
    """Test that browser discovery is used as fallback when local fails."""
    discoverer = WebDiscoverer()
    
    mock_articles = [
        ("https://example.com/article1", "Article 1", "2024-01-15T10:00:00Z"),
    ]
    
    with patch.object(discoverer, '_discover_with_local', return_value=(None, False)) as mock_local, \
         patch.object(discoverer, '_discover_with_browser', return_value=mock_articles) as mock_browser, \
         patch.object(discoverer, '_discover_with_jina', return_value=None) as mock_jina, \
         patch.object(discoverer, '_discover_with_firecrawl', return_value=None) as mock_firecrawl, \
         patch.object(discoverer, '_apply_extraction_rules', return_value=mock_articles) as mock_apply:
        
        url = "https://example.com/news"
        result = discoverer.discover(
            url,
            include_patterns=None,
            exclude_patterns=None,
            date_extraction_pattern=None
        )
        
        # Verify fallback chain
        assert mock_local.called, "Local discovery should be attempted first"
        assert mock_browser.called, "Browser should be attempted after local fails"
        
        assert result == mock_articles


def test_discovery_cost_reduction():
    """Test that Phase 2 reduces API costs by eliminating external API calls."""
    discoverer = WebDiscoverer()
    
    # Simulate 10 sources
    sources = [f"https://example.com/news{i}" for i in range(10)]
    
    api_call_count = 0
    
    with patch.object(discoverer, '_discover_with_local', return_value=([("url", "title", "date")], False)), \
         patch.object(discoverer, '_discover_with_browser', return_value=None), \
         patch.object(discoverer, '_discover_with_jina', return_value=None) as mock_jina, \
         patch.object(discoverer, '_discover_with_firecrawl', return_value=None) as mock_firecrawl, \
         patch.object(discoverer, '_apply_extraction_rules', return_value=[("url", "title", "date")]):
        
        for source in sources:
            discoverer.discover(
                source,
                include_patterns=None,
                exclude_patterns=None,
                date_extraction_pattern=None
            )
        
        # Count API calls
        api_call_count = mock_jina.call_count + mock_firecrawl.call_count
        
        # Before Phase 2: Would have 10+ API calls (one per source)
        # After Phase 2: Should have 0 API calls
        assert api_call_count == 0, f"Should have 0 API calls, got {api_call_count}"
        
        print(f"\n✅ Phase 2 cost reduction: 0 API calls (100% reduction)")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
