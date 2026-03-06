import pytest
from unittest.mock import MagicMock, patch
from src.extraction import LocalExtractor

class TestLocalExtractor:
    @pytest.fixture
    def extractor(self):
        return LocalExtractor()

    def test_tier1_success(self, extractor):
        """Test successful static extraction."""
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            # Mock HTML with > 50 chars of content
            content_text = "Valid content " * 10
            mock_response.text = f"<html><body><p>{content_text}</p><script>var x=1;</script></body></html>"
            mock_get.return_value = mock_response

            result = extractor._extract_tier1_static("http://example.com")
            assert result is not None
            assert "Valid content" in result
            assert "var x=1" not in result # Script should be removed

    def test_tier1_failure_status(self, extractor):
        """Test static extraction failure on 404."""
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response

            result = extractor._extract_tier1_static("http://example.com")
            assert result is None

    def test_tier1_failure_empty(self, extractor):
        """Test static extraction failure on empty content."""
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = "<html><body></body></html>"
            mock_get.return_value = mock_response

            result = extractor._extract_tier1_static("http://example.com")
            assert result is None

    def test_tier2_success(self, extractor):
        """Test successful dynamic extraction."""
        with patch('src.extraction.sync_playwright') as mock_playwright:
            mock_p = MagicMock()
            mock_browser = MagicMock()
            mock_context = MagicMock()
            mock_page = MagicMock()
            
            mock_playwright.return_value.__enter__.return_value = mock_p
            mock_p.chromium.launch.return_value = mock_browser
            mock_browser.new_context.return_value = mock_context
            mock_context.new_page.return_value = mock_page
            
            # Mock content > 50 chars
            content_text = "Dynamic Content " * 10
            mock_page.content.return_value = f"<html><body><div>{content_text}</div></body></html>"
            
            result = extractor._extract_tier2_dynamic("http://example.com")
            
            assert result is not None
            assert "Dynamic Content" in result
            mock_page.goto.assert_called()
            mock_browser.close.assert_called()

    def test_extract_fallback_chain(self, extractor):
        """Test that extract calls tier 1 then tier 2."""
        with patch.object(extractor, '_extract_tier1_static') as mock_tier1, \
             patch.object(extractor, '_extract_tier2_dynamic') as mock_tier2:
            
            # Case 1: Tier 1 succeeds
            mock_tier1.return_value = "Tier 1 Content"
            result = extractor.extract("http://example.com")
            assert result == "Tier 1 Content"
            mock_tier2.assert_not_called()
            
            # Case 2: Tier 1 fails, Tier 2 succeeds
            mock_tier1.reset_mock()
            mock_tier1.return_value = None
            mock_tier2.return_value = "Tier 2 Content"
            result = extractor.extract("http://example.com")
            assert result == "Tier 2 Content"
            mock_tier1.assert_called()
            mock_tier2.assert_called()

            # Case 3: Both fail
            mock_tier1.reset_mock()
            mock_tier2.reset_mock()
            mock_tier1.return_value = None
            mock_tier2.return_value = None
            result = extractor.extract("http://example.com")
            assert result is None
