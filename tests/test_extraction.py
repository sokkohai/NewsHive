"""Tests for extraction module.

Tests WebExtractor, EmailExtractor, and Extractor as specified in
specs/core/EXTRACTION.md.
"""

from unittest.mock import patch, MagicMock
from src.extraction import BrowserCrawler, EmailExtractor, Extractor, WebExtractor, SELENIUM_AVAILABLE
from src.models import ContentItem


class TestBrowserCrawler:
    """Test BrowserCrawler fallback method."""

    def test_browser_crawler_initialization(self):
        """Test BrowserCrawler initialization."""
        crawler = BrowserCrawler()
        assert crawler is not None
        assert len(crawler.USER_AGENTS) > 0

    def test_extract_returns_none_on_network_error(self):
        """Test extraction returns None when all browser methods fail."""
        crawler = BrowserCrawler()

        # Mock both Playwright and Selenium to fail
        with patch('src.extraction.sync_playwright') as mock_playwright:
            mock_playwright.side_effect = Exception("Network error")
            
            # Mock Selenium if available
            if SELENIUM_AVAILABLE:
                with patch('src.extraction.webdriver') as mock_webdriver:
                    mock_webdriver.Chrome.side_effect = Exception("ChromeDriver error")
                    result = crawler.extract("https://example.com/article")
            else:
                result = crawler.extract("https://example.com/article")
            
            assert result is None

    def test_extract_returns_none_on_timeout(self):
        """Test extraction returns None when navigation times out."""
        crawler = BrowserCrawler()

        # Mock Playwright to fail with timeout
        with patch('src.extraction.sync_playwright') as mock_playwright:
            mock_p = MagicMock()
            mock_browser = MagicMock()
            mock_context = MagicMock()
            mock_page = MagicMock()

            mock_playwright.return_value.__enter__.return_value = mock_p
            mock_p.chromium.launch.return_value = mock_browser
            mock_browser.new_context.return_value = mock_context
            mock_context.new_page.return_value = mock_page
            mock_page.goto.side_effect = Exception("Timeout")

            # Also mock Selenium to fail if available
            if SELENIUM_AVAILABLE:
                with patch('src.extraction.webdriver') as mock_webdriver:
                    mock_webdriver.Chrome.side_effect = Exception("ChromeDriver timeout")
                    result = crawler.extract("https://example.com/article")
            else:
                result = crawler.extract("https://example.com/article")
            
            assert result is None


class TestWebExtractor:
    """Test WebExtractor."""

    def test_web_extractor_initialization(self):
        """Test WebExtractor initialization."""
        extractor = WebExtractor()
        assert extractor is not None
        assert extractor.local_extractor is not None
        assert extractor.browser_crawler is not None

    def test_extract_uses_local_when_no_keys(self, monkeypatch):
        """Test fallback to local extractor when API keys are missing."""
        monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
        monkeypatch.delenv("JINA_API_KEY", raising=False)

        extractor = WebExtractor()
        
        # Mock local extractor to succeed
        with patch.object(extractor.local_extractor, 'extract', return_value="Local content"):
            result = extractor.extract("https://example.com/article")
            assert result is not None
            content, method = result
            assert content == "Local content"
            assert method == "local"

    def test_extract_returns_none_when_all_fail(self, monkeypatch):
        """Test extraction returns None when all methods fail."""
        monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
        monkeypatch.delenv("JINA_API_KEY", raising=False)

        extractor = WebExtractor()
        
        # Mock local and browser extractors to fail
        with patch.object(extractor.local_extractor, 'extract', return_value=None):
            with patch.object(extractor.browser_crawler, 'extract', return_value=None):
                result = extractor.extract("https://example.com/article")
                assert result is None

    def test_extract_browser_crawl_fallback(self, monkeypatch):
        """Test browser crawl method is used as final fallback.
        
        Verifies the 4-method fallback chain: Firecrawl -> Jina -> Local -> Browser
        """
        monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
        monkeypatch.delenv("JINA_API_KEY", raising=False)

        extractor = WebExtractor()
        
        # Mock local extractor to fail, browser crawl to succeed
        with patch.object(extractor.local_extractor, 'extract', return_value=None):
            with patch.object(extractor.browser_crawler, 'extract', return_value="Browser crawled content"):
                result = extractor.extract("https://example.com/article")
                assert result is not None
                content, method = result
                assert content == "Browser crawled content"
                assert method == "browser_crawl"


class TestEmailExtractor:
    """Test EmailExtractor."""

    def test_email_extractor_initialization(self):
        """Test EmailExtractor initialization."""
        extractor = EmailExtractor()
        assert extractor is not None

    def test_extract_email_body_string(self):
        """Test extracting email body from string.

        Per spec: Email body extraction always succeeds (no API failure possible).
        Extraction method should be 'email_body'.
        """
        extractor = EmailExtractor()
        email_body = "This is the email content\nWith multiple lines"

        content, method = extractor.extract(email_body)
        assert content == email_body
        assert method == "email_body"

    def test_extract_email_body_bytes(self):
        """Test extracting email body from bytes.

        Per spec: Email body extraction handles both string and bytes input.
        Converts bytes to UTF-8 string internally.
        """
        extractor = EmailExtractor()
        email_body_bytes = b"This is the email content\nWith multiple lines"
        expected_content = "This is the email content\nWith multiple lines"

        content, method = extractor.extract(email_body_bytes)
        assert content == expected_content
        assert method == "email_body"

    def test_extract_email_body_html(self):
        """Test extracting email body with HTML content.

        Per spec: HTML emails are converted/used directly as content.
        """
        extractor = EmailExtractor()
        email_body = "<html><body><p>Email content</p></body></html>"

        content, method = extractor.extract(email_body)
        assert content == email_body
        assert method == "email_body"


class TestExtractor:
    """Test Extractor orchestrator."""

    def test_extractor_initialization(self):
        """Test Extractor initialization."""
        extractor = Extractor()
        assert extractor.web_extractor is not None
        assert extractor.email_extractor is not None

    def test_extract_web_item_fails(self, monkeypatch):
        """Test extraction of web item fails without API keys."""
        monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
        monkeypatch.delenv("JINA_API_KEY", raising=False)
        
        extractor = Extractor()

        item = ContentItem(
            id="https://example.com/article",
            source_type="web",
            source_key="https://example.com/article",
            title="Article Title",
            summary="",
            content="",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-08T10:00:00Z",
            extracted_at="",
        )

        # Mock local and browser extractors to fail
        with patch.object(extractor.web_extractor.local_extractor, 'extract', return_value=None):
            with patch.object(extractor.web_extractor.browser_crawler, 'extract', return_value=None):
                result, status = extractor.extract(item)
                # Should return None with failure reason (now descriptive)
                assert result is None
                assert "extraction failed" in status.lower()

    def test_process_empty_list(self):
        """Test processing empty list of items."""
        extractor = Extractor()
        extracted, failed, filtered = extractor.process([])

        assert len(extracted) == 0
        assert len(failed) == 0
        assert len(filtered) == 0

    def test_process_web_items(self, monkeypatch):
        """Test processing web items."""
        monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
        monkeypatch.delenv("JINA_API_KEY", raising=False)
        
        extractor = Extractor()

        items = [
            ContentItem(
                id="https://example.com/article1",
                source_type="web",
                source_key="https://example.com/article1",
                title="Article 1",
                summary="",
                content="",
                categories=[],
                published_at="unknown",
                discovered_at="2026-01-08T10:00:00Z",
                extracted_at="",
            ),
            ContentItem(
                id="https://example.com/article2",
                source_type="web",
                source_key="https://example.com/article2",
                title="Article 2",
                summary="",
                content="",
                categories=[],
                published_at="unknown",
                discovered_at="2026-01-08T10:00:00Z",
                extracted_at="",
            ),
        ]

        # Mock local and browser extraction to fail
        with patch.object(extractor.web_extractor.local_extractor, 'extract', return_value=None):
            with patch.object(extractor.web_extractor.browser_crawler, 'extract', return_value=None):
                extracted, failed, filtered = extractor.process(items)

                assert len(extracted) == 0
                assert len(failed) == 2
                assert len(filtered) == 0
                assert failed[0][0] == "https://example.com/article1"
