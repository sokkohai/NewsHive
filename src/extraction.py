"""Extraction stage implementation.

Implements content extraction from web and email sources as specified in
specs/core/EXTRACTION.md.
"""

import hashlib
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

try:
    import trafilatura
    TRAFILATURA_AVAILABLE = True
except ImportError:
    TRAFILATURA_AVAILABLE = False

from .models import ContentItem
from .language_detection import detect_language
from .listings_extractor import get_listings_extractor, ListingArticle
from .config import Configuration

# Try to import Playwright
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# Try to import Selenium
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service as ChromeService
    SELENIUM_AVAILABLE = True

    # Try to auto-install matching ChromeDriver
    try:
        import chromedriver_autoinstaller
        chromedriver_autoinstaller.install()
    except Exception:
        pass
except ImportError:
    SELENIUM_AVAILABLE = False

logger = logging.getLogger(__name__)


class _AuthRequiredError(Exception):
    """Raised when a 401 response indicates authentication is required.

    Used to short-circuit the entire extraction fallback chain — browser-based
    retries won't help when the server explicitly requires credentials.
    """
    pass


class LocalExtractor:
    """Extracts content using local resources (Tier 1: Static, Tier 2: Dynamic).

    Implements local extraction fallback as specified in specs/core/EXTRACTION.md.
    """

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    ]

    def _get_headers(self) -> dict[str, str]:
        return {
            "User-Agent": random.choice(self.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }

    def _clean_html(self, html_content: str) -> str:
        soup = BeautifulSoup(html_content, "html.parser")

        # Remove unwanted elements
        for element in soup(["script", "style", "nav", "footer", "header", "aside", "iframe", "noscript"]):
            element.decompose()

        # Get text
        text = soup.get_text(separator="\n\n")

        # Clean whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = "\n".join(chunk for chunk in chunks if chunk)

        return text

    def _extract_article_text(self, html_content: str) -> str | None:
        """Extract article body text from HTML.

        Uses trafilatura for accurate article body detection (removes nav,
        sidebar, footer boilerplate). Falls back to _clean_html if trafilatura
        returns nothing.
        """
        if TRAFILATURA_AVAILABLE:
            text = trafilatura.extract(
                html_content,
                include_comments=False,
                include_tables=False,
                no_fallback=False,
            )
            if text and len(text.strip()) > 100:
                return text.strip()
        # Fallback: generic HTML cleaning
        return self._clean_html(html_content)

    def _extract_tier1_static(self, url: str) -> str | None:
        """Attempt Tier 1: Static HTML parsing."""
        try:
            # Random delay 1-3s
            time.sleep(random.uniform(1.0, 3.0))

            response = requests.get(
                url,
                headers=self._get_headers(),
                timeout=10,
                allow_redirects=True
            )

            if response.status_code == 401:
                raise _AuthRequiredError(f"HTTP 401 for {url}")
            # 403 → return None; browser fallbacks may bypass bot-detection
            if response.status_code == 200:
                text = self._extract_article_text(response.text)
                if text and len(text.strip()) > 0:
                    return text
            return None
        except _AuthRequiredError:
            raise  # Re-raise so callers can short-circuit all fallbacks
        except Exception:
            return None

    def _extract_tier2_dynamic(self, url: str) -> str | None:
        """Attempt Tier 2: Headless browser rendering."""
        # Try Playwright first
        if PLAYWRIGHT_AVAILABLE:
            result = self._extract_with_playwright(url)
            if result:
                return result

        # Fall back to Selenium
        if SELENIUM_AVAILABLE:
            result = self._extract_with_selenium(url)
            if result:
                return result

        return None

    def _extract_with_playwright(self, url: str) -> str | None:
        """Extract using Playwright headless browser."""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                try:
                    context = browser.new_context(viewport={"width": 1920, "height": 1080})
                    page = context.new_page()
                    page.set_extra_http_headers(self._get_headers())
                    page.goto(url, timeout=15000, wait_until="domcontentloaded")
                    page.wait_for_timeout(2000)
                    content = page.content()
                    text = self._extract_article_text(content)
                    if text and len(text.strip()) > 0:
                        return text
                    return None
                finally:
                    browser.close()
        except Exception as e:
            logger.debug(f"Playwright extraction failed: {e}")
            return None

    def _extract_with_selenium(self, url: str) -> str | None:
        """Extract using Selenium with system ChromeDriver."""
        try:
            options = ChromeOptions()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument(f"--user-agent={random.choice(self.USER_AGENTS)}")

            # Try system ChromeDriver path first (Program Files)
            chromedriver_paths = [
                r"C:\Program Files\Google\Chrome\Application\chromedriver.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe",
            ]

            service = None
            for path in chromedriver_paths:
                if os.path.exists(path):
                    service = ChromeService(executable_path=path)
                    break

            if service:
                driver = webdriver.Chrome(service=service, options=options)
            else:
                # Fall back to PATH lookup
                driver = webdriver.Chrome(options=options)

            try:
                driver.get(url)
                time.sleep(2)  # Wait for JS execution
                content = driver.page_source
                text = self._extract_article_text(content)
                if text and len(text.strip()) > 0:
                    return text
                return None
            finally:
                driver.quit()
        except Exception as e:
            logger.debug(f"Selenium extraction failed: {e}")
            return None

    def extract(self, url: str) -> str | None:
        """Execute extraction fallback chain."""
        # Tier 1
        content = self._extract_tier1_static(url)
        if content:
            return content

        # Tier 2
        content = self._extract_tier2_dynamic(url)
        if content:
            return content

        return None


class BrowserCrawler:
    """Browser-based content extraction as final fallback.

    Implements browser crawling as specified in specs/core/EXTRACTION.md
    section 4: uses headless Chromium for JavaScript-rendered content.
    """

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    ]

    def _get_headers(self) -> dict[str, str]:
        """Get standard browser headers for requests."""
        return {
            "User-Agent": random.choice(self.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }

    def _clean_html(self, html_content: str) -> str:
        """Remove boilerplate and extract text from HTML."""
        soup = BeautifulSoup(html_content, "html.parser")

        # Remove unwanted elements
        for element in soup(["script", "style", "nav", "footer", "header", "aside", "iframe", "noscript"]):
            element.decompose()

        # Get text
        text = soup.get_text(separator="\n\n")

        # Clean whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = "\n".join(chunk for chunk in chunks if chunk)

        return text

    def _detect_article_content(self, soup: BeautifulSoup) -> str | None:
        """Detect article content from DOM using common patterns.

        Searches for common article patterns, falls back to reading-view
        algorithm if needed.
        """
        # Common article container selectors
        article_selectors = [
            "article",
            "[role='main']",
            ".article-body",
            ".post-content",
            ".entry-content",
            ".content",
            "main",
        ]

        for selector in article_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    # Found a candidate container
                    text = self._clean_html(str(element))
                    if text and len(text.strip()) > 0:
                        return text
            except Exception:
                continue

        # Fallback: reading-view algorithm (find largest text block)
        paragraphs = soup.find_all(["p", "div", "article"])
        if paragraphs:
            # Find the container with the most text
            largest = max(
                paragraphs,
                key=lambda x: len(x.get_text(strip=True))
            )
            text = self._clean_html(str(largest))
            if text and len(text.strip()) > 0:
                return text

        return None

    def extract(self, url: str) -> str | None:
        """Extract content using headless browser (final fallback).

        Args:
            url: URL to extract content from

        Returns:
            Extracted content, or None if extraction fails
        """
        # Try Playwright first
        if PLAYWRIGHT_AVAILABLE:
            result = self._extract_with_playwright(url)
            if result:
                return result

        # Fall back to Selenium
        if SELENIUM_AVAILABLE:
            result = self._extract_with_selenium(url)
            if result:
                return result

        return None

    def _extract_with_playwright(self, url: str) -> str | None:
        """Extract using Playwright headless browser."""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                try:
                    context = browser.new_context(viewport={"width": 1920, "height": 1080})
                    page = context.new_page()
                    page.set_extra_http_headers(self._get_headers())
                    page.goto(url, timeout=30000, wait_until="domcontentloaded")
                    content = page.content()
                    if TRAFILATURA_AVAILABLE:
                        text = trafilatura.extract(content, include_comments=False, include_tables=False, no_fallback=False)
                        if text and len(text.strip()) > 100:
                            return text.strip()
                    soup = BeautifulSoup(content, "html.parser")
                    text = self._detect_article_content(soup)
                    if text and len(text.strip()) > 0:
                        return text
                    return None
                finally:
                    browser.close()
        except Exception as e:
            logger.debug(f"BrowserCrawler Playwright failed: {e}")
            return None

    def _extract_with_selenium(self, url: str) -> str | None:
        """Extract using Selenium with system ChromeDriver."""
        try:
            options = ChromeOptions()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument(f"--user-agent={random.choice(self.USER_AGENTS)}")

            chromedriver_paths = [
                r"C:\Program Files\Google\Chrome\Application\chromedriver.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe",
            ]

            service = None
            for path in chromedriver_paths:
                if os.path.exists(path):
                    service = ChromeService(executable_path=path)
                    break

            if service:
                driver = webdriver.Chrome(service=service, options=options)
            else:
                driver = webdriver.Chrome(options=options)

            try:
                driver.set_page_load_timeout(30)
                driver.get(url)
                content = driver.page_source
                if TRAFILATURA_AVAILABLE:
                    text = trafilatura.extract(content, include_comments=False, include_tables=False, no_fallback=False)
                    if text and len(text.strip()) > 100:
                        return text.strip()
                soup = BeautifulSoup(content, "html.parser")
                text = self._detect_article_content(soup)
                if text and len(text.strip()) > 0:
                    return text
                return None
            finally:
                driver.quit()
        except Exception as e:
            logger.debug(f"BrowserCrawler Selenium failed: {e}")
            return None


class WebExtractor:
    """Extracts article content from web URLs.

    Implements web extraction with Local (Tier 1: static + Tier 2: dynamic),
    Browser Crawling, and Listings extraction as specified in
    specs/core/EXTRACTION.md.
    """

    def __init__(self, config: Configuration | None = None) -> None:
        """Initialize the WebExtractor.
        
        Args:
            config: Configuration object for checking listing page URLs
        """
        self.config = config
        self.article_max_age_days = config.article_max_age_days if config else 3
        self.local_extractor = LocalExtractor()
        self.browser_crawler = BrowserCrawler()
        self.listings_extractor = get_listings_extractor()  # Add listings extractor

    def _is_configured_listing_page(self, url: str) -> bool:
        """Check if URL is configured as an inline listing page in the config.

        Only URLs explicitly marked as listing pages with listings_type="inline"
        should have article container detection applied.
        
        Note: Articles extracted from inline listing pages are marked as
        extraction_method="listings_article" and will NOT have container detection
        applied if re-encountered, preventing recursive extraction.

        Args:
            url: URL to check

        Returns:
            True if URL is configured as a listing page (listings_type: "inline")
        """
        if not self.config or not self.config.web_sources:
            return False

        for source in self.config.web_sources:
            # Check if URL matches this source
            if url.startswith(source.url) or source.url in url:
                # Only detect listings on URLs configured with listings_type="inline"
                # "linked" pages have individual article URLs and don't need listings detection
                return source.listings_type == "inline"

        return False

    def extract(self, url: str, fetch_method: str = "auto") -> tuple[str, str] | None:
        """Extract article content from URL with listings support.

        When fetch_method is "auto" (default), tries the full fallback chain:
        1. Check if URL is configured as a listing page (listings_type="inline")
        2. Local (Tier 1: static HTTP + Tier 2: dynamic headless browser)
        3. Browser Crawling (final fallback)

        When a specific method is set, only that method is used:
        - "static": Only HTTP requests (no JS rendering)
        - "browser" / "playwright": Only headless browser (Playwright/Selenium)

        Args:
            url: URL to extract content from
            fetch_method: Fetch method to use ("auto", "static", "browser", "playwright")

        Returns:
            Tuple of (content, extraction_method) or None if all fail
        """
        method = fetch_method.lower()
        
        # 0. Check if this URL is configured as a listing page
        # Only attempt listings detection on URLs explicitly marked in config
        if self._is_configured_listing_page(url):
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
            
            # Check for URL fragment (targeted extraction)
            from urllib.parse import urlparse
            parsed_url = urlparse(url)
            fragment = parsed_url.fragment
            
            html_content = None
            try:
                # Use base URL without fragment for fetching
                base_url = url.split("#")[0]
                response = requests.get(base_url, headers=headers, timeout=10)
                html_content = response.text
            except Exception:
                html_content = None
            
            # If fragment is present, extract ONLY that article
            if fragment and html_content:
                logger.info(f"Targeted extraction for inline article: {url} (fragment: {fragment})")
                article_content = self.listings_extractor.extract_single_article(html_content, base_url, fragment)
                if article_content:
                    return article_content, "listings_article"
                else:
                    logger.warning(f"Failed to find article by fragment {fragment} in {base_url}")
                    return None
            
            # Otherwise, try to detect ALL articles in the configured listing page (legacy/hub behavior)
            if html_content and self.listings_extractor.detect_listings_page(html_content):
                logger.info(f"Detected articles on configured listing page: {url}")
                articles = self.listings_extractor.parse_articles(html_content, url, self.article_max_age_days)
                if articles:
                    listings_data = {
                        "type": "listings",
                        "source_url": url,
                        "count": len(articles),
                        "articles": [
                            {
                                "title": article.title,
                                "date": article.date,
                                "content": article.content,
                                "url": article.url,
                                "category": article.category,
                                "confidence": article.confidence,
                                "extraction_method": article.extraction_method,
                            }
                            for article in articles
                        ]
                    }
                    logger.info(f"Successfully extracted {len(articles)} articles from listings page")
                    return json.dumps(listings_data, ensure_ascii=False), "listings"
                else:
                    logger.info(f"Configured listing page but no articles extracted from {url}")
                    listings_data = {
                        "type": "listings",
                        "source_url": url,
                        "count": 0,
                        "articles": []
                    }
                    return json.dumps(listings_data, ensure_ascii=False), "listings"
            else:
                logger.debug(f"Configured listing page but no article pattern detected: {url}")
        
        # --- Specific method: static only ---
        if method == "static":
            content = self.local_extractor._extract_tier1_static(url)
            if content:
                return content, "local"
            return None
        
        # --- Specific method: browser/playwright only ---
        if method in ("browser", "playwright"):
            content = self.local_extractor._extract_tier2_dynamic(url)
            if content:
                return content, "browser"
            content = self.browser_crawler.extract(url)
            if content:
                return content, "browser_crawl"
            return None
        
        # --- Auto: full fallback chain (default) ---
        # 1. Try primary method (Local - Tier 1: static + Tier 2: dynamic)
        # _AuthRequiredError (401) propagates upward to skip all browser fallbacks.
        # 403 falls through to browser_crawl — Playwright may bypass bot-detection.
        content = self.local_extractor.extract(url)
        if content:
            return content, "local"

        # 2. Try secondary fallback (Browser Crawling)
        content = self.browser_crawler.extract(url)
        if content:
            return content, "browser_crawl"

        # All methods failed
        return None

    def extract_published_date(self, url: str, html_content: str) -> str:
        """Extract publication date from HTML content.

        Tries multiple methods in priority order:
        0. trafilatura bare_extraction (uses htmldate internally – fastest & most robust)
        1. Schema.org structured data (JSON-LD) - most reliable
        2. Open Graph meta tags (og:published_time, og:updated_time)
        3. Common meta tags (article:published_time, publish_date, date)
        4. Data attributes (data-date, data-published, etc.)
        5. Time tags with datetime attribute
        6. HTML patterns (heuristic)

        If date extraction fails, returns "unknown".

        Args:
            url: Article URL (for logging)
            html_content: HTML content of the article

        Returns:
            ISO 8601 timestamp string or "unknown"
        """
        try:
            # 0. Try trafilatura's metadata extraction (uses htmldate – highest quality)
            if TRAFILATURA_AVAILABLE:
                try:
                    result = trafilatura.bare_extraction(
                        html_content,
                        only_with_metadata=False,
                        include_comments=False,
                    )
                    if result and result.date:
                        normalized = self._normalize_date(result.date)
                        if normalized != "unknown":
                            logger.debug(f"Date extracted via trafilatura for {url}: {normalized}")
                            return normalized
                except Exception as e:
                    logger.debug(f"trafilatura metadata extraction failed for {url}: {e}")

            soup = BeautifulSoup(html_content, "html.parser")

            # 1. Try Schema.org structured data (most reliable)
            date_str = self._extract_schema_date(soup)
            if date_str:
                normalized = self._normalize_date(date_str)
                if normalized != "unknown":
                    logger.debug(f"Date extracted from Schema.org for {url}: {normalized}")
                    return normalized

            # 2. Try Open Graph meta tags
            date_str = self._extract_og_date(soup)
            if date_str:
                normalized = self._normalize_date(date_str)
                if normalized != "unknown":
                    logger.debug(f"Date extracted from Open Graph for {url}: {normalized}")
                    return normalized

            # 3. Try common meta tags
            date_str = self._extract_meta_date(soup)
            if date_str:
                normalized = self._normalize_date(date_str)
                if normalized != "unknown":
                    logger.debug(f"Date extracted from meta tags for {url}: {normalized}")
                    return normalized

            # 4. Try data attributes
            date_str = self._extract_data_attributes(soup)
            if date_str:
                normalized = self._normalize_date(date_str)
                if normalized != "unknown":
                    logger.debug(f"Date extracted from data attributes for {url}: {normalized}")
                    return normalized

            # 5. Try time tags
            date_str = self._extract_time_tags(soup)
            if date_str:
                normalized = self._normalize_date(date_str)
                if normalized != "unknown":
                    logger.debug(f"Date extracted from time tags for {url}: {normalized}")
                    return normalized

            # 6. Try HTML patterns (fallback)
            date_str = self._extract_pattern_date(html_content)
            if date_str:
                normalized = self._normalize_date(date_str)
                if normalized != "unknown":
                    logger.debug(f"Date extracted from patterns for {url}: {normalized}")
                    return normalized

            logger.debug(f"Could not extract publication date for URL: {url}")
            return "unknown"
        except Exception as e:
            logger.debug(
                f"Error extracting publication date for {url}: {e}"
            )
            return "unknown"

    def _extract_og_date(self, soup: BeautifulSoup) -> str | None:
        """Extract date from Open Graph meta tags."""
        # Try og:published_time first
        for tag_name in ["og:published_time", "og:updated_time"]:
            meta = soup.find("meta", property=tag_name)
            if meta and meta.get("content"):
                return meta.get("content")
        return None

    def _extract_schema_date(self, soup: BeautifulSoup) -> str | None:
        """Extract date from Schema.org structured data."""
        # Try JSON-LD
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                data = json.loads(script.string)
                # Handle both direct object and array
                items = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
                for item in items:
                    if isinstance(item, dict):
                        # Try datePublished first
                        if "datePublished" in item:
                            return item["datePublished"]
                        # Try dateModified as fallback
                        if "dateModified" in item:
                            return item["dateModified"]
            except (json.JSONDecodeError, TypeError):
                continue

        # Try microdata
        for tag in ["time", "span", "div"]:
            elem = soup.find(
                tag,
                attrs={
                    "itemprop": re.compile(r"datePublished|dateModified", re.I)
                },
            )
            if elem and elem.get("datetime"):
                return elem.get("datetime")
            if elem and elem.get("content"):
                return elem.get("content")

        return None

    def _extract_meta_date(self, soup: BeautifulSoup) -> str | None:
        """Extract date from common meta tags."""
        meta_tags = [
            "article:published_time",
            "article:modified_time",
            "publish_date",
            "date",
            "dc:date",
        ]
        for tag in meta_tags:
            # Check both name and property attributes
            meta = soup.find("meta", attrs={"name": tag})
            if not meta:
                meta = soup.find("meta", attrs={"property": tag})
            if meta and meta.get("content"):
                return meta.get("content")

        return None

    def _extract_data_attributes(self, soup: BeautifulSoup) -> str | None:
        """Extract date from data-* attributes."""
        # Common data attributes used for dates
        data_attrs = [
            "data-date",
            "data-published",
            "data-publish-date",
            "data-time",
            "data-timestamp",
            "data-created",
            "data-article-date"
        ]
        
        for attr in data_attrs:
            elem = soup.find(attrs={attr: True})
            if elem and elem.get(attr):
                date_value = elem.get(attr)
                # Skip empty or placeholder values
                if date_value and len(str(date_value).strip()) > 0:
                    return str(date_value)
        
        return None

    def _extract_time_tags(self, soup: BeautifulSoup) -> str | None:
        """Extract date from HTML5 time tags."""
        # Look for <time> tags with datetime attribute
        time_tag = soup.find("time", attrs={"datetime": True})
        if time_tag and time_tag.get("datetime"):
            return time_tag.get("datetime")
        
        # Look for time tags with common classes
        time_classes = ["published", "entry-date", "post-date", "article-date", "date"]
        for cls in time_classes:
            time_tag = soup.find("time", class_=cls)
            if time_tag:
                # Try datetime attribute first
                if time_tag.get("datetime"):
                    return time_tag.get("datetime")
                # Fallback to text content
                text = time_tag.get_text(strip=True)
                if text and len(text) < 50:  # Reasonable length for a date
                    return text
        
        return None

    def _extract_pattern_date(self, html_content: str) -> str | None:
        """Extract date using regex patterns."""
        # Common date patterns (simplified) - supports German and English month names
        patterns = [
            r"(\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2})",  # ISO-like
            r"(\d{4}[-/]\d{2}[-/]\d{2})",  # Date only
            # English months with flexible spacing
            r"(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}",
            r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})",  # 16 Oct 2017
            # German months (full and abbreviated) with flexible spacing
            r"(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember|Jan|Feb|Mär|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dez)\s+\d{1,2},?\s+\d{4}",
            r"(\d{1,2}\s+(?:Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember|Jan|Feb|Mär|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dez)\s+\d{4})",  # 16 Okt 2017
            # German day.month.year format: 16. Okt 2017 or 16.10.2017
            r"(\d{1,2}\.\s+(?:Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember|Jan|Feb|Mär|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dez)\s+\d{4})",
            r"(\d{1,2}\.\d{1,2}\.\d{4})",
        ]

        for pattern in patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                return match.group(1)

        return None

    def _normalize_date(self, date_str: str) -> str:
        """Convert date string to ISO 8601 UTC format.

        Returns "unknown" if parsing fails.
        Supports German and English date formats.
        """
        try:
            # German month mapping for parsing
            german_months = {
                'januar': 'January', 'februar': 'February', 'märz': 'March',
                'april': 'April', 'mai': 'May', 'juni': 'June',
                'juli': 'July', 'august': 'August', 'september': 'September',
                'oktober': 'October', 'november': 'November', 'dezember': 'December',
                'jan': 'Jan', 'feb': 'Feb', 'mär': 'Mar', 'apr': 'Apr',
                'jun': 'Jun', 'jul': 'Jul', 'aug': 'Aug', 'sep': 'Sep',
                'okt': 'Oct', 'nov': 'Nov', 'dez': 'Dec'
            }
            
            # Replace German month names with English equivalents
            normalized = date_str
            for de_month, en_month in german_months.items():
                normalized = re.sub(r'\b' + de_month + r'\b', en_month, normalized, flags=re.IGNORECASE)
            
            # Handle German date format: "16. Oktober 2017" -> "16 October 2017"
            normalized = re.sub(r'(\d{1,2})\.\s+', r'\1 ', normalized)
            
            # Try dateutil parser
            dt = dateutil_parser.parse(normalized, dayfirst=False)
            # Convert to UTC if timezone-aware
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc)
            else:
                # Assume UTC if no timezone info
                dt = dt.replace(tzinfo=timezone.utc)
            # Format as ISO 8601 with Z suffix
            return dt.isoformat().replace("+00:00", "Z")
        except (ValueError, TypeError):
            logger.debug(f"Could not parse date: {date_str}")
            return "unknown"


class EmailExtractor:
    """Extracts article content from email sources.

    Implements email extraction as specified in specs/core/EXTRACTION.md.
    """

    def extract(self, email_body: str | bytes) -> tuple[str, str]:
        """Extract content from email body.

        For email sources, the email body is used directly as the article
        content.
        Per spec: Email body extraction always succeeds (no API failure
        possible).

        Args:
            email_body: The email body text (string or bytes)

        Returns:
            Tuple of (content, extraction_method)
        """
        # Convert bytes to string if needed
        if isinstance(email_body, bytes):
            content = email_body.decode("utf-8", errors="replace")
        else:
            content = email_body

        return content, "email_body"


class Extractor:
    """Orchestrates content extraction for both web and email sources.

    Implements extraction stage as specified in specs/core/EXTRACTION.md.
    """

    def __init__(self, article_max_age_days: int = 3, config: Configuration | None = None) -> None:
        """Initialize the Extractor.

        Args:
            article_max_age_days: Maximum age of articles in days (default: 3)
            config: Configuration object for getting source categories (optional)
        """
        self.web_extractor = WebExtractor(config=config)
        self.email_extractor = EmailExtractor()
        self.article_max_age_days = article_max_age_days
        self.config = config

    def _get_source_categories(self, url: str) -> list[str]:
        """Get categories for a given URL from the configuration.
        
        Attempts to match the URL against configured web sources and returns
        the categories assigned to that source.
        
        Args:
            url: The URL to look up
            
        Returns:
            List of category names, or empty list if no match found
        """
        if not self.config or not self.config.web_sources:
            return []
        
        for source in self.config.web_sources:
            # Check if URL matches the source URL
            # Handle both exact matches and substring matches for URL roots
            if url.startswith(source.url) or source.url in url:
                return source.categories
        
        return []

    def _get_category_keywords(self, category_name: str) -> list[str]:
        """Get keywords for a specific category.
        
        Args:
            category_name: The category name to look up
            
        Returns:
            List of keywords for the category, or empty list if not found
        """
        if not self.config:
            return []
        
        for category in self.config.categories:
            if category.name == category_name:
                return category.keywords
        
        return []

    def _matches_category_keywords(self, text: str, category_keywords: list[str]) -> bool:
        """Check if text matches any of the category keywords.

        Uses case-insensitive substring matching with OR logic:
        Text matches if it contains ANY keyword.

        Args:
            text: Text to check against keywords
            category_keywords: List of keywords to match against

        Returns:
            True if text matches any keyword
        """
        if not category_keywords:
            return True  # No keywords = allow through
            
        text_lower = text.lower()
        for keyword in category_keywords:
            if keyword.lower() in text_lower:
                return True
        return False

    def _get_html_for_date_extraction(self, url: str) -> str | None:
        """Get HTML content for date extraction.
        
        Attempts a simple HTTP GET to retrieve the HTML for metadata extraction.
        This is separate from content extraction to get access to headers/metadata.
        
        Args:
            url: URL to fetch
            
        Returns:
            HTML content or None if fetch fails
        """
        try:
            # Use timeout of 10 seconds for quick fetch
            response = requests.get(
                url,
                headers=self.web_extractor.local_extractor._get_headers(),
                timeout=10,
                allow_redirects=True
            )
            if response.status_code == 200:
                return response.text
            return None
        except Exception as e:
            logger.debug(f"Failed to fetch HTML for date extraction from {url}: {e}")
            return None

    def extract(self, item: ContentItem) -> tuple[ContentItem | None, str | None]:
        """Extract content for a candidate item.

        Performs extraction and applies post-extraction keyword filtering.
        Per SPEC_CHANGES.md, keyword filtering occurs ONLY during extraction
        (post-extraction relevance validation).
        
        Note: If item.extraction_method is already set (e.g., by a previous stage),
        it is returned as-is without re-extraction. This allows listings responses
        to be passed through from WebExtractor to process() for multi-item expansion.

        Args:
            item: Candidate ContentItem (with source_key but no content yet)

        Returns:
            Tuple of (extracted_item, filter_status):
            - extracted_item: ContentItem with content and extraction_method, or None if extraction fails
            - filter_status: One of:
              - None: Extraction succeeded and passed keyword filtering
              - "extraction_failed": Extraction failed
              - "filtered_out": Extraction succeeded but no keywords matched
        """
        # PHASE 2: If extraction already done (e.g., listings response), pass through
        if item.extraction_method and item.extraction_method in ("listings", "listings_article"):
            logger.debug(
                f"Passing through already-extracted item: {item.source_key} "
                f"(method: {item.extraction_method})"
            )
            return item, None
        
        if item.source_type == "web":
            # Strip the "web:" prefix from source_key if present
            url = item.source_key
            if url.startswith("web:"):
                url = url[4:]
            # Look up fetch_method from config for this URL
            fetch_method = "auto"
            if self.config and self.config.web_sources:
                for source in self.config.web_sources:
                    if url.startswith(source.url) or source.url in url:
                        fetch_method = source.fetch_method
                        break
            try:
                result = self.web_extractor.extract(url, fetch_method=fetch_method)
            except _AuthRequiredError as e:
                logger.warning(
                    f"Auth required (HTTP 401) for {item.source_key}: {e} — skipping all fallbacks"
                )
                return None, "auth_required"
            if result:
                content, method = result
                item.content = content
                item.extraction_method = method
                item.extracted_at = (
                    datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
                
                # Detect source language from extracted content
                item.language_detected = detect_language(content)
                
                # NEW: Conditional date filtering (only if published_at was "unknown" in Discovery)
                # Per updated EXTRACTION.md spec: Only extract date if Discovery returned "unknown"
                if item.published_at == "unknown":
                    html_content = self._get_html_for_date_extraction(url)
                    if html_content:
                        item.published_at = self.web_extractor.extract_published_date(url, html_content)
                        logger.debug(
                            f"Extracted date from article page: {item.published_at} for {url}"
                        )
                    
                    # Check freshness ONLY if we just extracted a date
                    # BUT: Skip age filtering for listings pages - their age doesn't matter,
                    # only the age of articles within them matters
                    # Articles older than article_max_age_days are filtered out
                    if method != "listings" and item.published_at and item.published_at != "unknown":
                        from datetime import timedelta
                        try:
                            published_time = datetime.fromisoformat(
                                item.published_at.replace('Z', '+00:00')
                            )
                            current_time = datetime.now(timezone.utc)
                            article_age = current_time - published_time
                            
                            if article_age > timedelta(days=self.article_max_age_days):
                                filter_reason = f"Article too old ({article_age.days}d {article_age.seconds//3600}h, max: {self.article_max_age_days}d)"
                                logger.info(
                                    f"Filtering out old article: {item.source_key} - {filter_reason}"
                                )
                                return None, "Article too old"
                        except (ValueError, AttributeError):
                            # Can't parse date, allow article through (Option A)
                            logger.debug(
                                f"Unparseable date after extraction, allowing through: {item.source_key}"
                            )
                            pass
                # If published_at already has a valid date from Discovery, skip date extraction

                # Apply keyword filtering post-extraction using category keywords
                # Get the categories for this item and validate against category keywords
                categories = item.categories
                if categories:
                    matched_any = False
                    for category in categories:
                        category_keywords = self._get_category_keywords(category)
                        if self._matches_category_keywords(content, category_keywords):
                            matched_any = True
                            break
                    
                    if not matched_any:
                        filter_reason = f"No keywords matched in extracted content for categories '{categories}'"
                        logger.info(
                            f"Item {item.source_key} filtered out: {filter_reason}"
                        )
                        return None, filter_reason
                else:
                    logger.warning(
                        f"Item {item.source_key} has no category assigned, allowing through without keyword filtering"
                    )

                return item, None
            else:
                # Extraction failed
                return None, "Extraction failed: could not retrieve content"

        elif item.source_type == "email":
            # Note: Emails are treated as link containers during Discovery.
            # The EmailDiscoverer extracts links from email bodies and creates
            # ContentItems with source_type="web" for each link. Therefore,
            # extraction should never receive items with source_type="email".
            # If this occurs, it indicates a discovery/extraction pipeline error.
            logger.warning(
                f"Unexpected source_type='email' in extraction: {item.id}. "
                "Emails should be converted to web items during discovery."
            )
            return None, "extraction_failed"

        return None, "extraction_failed"

    def process(
        self, items: list[ContentItem]
    ) -> tuple[list[ContentItem], list[tuple[str, str]], list[tuple[str, str]]]:
        """Process multiple items for extraction.

        Implements Phase 2 listings support:
        - Detects listings page responses (extraction_method="listings")
        - Converts listings JSON into multiple ContentItems
        - Each article from listings becomes an independent item

        Args:
            items: List of candidate ContentItems to extract

        Returns:
            Tuple of (extracted_items, failed_items, filtered_items)
            - extracted_items: Successfully extracted and keyword-filtered items
            - failed_items: Items where extraction failed
            - filtered_items: Items where extraction succeeded but keywords didn't match
        """
        extracted_items: list[ContentItem] = []
        failed_items: list[tuple[str, str]] = []
        filtered_items: list[tuple[str, str]] = []
        new_listings_items: list[ContentItem] = []  # Items generated from listings
        listings_items_by_source: dict[str, list[ContentItem]] = {}  # Track which articles belong to which listings
        listings_filter_status: dict[str, str] = {}  # Track if all articles from listings were filtered

        for item in items:
            extracted, filter_status = self.extract(item)
            if extracted:
                # PHASE 2: Check if this is a listings response
                if extracted.extraction_method == "listings":
                    logger.info(
                        f"Detected listings page extraction for {item.source_key}. "
                        "Converting to individual article items."
                    )
                    logger.debug(
                        f"  Listings page has inline articles "
                        f"(extraction_method='listings' indicates direct extraction)"
                    )
                    
                    # Parse listings JSON from content
                    try:
                        listings_data = json.loads(extracted.content)
                        
                        if not isinstance(listings_data, dict) or "articles" not in listings_data:
                            logger.error(
                                f"Invalid listings JSON structure for {item.source_key}"
                            )
                            failed_items.append(
                                (item.source_key, "Invalid listings JSON structure")
                            )
                            continue
                        
                        articles = listings_data.get("articles", [])
                        logger.info(
                            f"Extracted {len(articles)} articles from listings page"
                        )
                        
                        articles_for_this_listing: list[ContentItem] = []
                        
                        # Generate new ContentItem for each article
                        for idx, article_data in enumerate(articles, 1):
                            try:
                                # Create new item for each article
                                article_url = article_data.get("url") or listings_data.get("source_url")
                                article_title = article_data.get("title", f"Article {idx}")
                                article_content = article_data.get("content", "")
                                article_date = article_data.get("date", "unknown")
                                article_confidence = article_data.get("confidence", 0.8)
                                
                                # Get category from source configuration
                                source_url = listings_data.get("source_url", "")
                                source_categories = self._get_source_categories(source_url)
                                
                                # Generate unique source key using content hash (not position)
                                # This enables proper deduplication - same article always gets same ID
                                # regardless of position on the listings page
                                # Hash is based on title + date for deterministic, stable identification
                                content_signature = f"{article_title.strip()}|{article_date}".encode()
                                article_hash = hashlib.sha256(content_signature).hexdigest()[:12]
                                article_source_key = f"listings:{listings_data.get('source_url', 'unknown')}#{article_hash}"
                                
                                # Create new ContentItem from article data
                                new_item = ContentItem(
                                    id=f"{article_source_key}-extracted",
                                    source_key=article_source_key,
                                    source_type="web",
                                    source_url=article_url,
                                    title=article_title,
                                    summary="",
                                    content=article_content,
                                    categories=source_categories,  # Assign from source config
                                    published_at=article_date,
                                    discovered_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                                    extracted_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                                    extraction_method="listings_article",
                                    language_detected=detect_language(article_content) if article_content else "unknown"
                                )
                                
                                # Log confidence
                                logger.debug(
                                    f"  Generated article item: {article_source_key} "
                                    f"(confidence: {article_confidence})"
                                )
                                
                                articles_for_this_listing.append(new_item)
                                new_listings_items.append(new_item)
                                
                            except (KeyError, ValueError) as e:
                                logger.warning(
                                    f"Failed to parse article {idx} from listings: {e}"
                                )
                                continue
                        
                        if not articles_for_this_listing:
                            logger.info(
                                f"No articles extracted from listings page {item.source_key} - likely a category/filter page"
                            )
                            reason = "No articles found in listings page (category/filter page)"
                            filtered_items.append((item.source_key, reason))
                        else:
                            logger.info(
                                f"Successfully generated {len(articles_for_this_listing)} items from listings page"
                            )
                            # Store mapping of articles to their parent listings page
                            listings_items_by_source[item.source_key] = articles_for_this_listing
                        
                        # Don't add the original listings item - we've converted it
                        continue
                        
                    except json.JSONDecodeError as e:
                        logger.error(
                            f"Failed to parse listings JSON for {item.source_key}: {e}"
                        )
                        failed_items.append(
                            (item.source_key, f"Invalid JSON in listings response: {e}")
                        )
                        continue
                
                # Normal extraction (non-listings)
                extracted_items.append(extracted)
            elif filter_status and ("No keywords matched" in filter_status or filter_status == "filtered_out"):
                # Keyword mismatch is a filter, not an error
                filtered_items.append((item.source_key, filter_status))
            elif filter_status and "too old" in filter_status.lower():
                filtered_items.append((item.source_key, filter_status))
            elif filter_status == "auth_required":
                logger.warning(
                    f"Auth required (HTTP 401) for {item.source_key}: skipped"
                )
                failed_items.append(
                    (item.source_key, filter_status)
                )
            else:  # extraction_failed
                logger.error(
                    f"Extraction failed for {item.source_key}: {filter_status}"
                )
                failed_items.append(
                    (
                        item.source_key,
                        filter_status or "Extraction failed for both primary and fallback methods",
                    )
                )
        
        # Process newly generated listings items - check if they should be filtered by age
        # Don't call extract() on them since they're already extracted from JSON
        from datetime import timedelta
        
        for listing_item in new_listings_items:
            # Check if article is too old (published more than article_max_age_days ago)
            if listing_item.published_at and listing_item.published_at != "unknown":
                try:
                    published_time = datetime.fromisoformat(
                        listing_item.published_at.replace('Z', '+00:00')
                    )
                    current_time = datetime.now(timezone.utc)
                    article_age = current_time - published_time
                    
                    if article_age > timedelta(days=self.article_max_age_days):
                        filter_reason = f"Article too old ({article_age.days}d {article_age.seconds//3600}h, max: {self.article_max_age_days}d)"
                        logger.debug(
                            f"Filtering out old article from listings: {listing_item.source_key} "
                            f"(age: {article_age.days}d {article_age.seconds//3600}h, max: {self.article_max_age_days}d)"
                        )
                        filtered_items.append((listing_item.source_key, filter_reason))
                    else:
                        # Article is recent enough
                        extracted_items.append(listing_item)
                except (ValueError, AttributeError) as e:
                    # Can't parse date, include the article
                    logger.debug(
                        f"Couldn't parse date for {listing_item.source_key}: {e}, including article"
                    )
                    extracted_items.append(listing_item)
            else:
                # No date available, include the article
                extracted_items.append(listing_item)
        
        # Now check if all articles from a listings page were filtered
        # If so, mark the parent listings item as filtered instead of failed
        for parent_source_key, articles in listings_items_by_source.items():
            # Count how many of these articles ended up in extracted_items
            passed_count = sum(
                1 for article in articles 
                if article.source_key in [item.source_key for item in extracted_items]
            )
            
            # If ALL articles from this listing were filtered (none passed), change parent to filtered
            if passed_count == 0 and len(articles) > 0:
                # Find and remove from failed_items if it's there
                failed_items = [
                    (key, msg) for key, msg in failed_items 
                    if key != parent_source_key
                ]
                
                # Get the filter reason from the first filtered article
                filter_reason = None
                for article in articles:
                    for source_key, reason in filtered_items:
                        if source_key == article.source_key:
                            filter_reason = reason
                            break
                    if filter_reason:
                        break
                
                if not filter_reason:
                    filter_reason = "All articles in listings page filtered out (too old)"
                
                logger.info(
                    f"All articles from listings page {parent_source_key} were filtered. "
                    f"Marking listings page as filtered: {filter_reason}"
                )
                filtered_items.append((parent_source_key, filter_reason))

        return extracted_items, failed_items, filtered_items
