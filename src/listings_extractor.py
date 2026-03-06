"""Listings Extractor - Extract multiple articles from listings pages.

Implements listings extraction as specified in specs/core/LISTINGS_EXTRACTION.md.
Handles pages with multiple article entries without individual article URLs.
"""

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup, NavigableString
from dateutil import parser as dateutil_parser
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


@dataclass
class ListingArticle:
    """Single article from a listings page."""

    # Primary fields
    title: str
    date: str
    content: str

    # Optional fields
    url: Optional[str] = None
    category: Optional[str] = None
    jurisdiction: Optional[str] = None
    image_url: Optional[str] = None
    author: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    raw_element_id: Optional[str] = None

    # Metadata
    source_page: str = ""
    extraction_method: str = "unknown"
    confidence: float = 0.8

    def __str__(self) -> str:
        return f"[{self.date}] {self.title}"


class ListingsExtractor:
    """Extracts multiple articles from listings pages.

    Detects pages containing multiple article entries and parses them
    into structured article metadata.
    """

    # Common article container selectors (in priority order)
    ARTICLE_SELECTORS = [
        ".article-panel",
        ".article-item",
        ".article-teaser",
        "[class*='article-item']",
        "[class*='article-teaser']",
        "[class*='article-card']",
        ".card[class*='article']",
        "[class*='news-item']",
        ".item[class*='article']",
        "[role='article']",
        ".post",
        ".entry",
        "article",
        "[class*='news']",        # Matches .news-item, .news-article, etc.
        "[class*='story']",       # Matches .story, .story-card, etc.
        "[class*='item']",        # Matches generic .item containers
        "[class*='card']",        # Matches .card and variants
        "li[class*='article']",   # List items with article class
        "li[class*='news']",      # List items with news class
        "li[class*='item']",      # List items with item class (common in listings)
        "div[class*='list'] > div",  # Direct children of list containers
        ".row[class*='item']",    # Row-based layouts with item class
    ]

    # Title selectors (in priority order)
    TITLE_SELECTORS = [
        ".title",
        "h2",
        "h3",
        "h4",
        "h5",
        "[class*='title']",
        "[class*='headline']",
        "[class*='heading']",
        "a[class*='title']",
        "a[class*='headline']",
        "strong",  # Fallback for bold text often used as titles
    ]

    # Date selectors (in priority order)
    DATE_SELECTORS = [
        ".date",
        "time",
        "[class*='date']",
        "[class*='published']",
        "[data-date]",
        "span[class*='date']",
        "small[class*='date']",
        "[class*='timestamp']",
        "[class*='meta']",        # Metadata might contain date
        "time[datetime]",
        "[data-published]",
        "span[class*='time']",
        ".info",  # Sometimes date is in "info" sections
        "[class*='author']",  # Author sections often contain dates
    ]

    # Content/summary selectors (in priority order)
    CONTENT_SELECTORS = [
        ".content",
        ".summary",
        ".description",
        "[class*='excerpt']",
        "[class*='body']",
        "[class*='desc']",
        "[class*='text']",
        "p",
        "div > p",  # Paragraph within div
    ]

    # Configuration
    CONFIG = {
        "min_articles": 3,
        "min_confidence": 0.55,
        "min_title_length": 5,
        "min_content_length": 10,  # Reduced from 20 to be more flexible
        "max_title_length": 500,
        "date_lookback_days": 730,
        "dedup_threshold": 0.95,
        "extract_images": True,
        "extract_urls": True,
    }

    def detect_listings_page(self, html_content: str, min_articles: int = None) -> bool:
        """Detect if HTML contains a listings page with multiple articles.

        Args:
            html_content: Raw HTML
            min_articles: Minimum article containers to consider listing

        Returns:
            True if page is identified as listings page
        """
        if min_articles is None:
            min_articles = self.CONFIG["min_articles"]

        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # Check for article page indicators
            # If explicit metadata says it's an article, trust it to avoid false positives
            og_type = soup.select_one("meta[property='og:type']")
            if og_type and og_type.get("content", "").lower() == "article":
                logger.info("Page has og:type='article', skipping listings detection")
                return False

            confidence = self._calculate_detection_confidence(soup)

            is_listing = confidence >= self.CONFIG["min_confidence"]
            article_count = self._count_article_containers(soup)

            if article_count < min_articles:
                return False

            logger.debug(
                f"Listings detection: {article_count} articles, confidence {confidence:.2f}"
            )
            return is_listing

        except Exception as e:
            logger.warning(f"Error detecting listings page: {e}")
            return False

    def parse_articles(self, html_content: str, url: str, max_age_days: int | None = None) -> list[ListingArticle]:
        """Parse all articles from a listings page.

        Args:
            html_content: Raw HTML
            url: Source URL (for metadata)
            max_age_days: Optional maximum age in days to stop extraction early

        Returns:
            List of structured articles with extracted fields
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            from datetime import datetime, timedelta, timezone
            
            # Calculate date cutoff if max_age_days is provided
            cutoff_date = None
            if max_age_days is not None:
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)
            
            pattern_info = self.detect_article_pattern(soup)

            if not pattern_info:
                logger.warning("Could not detect article pattern")
                return []

            articles = []
            article_elements = self._find_article_elements(
                soup, pattern_info["container_selector"]
            )

            logger.info(f"Found {len(article_elements)} article elements")

            for idx, element in enumerate(article_elements):
                try:
                    article = self._parse_article_item(element, url, pattern_info)
                    if article:
                        # Check for early exit if article is too old
                        if cutoff_date and article.date != "unknown":
                            try:
                                # Parse article.date (ISO format)
                                dt = datetime.fromisoformat(article.date.replace("Z", "+00:00"))
                                if dt < cutoff_date:
                                    logger.info(
                                        f"Encountered listing article older than {max_age_days} days ({article.date}). "
                                        "Stopping extraction on this page."
                                    )
                                    break
                            except Exception:
                                pass
                                
                        # Validate article
                        if self._validate_article(article):
                            articles.append(article)
                        else:
                            logger.debug(f"Article {idx} failed validation")
                except Exception as e:
                    logger.debug(f"Error parsing article {idx}: {e}")
                    continue

            # Deduplicate
            articles = self._deduplicate_articles(articles)

            # Sort by date (newest first)
            articles.sort(key=lambda a: a.date, reverse=True)

            logger.info(f"Successfully parsed {len(articles)} articles")
            return articles

        except Exception as e:
            logger.error(f"Error parsing articles: {e}")
            return []

    def extract_single_article(self, html_content: str, url: str, fragment: str) -> Optional[str]:
        """Extract content of a single article identified by fragment ID.

        Args:
            html_content: Raw HTML
            url: Source URL
            fragment: Element ID or identifier

        Returns:
            Article content string or None if not found
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            
            element = None
            
            # 1. Try to find element by ID exactly
            element = soup.find(id=fragment)

            # 2. Try to find element by onclick (Hogan Lovells specific)
            if not element:
                import re
                escaped_fragment = re.escape(fragment)
                # Look for onclick="showNTArticlePopup(fragment)"
                btn = soup.find(lambda tag: tag.name == "button" and tag.has_attr("onclick") and f"({fragment})" in tag["onclick"])
                if btn:
                    # The button usually is inside the article panel or is the toggle
                    # We want the container. For Hogan Lovells, it's .article-panel
                    element = btn.find_parent(class_="article-panel") or btn.find_parent(class_="item") or btn.parent

            if not element:
                logger.warning(f"Could not find article element for fragment: {fragment}")
                # Fallback: maybe the ID is part of a larger ID string?
                # e.g. fragment "918" in "cpBody_cpBody_rptrArticles_ucArticlePanel_0_pnlArticlePanel_0" NO, unlikely.
                return None

            # Extract fields from the found element using generic selectors
            # We don't have the "detected pattern" here, so we iterate all probable selectors
            title = self._extract_text_field(element, self.TITLE_SELECTORS)
            content = self._extract_text_field(element, self.CONTENT_SELECTORS)
            date_str = self._extract_text_field(element, self.DATE_SELECTORS)
            
            # Construct a readable article representation
            parts = []
            if title:
                parts.append(f"# {title}")
            if date_str:
                parts.append(f"**Date:** {date_str}")
            if content:
                parts.append(content)
            
            if not parts:
                # Fallback: just get all text
                return element.get_text(separator="\n", strip=True)
                
            return "\n\n".join(parts)

        except Exception as e:
            logger.error(f"Error extracting single article: {e}")
            return None

    def detect_article_pattern(self, soup: BeautifulSoup) -> Optional[dict[str, Any]]:
        """Detect HTML structure pattern for articles.

        Returns:
            Dict with pattern info or None if not detected
        """
        min_for_pattern = max(2, self.CONFIG["min_articles"] - 1)  # Relax threshold for pattern detection
        
        for container_selector in self.ARTICLE_SELECTORS:
            elements = soup.select(container_selector)

            if len(elements) < min_for_pattern:
                continue

            # Try to find consistent selectors in containers
            # Relax the requirement: 80% must have titles and dates (not 100%)
            sample_size = min(5, len(elements))
            has_titles_count = sum(
                1 for elem in elements[:sample_size]
                if self._find_element(elem, self.TITLE_SELECTORS)
            )
            has_dates_count = sum(
                1 for elem in elements[:sample_size]
                if self._find_element(elem, self.DATE_SELECTORS)
            )

            # At least 80% must have both title and date
            if has_titles_count >= sample_size * 0.8 and has_dates_count >= sample_size * 0.8:
                logger.info(
                    f"Detected pattern with selector '{container_selector}': "
                    f"{len(elements)} elements, {has_titles_count}/{sample_size} titles, "
                    f"{has_dates_count}/{sample_size} dates"
                )
                return {
                    "container_selector": container_selector,
                    "title_selector": None,  # Will be detected per element
                    "date_selector": None,
                    "content_selector": None,
                    "count": len(elements),
                    "pattern": self._detect_pattern_type(elements),
                    "confidence": 0.85,
                }

        return None

    def _calculate_detection_confidence(self, soup: BeautifulSoup) -> float:
        """Calculate confidence score for listings page detection."""
        score = 0.0

        # Check for article containers
        article_count = self._count_article_containers(soup)
        if article_count >= self.CONFIG["min_articles"]:
            score += 30

        # Check for consistent date fields
        article_elements = self._find_article_elements(soup)
        if article_elements:
            dates_found = sum(
                1
                for elem in article_elements[: min(5, len(article_elements))]
                if self._find_element(elem, self.DATE_SELECTORS)
            )
            if dates_found >= len(article_elements[: min(5, len(article_elements))]) * 0.8:
                score += 25

            # Check for titles
            titles_found = sum(
                1
                for elem in article_elements[: min(5, len(article_elements))]
                if self._find_element(elem, self.TITLE_SELECTORS)
            )
            if titles_found >= len(article_elements[: min(5, len(article_elements))]) * 0.8:
                score += 25

            # Check for content
            content_found = sum(
                1
                for elem in article_elements[: min(5, len(article_elements))]
                if self._find_element(elem, self.CONTENT_SELECTORS)
            )
            if content_found >= len(article_elements[: min(5, len(article_elements))]) * 0.6:
                score += 15

            # Check for structure consistency
            if self._check_structure_consistency(article_elements[:5]):
                score += 5

        return min(score, 100.0)

    def _count_article_containers(self, soup: BeautifulSoup) -> int:
        """Count article containers in page."""
        max_count = 0
        for selector in self.ARTICLE_SELECTORS:
            count = len(soup.select(selector))
            max_count = max(max_count, count)
        return max_count

    def _find_article_elements(
        self, soup: BeautifulSoup, selector: Optional[str] = None
    ) -> list:
        """Find article container elements."""
        if selector:
            return soup.select(selector)

        # Try selectors in order
        for selector in self.ARTICLE_SELECTORS:
            elements = soup.select(selector)
            if len(elements) >= self.CONFIG["min_articles"]:
                return elements

        return []

    def _parse_article_item(
        self, element, url: str, pattern_info: dict
    ) -> Optional[ListingArticle]:
        """Parse single article item from element."""
        try:
            # Extract fields
            title = self._extract_text_field(element, self.TITLE_SELECTORS)
            date_str = self._extract_text_field(element, self.DATE_SELECTORS)
            content = self._extract_text_field(element, self.CONTENT_SELECTORS)
            article_url = self._extract_url_field(element)
            category = self._extract_text_field(element, [".tag", ".type", ".badge"])
            image_url = self._extract_image_url(element)
            element_id = element.get("id", None)

            # Require at least title and date
            # Content can be empty for listings that only show titles
            if not title or not date_str:
                return None
            
            # If content is empty, try to use title as content (fallback)
            if not content:
                content = title

            # Normalize date
            normalized_date = self._normalize_date(date_str)
            if not normalized_date:
                logger.debug(f"Could not normalize date: {date_str}")
                normalized_date = datetime.now(timezone.utc).isoformat()

            article = ListingArticle(
                title=title.strip(),
                date=normalized_date,
                content=content.strip(),
                url=article_url,
                category=category,
                image_url=image_url,
                raw_element_id=element_id,
                source_page=url,
                extraction_method=pattern_info.get("pattern", "unknown"),
                confidence=pattern_info.get("confidence", 0.8),
            )

            return article

        except Exception as e:
            logger.debug(f"Error parsing article item: {e}")
            return None

    def _extract_text_field(
        self, element, selectors: list[str], max_length: int = None
    ) -> str:
        """Extract text content from element using selectors."""
        for selector in selectors:
            found = self._find_element(element, [selector])
            if found:
                text = found.get_text(strip=True)
                if text:
                    if max_length:
                        text = text[:max_length]
                    return text

        return ""

    def _extract_url_field(self, element) -> Optional[str]:
        """Extract URL from element."""
        if not self.CONFIG["extract_urls"]:
            return None

        # Try href attribute
        link = element.find("a", href=True)
        if link:
            return link["href"]

        # Try data-href
        if "data-href" in element.attrs:
            return element["data-href"]

        return None

    def _extract_image_url(self, element) -> Optional[str]:
        """Extract image URL from element."""
        if not self.CONFIG["extract_images"]:
            return None

        # Try img tag
        img = element.find("img", src=True)
        if img:
            return img["src"]

        # Try background-image style
        for elem in element.find_all():
            style = elem.get("style", "")
            match = re.search(r'background-image\s*:\s*url\(["\']?([^"\')\s]+)["\']?\)', style)
            if match:
                return match.group(1)

        return None

    def _find_element(self, element, selectors: list[str]):
        """Find element using list of selectors."""
        for selector in selectors:
            found = element.select_one(selector)
            if found:
                return found
        return None

    def _normalize_date(self, date_str: str) -> Optional[str]:
        """Convert date string to ISO 8601 UTC format using robust regex patterns."""
        if not date_str:
            return None

        date_str = date_str.strip()
        
        # German month mappings
        german_to_english = {
            'januar': 'january', 'februar': 'february', 'märz': 'march',
            'april': 'april', 'mai': 'may', 'juni': 'june',
            'juli': 'july', 'august': 'august', 'september': 'september',
            'oktober': 'october', 'november': 'november', 'dezember': 'december',
            'mär': 'march', 'dez': 'december', 'okt': 'october',
            'jan': 'january', 'feb': 'february', 'apr': 'april', 'jun': 'june',
            'jul': 'july', 'aug': 'august', 'sep': 'september', 'nov': 'november'
        }
        
        date_str_lower = date_str.lower()
        # Replace German months with English using word boundaries
        for de, en in german_to_english.items():
            date_str_lower = re.sub(r'\b' + de + r'\b', en, date_str_lower)

        # Month name to number mapping
        month_names = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12,
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }

        try:
            # Pattern 1: DD Month YYYY or D Month YYYY (e.g., "22 January 2026", "7 January 2026")
            day_month_year = re.search(
                r'(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})',
                date_str_lower, re.IGNORECASE
            )
            if day_month_year:
                day = int(day_month_year.group(1))
                month_str = day_month_year.group(2).lower()
                year = int(day_month_year.group(3))
                month = month_names.get(month_str, 0)
                if 1 <= month <= 12 and 1 <= day <= 31:
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    # Validate date is in reasonable range (allow some future dates for events)
                    now = datetime.now(timezone.utc)
                    days_old = (now - dt).days
                    if -365 <= days_old <= self.CONFIG["date_lookback_days"]:  # Allow up to 1 year in future
                        return dt.isoformat()
            
            # Pattern 2: Month DD, YYYY or Month D, YYYY (e.g., "January 8, 2026", "November 25, 2025")
            month_day_year = re.search(
                r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})',
                date_str_lower, re.IGNORECASE
            )
            if month_day_year:
                month_str = month_day_year.group(1).lower()
                day = int(month_day_year.group(2))
                year = int(month_day_year.group(3))
                month = month_names.get(month_str, 0)
                if 1 <= month <= 12 and 1 <= day <= 31:
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    # Validate date is in reasonable range (allow some future dates for events)
                    now = datetime.now(timezone.utc)
                    days_old = (now - dt).days
                    if -365 <= days_old <= self.CONFIG["date_lookback_days"]:  # Allow up to 1 year in future
                        return dt.isoformat()
            
            # Pattern 3: DD.MM.YYYY (e.g., "22.01.2026")
            ddmmyyyy = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
            if ddmmyyyy:
                day = int(ddmmyyyy.group(1))
                month = int(ddmmyyyy.group(2))
                year = int(ddmmyyyy.group(3))
                if 1 <= month <= 12 and 1 <= day <= 31:
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    days_old = (now - dt).days
                    if -365 <= days_old <= self.CONFIG["date_lookback_days"]:  # Allow up to 1 year in future
                        return dt.isoformat()
            
            # Pattern 4: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ (ISO format)
            iso_date = re.search(r'(\d{4})[-/](\d{2})[-/](\d{2})(?:[T ](\d{2}):(\d{2}):(\d{2})(.*))?', date_str)
            if iso_date:
                year = int(iso_date.group(1))
                month = int(iso_date.group(2))
                day = int(iso_date.group(3))
                hour = int(iso_date.group(4)) if iso_date.group(4) else 0
                minute = int(iso_date.group(5)) if iso_date.group(5) else 0
                second = int(iso_date.group(6)) if iso_date.group(6) else 0
                if 1 <= month <= 12 and 1 <= day <= 31:
                    dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    days_old = (now - dt).days
                    if -365 <= days_old <= self.CONFIG["date_lookback_days"]:  # Allow up to 1 year in future
                        return dt.isoformat()
            
            # Fallback: Try dateutil parser with dayfirst=True for German formats
            parsed = dateutil_parser.parse(date_str_lower, fuzzy=True, dayfirst=True)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            
            now = datetime.now(timezone.utc)
            days_old = (now - parsed).days
            if days_old < 0 or days_old > self.CONFIG["date_lookback_days"]:
                logger.debug(f"Date out of range: {date_str} ({days_old} days old)")
                return None

            return parsed.isoformat()
            
        except Exception as e:
            logger.debug(f"Error parsing date '{date_str}': {e}")
            return None

    def _validate_article(self, article: ListingArticle) -> bool:
        """Validate article meets minimum requirements."""
        # Check title
        if not article.title or len(article.title) < self.CONFIG["min_title_length"]:
            return False
        if len(article.title) > self.CONFIG["max_title_length"]:
            return False

        # Check date
        if not article.date:
            return False

        # Content is optional - some listings only show titles
        # But if content exists, check minimum length
        if article.content and len(article.content) < self.CONFIG["min_content_length"]:
            return False

        return True

    def _deduplicate_articles(self, articles: list[ListingArticle]) -> list[ListingArticle]:
        """Remove duplicate or near-duplicate articles."""
        unique_articles = []
        seen_signatures = set()

        for article in articles:
            # Create signature: date + title (first 30 chars)
            signature = f"{article.date}|{article.title[:30]}"

            # Check for near-duplicates
            is_duplicate = False
            for seen_sig in seen_signatures:
                similarity = SequenceMatcher(None, signature, seen_sig).ratio()
                if similarity > self.CONFIG["dedup_threshold"]:
                    is_duplicate = True
                    break

            if not is_duplicate:
                unique_articles.append(article)
                seen_signatures.add(signature)

        return unique_articles

    def _check_structure_consistency(self, elements: list) -> bool:
        """Check if elements have consistent HTML structure."""
        if len(elements) < 2:
            return True

        # Check if similar number of children
        child_counts = [len(list(elem.children)) for elem in elements]
        avg_children = sum(child_counts) / len(child_counts)

        # Allow 30% variance
        variance_ok = all(abs(count - avg_children) / avg_children < 0.3 for count in child_counts)
        return variance_ok

    def _detect_pattern_type(self, elements: list) -> str:
        """Detect pattern type (grid, carousel, list, etc.)."""
        if not elements:
            return "unknown"

        # Check if elements have display properties
        first_elem = elements[0]

        # Simple heuristic based on class names
        class_str = " ".join(first_elem.get("class", []))
        if "carousel" in class_str:
            return "carousel"
        if "grid" in class_str or "col" in class_str:
            return "grid"
        if "list" in class_str or "item" in class_str:
            return "list"

        return "generic"


# Singleton instance
_extractor = None


def get_listings_extractor() -> ListingsExtractor:
    """Get or create singleton ListingsExtractor instance."""
    global _extractor
    if _extractor is None:
        _extractor = ListingsExtractor()
    return _extractor
