"""Discovery stage implementation.

Implements content discovery from web and email sources as specified in
specs/core/DISCOVERY.md.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from bs4 import BeautifulSoup

from .config import Configuration, ConfigLoader
from .models import ContentItem

logger = logging.getLogger(__name__)


def _extract_page_level_date(soup: Any) -> str:
    """Extract publication date from page-level metadata as fallback.
    
    Checks page-wide sources for dates when article-level extraction fails:
    - Page-level JSON-LD structured data
    - Meta tags in document head
    - Open Graph metadata
    
    Args:
        soup: BeautifulSoup object of the entire page
        
    Returns:
        ISO 8601 timestamp string or "unknown"
    """
    try:
        # Check page-level JSON-LD (in head or body)
        json_ld_scripts = soup.find_all("script", type="application/ld+json")
        for script in json_ld_scripts:
            if script.string:
                try:
                    data = json.loads(script.string)
                    items = [data] if isinstance(data, dict) else data if isinstance(data, list) else []
                    for item in items:
                        if isinstance(item, dict):
                            for date_field in ["datePublished", "dateModified", "dateCreated"]:
                                if date_field in item:
                                    normalized = _normalize_discovered_date(item[date_field])
                                    if normalized != "unknown":
                                        logger.debug(f"Page-level date from JSON-LD {date_field}: {normalized}")
                                        return normalized
                except (json.JSONDecodeError, TypeError):
                    continue
        
        # Check meta tags in head
        head = soup.find("head")
        if head:
            # Open Graph and standard meta tags
            meta_properties = [
                "article:published_time", "article:modified_time",
                "og:published_time", "og:updated_time"
            ]
            for prop in meta_properties:
                meta = head.find("meta", attrs={"property": prop})
                if meta and meta.get("content"):
                    normalized = _normalize_discovered_date(meta["content"])
                    if normalized != "unknown":
                        logger.debug(f"Page-level date from meta property '{prop}': {normalized}")
                        return normalized
            
            # Standard meta name attributes
            meta_names = ["publish-date", "publishdate", "date", "publication-date", "DC.date"]
            for name in meta_names:
                meta = head.find("meta", attrs={"name": name})
                if meta and meta.get("content"):
                    normalized = _normalize_discovered_date(meta["content"])
                    if normalized != "unknown":
                        logger.debug(f"Page-level date from meta name '{name}': {normalized}")
                        return normalized
        
        return "unknown"
    except Exception as e:
        logger.debug(f"Error in page-level date extraction: {e}")
        return "unknown"


def _normalize_url_for_compare(url: str) -> str:
    """Normalize a URL for comparison by removing fragments, query params, and trailing slashes."""
    parts = urlsplit(url)
    normalized = urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))
    return normalized.lower()


def _upsert_article_candidate(
    articles_by_url: dict[str, tuple[str, str]],
    article_order: list[str],
    link_url: str,
    link_text: str,
    published_date: str,
) -> None:
    """Insert or update a discovered article candidate.

    If the URL already exists, prefer records that have a known date over
    unknown dates, and keep the longer non-empty title.
    """
    existing = articles_by_url.get(link_url)
    clean_title = link_text.strip()

    if existing is None:
        articles_by_url[link_url] = (clean_title, published_date)
        article_order.append(link_url)
        return

    existing_title, existing_date = existing

    best_date = existing_date
    if existing_date == "unknown" and published_date != "unknown":
        best_date = published_date

    best_title = existing_title
    if clean_title and len(clean_title) > len(existing_title):
        best_title = clean_title

    articles_by_url[link_url] = (best_title, best_date)


def _extract_link_context_date(
    a_tag: Any,
    date_extraction_pattern: Any,
    page_level_date: str,
) -> str:
    """Extract date by scanning link, nearby siblings, and parent containers."""
    published_date = extract_date_from_listing_element(a_tag, date_extraction_pattern)
    if published_date != "unknown":
        return published_date

    candidates: list[Any] = []

    for sibling in (a_tag.previous_sibling, a_tag.next_sibling):
        if getattr(sibling, "name", None):
            candidates.append(sibling)

    parent = a_tag.parent
    if parent is not None:
        candidates.append(parent)
        for sibling in (parent.previous_sibling, parent.next_sibling):
            if getattr(sibling, "name", None):
                candidates.append(sibling)

    parent = a_tag.parent
    for _ in range(5):
        if parent is None:
            break
        if parent.name in ["article", "li", "div", "td", "p", "span"]:
            candidates.append(parent)
        parent = parent.parent

    seen_ids: set[int] = set()
    for candidate in candidates:
        candidate_id = id(candidate)
        if candidate_id in seen_ids:
            continue
        seen_ids.add(candidate_id)
        normalized = extract_date_from_listing_element(candidate, date_extraction_pattern)
        if normalized != "unknown":
            return normalized

    if page_level_date != "unknown":
        return page_level_date

    return "unknown"


def _is_pagination_url(url: str) -> bool:
    """Heuristic to detect paginated listing URLs (page 2+, offsets)."""
    url_lower = url.lower()

    # Query-based pagination
    if any(token in url_lower for token in [
        "?page=", "&page=", "?offset=", "&offset=",
        "?start=", "&start=", "?p=", "&p=", "?page_num=", "&page_num=",
    ]):
        return True

    # Path-based pagination
    if "/page/" in url_lower or "/seite/" in url_lower:
        return True

    # Common short pagination paths like /p/2
    if re.search(r"/p/\d+", url_lower):
        return True

    return False


def extract_date_from_listing_element(article_element: Any, date_pattern: Any = None) -> str:
    """Try to extract publication date from a listing page article element.
    
    Searches for dates in common locations:
    - Custom patterns from configuration (if provided)
    - JSON-LD structured data (Schema.org)
    - Meta tags (Open Graph, article:published_time)
    - Data attributes (data-date, data-published)
    - time tags with datetime attribute
    - Schema.org datePublished/dateModified attributes
    - Common CSS classes (date, published, time, etc.)
    - Date patterns in text content
    
    Args:
        article_element: BeautifulSoup element containing the article link
        date_pattern: Optional DateExtractionPattern from config for this source
        
    Returns:
        ISO 8601 timestamp string or "unknown"
    """
    try:
        # Priority 0: Try JSON-LD structured data (most reliable)
        try:
            json_ld_scripts = article_element.find_all("script", type="application/ld+json")
            for script in json_ld_scripts:
                if script.string:
                    try:
                        data = json.loads(script.string)
                        # Handle both single objects and arrays
                        items = [data] if isinstance(data, dict) else data if isinstance(data, list) else []
                        for item in items:
                            if isinstance(item, dict):
                                # Check for datePublished
                                if "datePublished" in item:
                                    normalized = _normalize_discovered_date(item["datePublished"])
                                    if normalized != "unknown":
                                        logger.debug(f"Date extracted from JSON-LD datePublished: {normalized}")
                                        return normalized
                                # Check for dateModified as fallback
                                if "dateModified" in item:
                                    normalized = _normalize_discovered_date(item["dateModified"])
                                    if normalized != "unknown":
                                        logger.debug(f"Date extracted from JSON-LD dateModified: {normalized}")
                                        return normalized
                    except (json.JSONDecodeError, TypeError):
                        continue
        except Exception as e:
            logger.debug(f"Error extracting JSON-LD date: {e}")

        # Priority 0.5: Try meta tags (also very reliable)
        try:
            # Open Graph article:published_time
            meta_og = article_element.find("meta", attrs={"property": "article:published_time"})
            if meta_og and meta_og.get("content"):
                normalized = _normalize_discovered_date(meta_og["content"])
                if normalized != "unknown":
                    logger.debug(f"Date extracted from meta OG published_time: {normalized}")
                    return normalized
            
            # Standard meta name="publish-date" or similar
            for meta_name in ["publish-date", "publishdate", "date", "publication-date"]:
                meta_tag = article_element.find("meta", attrs={"name": meta_name})
                if meta_tag and meta_tag.get("content"):
                    normalized = _normalize_discovered_date(meta_tag["content"])
                    if normalized != "unknown":
                        logger.debug(f"Date extracted from meta tag '{meta_name}': {normalized}")
                        return normalized
        except Exception as e:
            logger.debug(f"Error extracting meta tag date: {e}")

        # Priority 0.7: Try data attributes
        try:
            for attr in ["data-date", "data-published", "data-publish-date", "data-time"]:
                elem = article_element.find(attrs={attr: True})
                if elem:
                    normalized = _normalize_discovered_date(elem[attr])
                    if normalized != "unknown":
                        logger.debug(f"Date extracted from attribute '{attr}': {normalized}")
                        return normalized
        except Exception as e:
            logger.debug(f"Error extracting data attribute date: {e}")

        # Priority 1: Try configured patterns first if provided
        if date_pattern is not None:
            # Try CSS selectors
            if date_pattern.css_selectors:
                for selector in date_pattern.css_selectors:
                    try:
                        elem = article_element.select_one(selector)
                        if elem:
                            # Try datetime attribute
                            if elem.get("datetime"):
                                normalized = _normalize_discovered_date(elem["datetime"], date_pattern.date_format)
                                if normalized != "unknown":
                                    logger.debug(f"Date extracted via CSS selector '{selector}': {normalized}")
                                    return normalized
                            # Try text content
                            text = elem.get_text(strip=True)
                            if text and len(text) < 50:
                                normalized = _normalize_discovered_date(text, date_pattern.date_format)
                                if normalized != "unknown":
                                    logger.debug(f"Date extracted via CSS selector '{selector}' text: {normalized}")
                                    return normalized
                            
                            # NEW: Try parent/sibling container text
                            # where date is adjacent to the link, not inside it
                            parent = elem.parent
                            if parent:
                                parent_text = parent.get_text(strip=True)
                                if parent_text and len(parent_text) < 300:  # Avoid overly large blocks
                                    for pattern in (date_pattern.regex_patterns or []):
                                        try:
                                            match = re.search(pattern, parent_text, re.IGNORECASE)
                                            if match:
                                                date_str = match.group(0)
                                                normalized = _normalize_discovered_date(date_str, date_pattern.date_format)
                                                if normalized != "unknown":
                                                    logger.debug(f"Date extracted via parent text from selector '{selector}': {normalized}")
                                                    return normalized
                                        except Exception as e:
                                            logger.debug(f"Error with regex on parent text: {e}")
                                            continue
                    except Exception as e:
                        logger.debug(f"Error with CSS selector '{selector}': {e}")
                        continue
            
            # Try regex patterns
            if date_pattern.regex_patterns:
                element_text = article_element.get_text()
                logger.debug(f"Checking regex on element text (len={len(element_text)}): {element_text[:100]}")
                for pattern in date_pattern.regex_patterns:
                    try:
                        match = re.search(pattern, element_text, re.IGNORECASE)
                        if match:
                            # Always use group(0) for the full match
                            date_str = match.group(0)
                            normalized = _normalize_discovered_date(date_str, date_pattern.date_format)
                            if normalized != "unknown":
                                logger.debug(f"Date extracted via regex pattern '{pattern}': {normalized}")
                                return normalized
                    except Exception as e:
                        logger.debug(f"Error with regex pattern '{pattern}': {e}")
                        continue
        
        # Priority 2: Generic methods (fallback)
        # Method 1: Look for <time> tags with datetime attribute
        time_tag = article_element.find("time", attrs={"datetime": True})
        if time_tag and time_tag.get("datetime"):
            date_str = time_tag["datetime"]
            normalized = _normalize_discovered_date(date_str)
            if normalized != "unknown":
                return normalized
        
        # Method 2: Look for Schema.org structured data in the element
        # Check for itemprop="datePublished" or itemprop="dateModified"
        date_prop = article_element.find(attrs={"itemprop": re.compile(r"date(Published|Modified)", re.I)})
        if date_prop:
            # Try datetime attribute first
            if date_prop.get("datetime"):
                normalized = _normalize_discovered_date(date_prop["datetime"])
                if normalized != "unknown":
                    return normalized
            # Try content attribute
            if date_prop.get("content"):
                normalized = _normalize_discovered_date(date_prop["content"])
                if normalized != "unknown":
                    return normalized
            # Try text content
            text = date_prop.get_text(strip=True)
            if text:
                normalized = _normalize_discovered_date(text)
                if normalized != "unknown":
                    return normalized
        
        # Method 3: Look for common date CSS classes
        date_classes = ["date", "published", "post-date", "article-date", "entry-date", "time", "pubdate"]
        for cls in date_classes:
            elem = article_element.find(class_=re.compile(cls, re.I))
            if elem:
                # Try datetime attribute
                if elem.get("datetime"):
                    normalized = _normalize_discovered_date(elem["datetime"])
                    if normalized != "unknown":
                        return normalized
                # Try text content
                text = elem.get_text(strip=True)
                if text and len(text) < 50:  # Avoid long text blocks
                    normalized = _normalize_discovered_date(text)
                    if normalized != "unknown":
                        return normalized
        
        # Method 4: Search for date patterns in URLs
        for link in article_element.find_all("a"):
            href = link.get("href", "")
            if href:
                # Look for YYYY/MM/DD or YYYY-MM-DD pattern in URL
                url_date_pattern = r"(\d{4}[-/]\d{2}[-/]\d{2})"
                match = re.search(url_date_pattern, href)
                if match:
                    date_str = match.group(1)
                    normalized = _normalize_discovered_date(date_str)
                    if normalized != "unknown":
                        return normalized
        
        # Method 5: Search for date patterns in the element's text
        element_text = article_element.get_text()
        date_patterns = [
            # ISO format: 2026-01-23 or 2026-01-23T10:00:00 or in URLs /2026/01/23/
            r"(\d{4}[-/]\d{2}[-/]\d{2}(?:[T ]\d{2}:\d{2}:\d{2})?)",
            # English format: DD Month YYYY (e.g., "22 January 2026", "7 January 2026")
            r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})",
            # English format: Month DD, YYYY or Month D, YYYY (e.g., "January 23, 2026", "January 23 2026")
            r"((?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})",
            # German format: 23.01.2026 or 23. Januar 2026 or 23. Jan 2026
            r"(\d{1,2}\.\s?(?:Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+\d{4})",
            r"(\d{1,2}\.\s?(?:Jan|Feb|Mär|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dez)\s+\d{4})",
            r"(\d{1,2}\.\s?\d{1,2}\.\s?\d{4})",
            # German shortened format: D Dez YYYY or DD Dez YYYY
            r"(\d{1,2}\s+(?:Jan|Feb|Mär|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dez)\s+\d{4})",
            # German shortened format: D Januar YYYY or DD Januar YYYY
            r"(\d{1,2}\s+(?:Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+\d{4})",
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, element_text, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                normalized = _normalize_discovered_date(date_str)
                if normalized != "unknown":
                    return normalized
        
        return "unknown"
    except Exception as e:
        logger.debug(f"Error extracting date from listing element: {e}")
        return "unknown"


def _extract_date_from_url(url: str) -> str:
    """Extract date from URL patterns.
    
    Supports patterns like:
    - /YYYYMMDD_ (e.g., /20250930_embargo)
    - /YYYY_MM_ (e.g., /2025_01_handreichung)
    - /YYYY/MM/DD/ (e.g., /2025/09/30/)
    - /YYYY-MM-DD/ (e.g., /2025-09-30/)
    
    Args:
        url: URL to extract date from
        
    Returns:
        ISO 8601 date string or "unknown"
    """
    try:
        # Pattern 1: YYYYMMDD_ format (BAFA style)
        # e.g., 20250930_embargo, 20260209_eve_anpassung
        match = re.search(r'/(\d{8})_', url)
        if match:
            date_str = match.group(1)
            year = int(date_str[0:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            if 1 <= month <= 12 and 1 <= day <= 31:
                try:
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    return dt.isoformat().replace("+00:00", "Z")
                except ValueError:
                    pass
        
        # Pattern 2: YYYY_MM_ format (alternative BAFA style)
        # e.g., 2025_01_handreichung, 2024_13_handreichung
        match = re.search(r'/(\d{4})_(\d{2})_', url)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            # Use 1st day of the month as default
            if 1 <= month <= 12:
                try:
                    dt = datetime(year, month, 1, tzinfo=timezone.utc)
                    return dt.isoformat().replace("+00:00", "Z")
                except ValueError:
                    pass
        
        # Pattern 3: /YYYY/MM/DD/ format
        match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', url)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                try:
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    return dt.isoformat().replace("+00:00", "Z")
                except ValueError:
                    pass
        
        # Pattern 4: /YYYY-MM-DD/ format
        match = re.search(r'/(\d{4})-(\d{2})-(\d{2})/', url)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                try:
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    return dt.isoformat().replace("+00:00", "Z")
                except ValueError:
                    pass
        
        return "unknown"
    except Exception:
        return "unknown"


def _normalize_discovered_date(date_str: str, date_format: str | None = None) -> str:
    """Normalize a date string from discovery to ISO 8601 format.
    
    Args:
        date_str: Date string to normalize
        date_format: Optional hint for date format (e.g., "DD.MM.YYYY", "MMMM D, YYYY")
        
    Returns:
        ISO 8601 timestamp or "unknown"
    """
    try:
        from dateutil import parser as dateutil_parser
        
        # Clean up the string
        date_str = date_str.strip()
        
        if not date_str:
            return "unknown"
        
        # Replace German month names with English
        # Only replace if the German month is a standalone word (not substring)
        german_months = {
            'januar': 'january', 'februar': 'february', 'märz': 'march',
            'april': 'april', 'mai': 'may', 'juni': 'june',
            'juli': 'july', 'august': 'august', 'september': 'september',
            'oktober': 'october', 'november': 'november', 'dezember': 'december',
            'mär': 'march', 'dez': 'december', 'okt': 'october',
            'jan': 'january', 'feb': 'february', 'apr': 'april', 'jun': 'june',
            'jul': 'july', 'aug': 'august', 'sep': 'september', 'nov': 'november'
        }
        date_str_lower = date_str.lower()
        # Use word boundaries to avoid replacing substrings
        for de, en in german_months.items():
            date_str_lower = re.sub(r'\b' + de + r'\b', en, date_str_lower)

        
        # Month name mappings for manual parsing
        month_names = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12,
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }
        
        # Try manual parsing for common patterns first (more reliable than dateutil for these)
        # Pattern 1: DD Month YYYY or D Month YYYY (e.g., "22 January 2026", "7 January 2026")
        # Also handles: "22 January 2026: Test Article" with optional trailing content
        day_month_year = re.search(r'(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})', date_str_lower, re.IGNORECASE)
        if day_month_year:
            day = int(day_month_year.group(1))
            month_str = day_month_year.group(2).lower()
            year = int(day_month_year.group(3))
            month = month_names.get(month_str, 0)
            if 1 <= month <= 12 and 1 <= day <= 31:
                try:
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    return dt.isoformat().replace("+00:00", "Z")
                except ValueError:
                    pass
        
        # Pattern 2: Month DD, YYYY or Month D, YYYY (e.g., "January 8, 2026")
        month_day_year = re.search(r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})', date_str_lower, re.IGNORECASE)
        if month_day_year:
            month_str = month_day_year.group(1).lower()
            day = int(month_day_year.group(2))
            year = int(month_day_year.group(3))
            month = month_names.get(month_str, 0)
            if 1 <= month <= 12 and 1 <= day <= 31:
                try:
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    return dt.isoformat().replace("+00:00", "Z")
                except ValueError:
                    pass
        
        # Pattern 3: DD.MM.YYYY (e.g., "22.01.2026")
        ddmmyyyy = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if ddmmyyyy:
            day = int(ddmmyyyy.group(1))
            month = int(ddmmyyyy.group(2))
            year = int(ddmmyyyy.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                try:
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    return dt.isoformat().replace("+00:00", "Z")
                except ValueError:
                    pass
        
        # Pattern 4: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ (ISO format) or in URLs /YYYY/MM/DD
        iso_date = re.search(r'(\d{4})[-/](\d{2})[-/](\d{2})(?:[T ](\d{2}):(\d{2}):(\d{2})(.*))?', date_str)
        if iso_date:
            year = int(iso_date.group(1))
            month = int(iso_date.group(2))
            day = int(iso_date.group(3))
            hour = int(iso_date.group(4)) if iso_date.group(4) else 0
            minute = int(iso_date.group(5)) if iso_date.group(5) else 0
            second = int(iso_date.group(6)) if iso_date.group(6) else 0
            try:
                dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
                return dt.isoformat().replace("+00:00", "Z")
            except ValueError:
                pass
        
        # Fallback: use dateutil parser with dayfirst for remaining cases
        dt = dateutil_parser.parse(date_str_lower, dayfirst=True, fuzzy=True)
        
        # Convert to UTC if no timezone
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        # Return ISO 8601 format
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return "unknown"


class WebDiscoverer:
    """Discovers articles from configured web sources.

    Implements web source discovery using LOCAL METHODS ONLY as specified in
    specs/core/IMPLEMENTATION_GUIDE.md Phase 2, with optional extraction rules from
    specs/core/LISTING_ANALYSIS_STRATEGY.md.
    
    NOTE: Firecrawl and Jina are NOT used in Discovery (30-40% API cost reduction).
    These external APIs remain available in Extraction stage as fallbacks.
    """

    def __init__(self) -> None:
        """Initialize the WebDiscoverer."""
        pass

    def _apply_extraction_rules(
        self,
        articles: list[tuple[str, str, str]],
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> list[tuple[str, str, str]]:
        """Filter articles based on extraction rules.

        Args:
            articles: List of (url, title, published_date) tuples to filter
            include_patterns: URL patterns that must be included
            exclude_patterns: URL patterns to exclude

        Returns:
            Filtered list of (url, title, published_date) tuples
        """
        def _matches_pattern(url: str, pattern: str) -> bool:
            if pattern.startswith("re:"):
                try:
                    return re.search(pattern[3:], url) is not None
                except re.error:
                    logger.debug("Invalid regex pattern in extraction rules: %s", pattern)
                    return False
            return pattern in url

        filtered = articles

        # Apply include patterns (if specified, only URLs matching are kept)
        if include_patterns:
            filtered = [
                (url, title, date) for url, title, date in filtered
                if any(_matches_pattern(url, pattern) for pattern in include_patterns)
            ]

        # Apply exclude patterns (remove matching URLs)
        if exclude_patterns:
            filtered = [
                (url, title, date) for url, title, date in filtered
                if not any(_matches_pattern(url, pattern) for pattern in exclude_patterns)
            ]

        return filtered

    def _discover_with_sitemap(
        self,
        url: str,
        sitemap_url: str | None = None,
        max_age_days: int | None = None,
    ) -> list[tuple[str, str, str]] | None:
        """Discover article URLs from an XML sitemap.

        Uses trafilatura.sitemaps.sitemap_search() which handles sitemap indexes,
        compressed sitemaps (.gz), and standard XML sitemaps automatically.

        Args:
            url: Source base URL (used for auto-discovery if sitemap_url is None)
            sitemap_url: Optional explicit sitemap URL; if None, trafilatura
                         probes robots.txt and common paths under url
            max_age_days: Optional maximum age of articles; filters by date
                          patterns found in the URL path (YYYY/MM/DD or YYYY-MM-DD)

        Returns:
            List of (article_url, title="", published_date="unknown") tuples,
            or None if no sitemap found or an error occurred.
        """
        try:
            from trafilatura import sitemaps as trafilatura_sitemaps
            from datetime import timedelta

            target = sitemap_url or url
            logger.debug(f"  Attempting sitemap discovery from: {target}")

            raw_urls = trafilatura_sitemaps.sitemap_search(target)

            if not raw_urls:
                logger.debug("  Sitemap returned no URLs")
                return None

            logger.debug(f"  Sitemap returned {len(raw_urls)} raw URLs")

            cutoff_date = None
            if max_age_days is not None:
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)

            articles: list[tuple[str, str, str]] = []
            skipped_old = 0

            for article_url in raw_urls:
                # Attempt to infer publication date from URL path
                published_date = "unknown"
                url_date_match = re.search(r"(20\d{2})[/-](\d{2})[/-](\d{2})", article_url)
                if url_date_match:
                    year, month, day = url_date_match.groups()
                    inferred_date = f"{year}-{month}-{day}T00:00:00Z"
                    try:
                        dt = datetime.fromisoformat(inferred_date.replace("Z", "+00:00"))
                        if cutoff_date and dt < cutoff_date:
                            skipped_old += 1
                            continue
                        published_date = inferred_date
                    except ValueError:
                        pass

                # Title is not available from standard sitemaps.
                # Leave empty; the Extraction stage fetches the article HTML
                # from which downstream enrichment derives the proper title.
                articles.append((article_url, "", published_date))

            logger.info(
                f"Sitemap discovery: {len(articles)} articles kept, {skipped_old} filtered as too old"
            )
            return articles if articles else None

        except ImportError:
            logger.warning("  trafilatura not available for sitemap discovery")
            return None
        except Exception as e:
            logger.warning(f"  Sitemap discovery failed for {url}: {e}")
            return None

    def _discover_with_rss_feed(
        self,
        url: str,
        max_age_days: int | None = None,
        rss_feed_url: str | None = None,
        rss_date_extraction: str = "both",
        prefilter_keywords: list[str] | None = None,
    ) -> tuple[list[tuple[str, str, str]] | None, bool, bool, str | None]:
        """Discover articles using RSS/Atom feed auto-discovery and parsing.
        
        This method:
        1. If rss_feed_url is provided, tries that first (highest priority)
        2. Checks if the URL itself is an RSS/Atom feed
        3. If not, tries to auto-discover feed links from the page
        4. Parses the feed and extracts articles with dates
        
        RSS/Atom feeds are highly reliable for date extraction (98-100% success).
        
        Args:
            url: URL of listing page or RSS feed
            max_age_days: Optional maximum age of articles in days
            rss_feed_url: Optional explicit RSS/Atom feed URL from config
            rss_date_extraction: How to extract dates: "feed_fields", "url_pattern", or "both" (default)
            prefilter_keywords: Optional list of keywords; when provided, entries whose title +
                RSS description contain none of the keywords are skipped before any HTTP fetch.
            
        Returns:
            Tuple containing:
            1. List of (article_url, article_title, published_date) tuples, or None if no feed found
            2. Boolean indicating if discovery stopped early due to age limit
            3. Boolean indicating if a valid feed was parsed
            4. Optional reason when a valid feed yielded 0 articles (e.g. "prefilter_keywords")
        """
        try:
            import feedparser
            import requests
            import random
            from urllib.parse import urlparse

            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ]
            
            headers = {
                "User-Agent": random.choice(user_agents),
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, text/html",
                "Accept-Encoding": "gzip, deflate",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
            
            feed_urls_to_try = []
            
            # Highest priority: explicit rss_feed_url from config
            if rss_feed_url:
                feed_urls_to_try.append(rss_feed_url)
            
            # Then try the URL itself as a feed
            feed_urls_to_try.append(url)
            
            # Try common RSS/Atom feed patterns
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            # Common feed paths
            common_feed_paths = [
                "/feed", "/rss", "/atom", "/feed.xml", "/rss.xml", "/atom.xml",
                "/feed/", "/rss/", "/atom/",
            ]
            
            for path in common_feed_paths:
                feed_urls_to_try.append(base_url + path)
            
            # Try to auto-discover feed links from the page using trafilatura.
            # Skip if an explicit rss_feed_url is already configured – avoids unnecessary
            # HTTP requests (and noisy 403 errors for sites that block crawlers).
            if not rss_feed_url:
                try:
                    from trafilatura import feeds as trafilatura_feeds
                    discovered = trafilatura_feeds.find_feed_urls(url)
                    for discovered_feed_url in discovered:
                        if discovered_feed_url not in feed_urls_to_try:
                            feed_urls_to_try.insert(1, discovered_feed_url)  # Higher priority than common paths
                except Exception as e:
                    logger.debug(f"  Failed to auto-discover feeds: {e}")
            
            # Calculate date cutoff if max_age_days is provided
            cutoff_date = None
            if max_age_days is not None:
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)
            
            # Try each potential feed URL
            for feed_url in feed_urls_to_try:
                try:
                    logger.debug(f"  Trying RSS/Atom feed: {feed_url}")
                    response = requests.get(feed_url, headers=headers, timeout=30)
                    
                    if response.status_code != 200:
                        continue
                    
                    # Parse the feed
                    feed = feedparser.parse(response.content)
                    
                    # Check if it's a valid feed
                    if not feed.entries or feed.bozo:
                        continue
                    
                    logger.info(f"  Found valid RSS/Atom feed with {len(feed.entries)} entries")
                    
                    articles: list[tuple[str, str, str]] = []
                    stopped_due_to_age = False
                    skipped_no_link = 0
                    skipped_short_title = 0
                    skipped_keyword_prefilter = 0
                    
                    for entry in feed.entries:
                        # Extract article URL
                        article_url = entry.get("link")
                        if not article_url:
                            skipped_no_link += 1
                            continue
                        
                        # Extract title
                        title = entry.get("title", "").strip()
                        if not title or len(title) < 8:
                            skipped_short_title += 1
                            continue

                        # Pre-filter: check title + RSS description against category keywords.
                        # This avoids queueing irrelevant entries for full HTTP extraction.
                        # Only active when prefilter_keywords are provided (discovery_method="rss").
                        if prefilter_keywords:
                            desc_raw = ""
                            for _desc_field in ("summary", "description"):
                                _val = entry.get(_desc_field, "")
                                if _val and isinstance(_val, str):
                                    desc_raw = _val
                                    break
                            desc_text = (
                                BeautifulSoup(desc_raw, "html.parser").get_text(" ", strip=True)
                                if desc_raw else ""
                            )
                            check_text = (title + " " + desc_text).lower()
                            kw_lower = [kw.lower() for kw in prefilter_keywords]
                            if not any(kw in check_text for kw in kw_lower):
                                skipped_keyword_prefilter += 1
                                logger.debug(
                                    f"  RSS pre-filter: skipped '{title[:70]}' "
                                    "(no keyword match in title/description)"
                                )
                                continue

                        # Extract date based on rss_date_extraction config
                        published_date = "unknown"
                        
                        # Strategy 1: Try feed date fields (if enabled)
                        if rss_date_extraction in ("feed_fields", "both"):
                            # Try various parsed date fields first
                            for date_field in ["published_parsed", "updated_parsed", "created_parsed"]:
                                if hasattr(entry, date_field):
                                    time_tuple = getattr(entry, date_field)
                                    if time_tuple:
                                        try:
                                            dt = datetime(*time_tuple[:6], tzinfo=timezone.utc)
                                            published_date = dt.isoformat().replace("+00:00", "Z")
                                            break
                                        except Exception:
                                            pass
                            
                            # Try string date fields if parsed dates failed
                            if published_date == "unknown":
                                for date_field in ["published", "updated", "created"]:
                                    if hasattr(entry, date_field):
                                        date_str = getattr(entry, date_field)
                                        if date_str:
                                            published_date = _normalize_discovered_date(date_str)
                                            if published_date != "unknown":
                                                break
                        
                        # Strategy 2: Try URL pattern extraction (if enabled and date not found yet)
                        if (rss_date_extraction in ("url_pattern", "both") and 
                            published_date == "unknown" and article_url):
                            date_from_url = _extract_date_from_url(article_url)
                            if date_from_url != "unknown":
                                published_date = date_from_url
                        
                        # Check for early exit if article is too old
                        if cutoff_date and published_date != "unknown":
                            try:
                                dt = datetime.fromisoformat(published_date.replace("Z", "+00:00"))
                                if dt < cutoff_date:
                                    logger.info(
                                        f"  Encountered article older than {max_age_days} days in feed. "
                                        "Stopping feed parsing."
                                    )
                                    stopped_due_to_age = True
                                    break
                            except Exception:
                                pass
                        
                        articles.append((article_url, title, published_date))
                    
                    # Always return once a valid feed has been processed.
                    # Even if articles is empty (e.g. all entries skipped due to
                    # short titles or missing links), there is no value in
                    # falling through to try the listing-page URL or common feed
                    # paths – those would just incur unnecessary HTTP requests
                    # (and potentially a 10-second timeout on the listing page).
                    dates_found = sum(1 for _, _, date in articles if date != "unknown")
                    if articles:
                        logger.info(
                            f"RSS/Atom feed discovery: {len(articles)} articles, "
                            f"{dates_found} with dates ({dates_found*100//len(articles) if articles else 0}%)"
                        )
                        return articles, stopped_due_to_age, True, None

                    empty_reason: str | None = None
                    if prefilter_keywords and skipped_keyword_prefilter:
                        empty_reason = "prefilter_keywords"
                        logger.info(
                            "  RSS/Atom feed parsed but all entries were filtered out by RSS keyword pre-filter "
                            "(derived from configured category keywords in config.yaml) "
                            f"({skipped_keyword_prefilter}/{len(feed.entries)} entries skipped; "
                            f"keywords={len(prefilter_keywords)})."
                        )
                    else:
                        logger.debug(
                            f"  RSS/Atom feed had {len(feed.entries)} entries but 0 articles "
                            f"passed internal filters (no link={skipped_no_link}, short title={skipped_short_title})"
                        )
                    return articles, stopped_due_to_age, True, empty_reason
                    
                except Exception as e:
                    logger.warning(f"  Failed to parse feed {feed_url}: {e}")
                    continue
            
            # No valid feed found
            logger.debug("  No valid RSS/Atom feed found")
            return None, False, False, None
            
        except ImportError:
            logger.debug("  feedparser not available for RSS/Atom discovery")
            return None, False, False, None
        except Exception as e:
            logger.debug(f"  RSS/Atom discovery failed: {e}")
            return None, False, False, None

    def _discover_with_firecrawl(
        self, url: str
    ) -> list[tuple[str, str, str]] | None:
        """Discover articles using Firecrawl API (fallback method).

        Args:
            url: URL of listing page to scan

        Returns:
            List of (article_url, article_title, published_date) tuples, or None if discovery
            fails. published_date is "unknown" (Firecrawl doesn't provide dates from listings)
        """
        if not self.firecrawl_api_key:
            return None

        try:
            import requests

            headers = {
                "Authorization": f"Bearer {self.firecrawl_api_key}",
                "Content-Type": "application/json",
            }
            # Request structured extraction of articles
            payload = {
                "url": url,
                "formats": ["extract"],
                "extract": {
                    "prompt": (
                        "Extract the main news articles or blog posts "
                        "from the listing. Ignore sidebar links, "
                        "navigation menus, categories, and footer links. "
                        "Only include actual articles shown in the main feed."
                    ),
                    "schema": {
                        "type": "object",
                        "properties": {
                            "articles": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "title": {"type": "string"},
                                        "url": {"type": "string"},
                                    },
                                    "required": ["title", "url"],
                                },
                            }
                        },
                        "required": ["articles"],
                    }
                },
            }

            response = requests.post(
                "https://api.firecrawl.dev/v1/scrape",
                json=payload,
                headers=headers,
                timeout=30,
            )

            if response.status_code == 200:
                data: Any = response.json()
                articles: list[tuple[str, str, str]] = []

                # Check for extracted article data
                if "data" in data and "extract" in data["data"]:
                    extract_data: Any = data["data"]["extract"]
                    if isinstance(extract_data, dict) and "articles" in extract_data:
                        for article in extract_data["articles"]:
                            if isinstance(article, dict):
                                title = article.get("title", "")
                                article_url = article.get("url", "")
                                if title and article_url:
                                    # Firecrawl doesn't provide dates from listings
                                    articles.append((article_url, title, "unknown"))

                return articles if articles else None
            return None
        except Exception:
            return None

    def _discover_with_jina(
        self, url: str
    ) -> list[tuple[str, str, str]] | None:
        """Discover articles using Jina AI Reader API (primary method).

        Args:
            url: URL of listing page to scan

        Returns:
            List of (article_url, article_title, published_date) tuples, or None if discovery
            fails. published_date is "unknown" (Jina doesn't provide dates from listings)
        """
        if not self.jina_api_key:
            return None

        try:
            import requests

            headers = {
                "Authorization": f"Bearer {self.jina_api_key}",
                "Accept": "application/json",
            }

            response = requests.get(
                f"https://r.jina.ai/{url}",
                headers=headers,
                timeout=30,
            )

            if response.status_code == 200:
                # Jina returns JSON with content in markdown format
                articles: list[tuple[str, str, str]] = []
                data: Any = response.json()

                # Jina response structure: {"code": 200, "data": {"content": "markdown...", ...}}
                content = ""
                if isinstance(data, dict) and "data" in data:
                    content_data = data["data"]
                    if isinstance(content_data, dict) and "content" in content_data:
                        content = content_data["content"]

                # Extract markdown links from content: [text](url)
                import re
                from urllib.parse import urljoin, urlparse

                pattern = r"\[([^\]]+)\]\(([^\)]+)\)"
                seen_urls = set()

                # Parse the base URL to resolve relative links
                parsed_base = urlparse(url)
                base_domain = f"{parsed_base.scheme}://{parsed_base.netloc}"

                for match in re.finditer(pattern, content):
                    link_text, link_url = match.groups()

                    # Resolve relative URLs to absolute URLs
                    if link_url.startswith("/"):
                        link_url = base_domain + link_url
                    elif link_url.startswith("."):
                        link_url = urljoin(url, link_url)
                    elif not link_url.startswith("http"):
                        # Might be a relative URL without leading /
                        link_url = urljoin(url, link_url)

                    # Skip if already seen
                    if link_url in seen_urls:
                        continue
                    seen_urls.add(link_url)

                    # Filter criteria for real articles:
                    # 1. Not empty and not navigation/anchor links
                    if not link_text or not link_url or link_url.startswith("#"):
                        continue

                    # 2. Skip very short text (likely navigation)
                    if len(link_text.strip()) < 5:
                        continue

                    # 3. Skip if URL is just the main domain or obvious navigation
                    if link_url.rstrip("/") == url.rstrip("/") or link_url.strip() == url:
                        continue

                    # 3b. Skip URLs that only end with / (navigation/category)
                    # But allow if they have content path before the trailing slash
                    if link_url.endswith("/") and not any(
                        content_marker in link_url.lower()
                        for content_marker in ["-", "ueberschrift",
                                               "artikel", "news", "blog"]
                    ):
                        # Allow URLs like /de/news/fachbeitraege/
                        # but skip other trailing slash URLs
                        path_parts = link_url.rstrip("/").split("/")
                        # Too few path components likely navigation
                        if len(path_parts) <= 4:
                            continue

                    # 3c. Skip pagination links (page 2+, offsets)
                    if _is_pagination_url(link_url):
                        continue

                    # 4. Skip if link text contains common navigation keywords
                    nav_keywords = [
                        "login",
                        "menu",
                        "search",
                        "contact",
                        "impressum",
                        "datenschutz",
                        "cookie",
                        "home",
                        "back",
                        "next",
                        "previous",
                        "skip",
                        "careers",
                        "jobs",
                        "right",
                        "left",
                        "top",
                        "down",
                        "up",
                        "share",
                        "download",
                        "subscribe",
                        "newsletter"]
                    if any(keyword in link_text.lower() for keyword in nav_keywords):
                        continue

                    # 5. Skip image files, media, and non-content URLs
                    media_extensions = [".jpg", ".jpeg", ".png",
                                        ".gif", ".pdf", ".mp3", ".mp4",
                                        ".webp"]
                    if any(link_url.lower().endswith(ext)
                           for ext in media_extensions):
                        continue

                    # 6. Skip category/tag/archive/listing pages
                    skip_patterns = [
                        "/category/", "/tag/", "/tags/", "/archive/",
                        "/archiv/", "/themen/", "/kategorien/",
                        "/kategorie/", "/kompetenzen/", "/topics/",
                        "/taxonomy/", "/search/", "/results/",
                        "/liste", "/list", "/all-", "/alle-",
                        "/index", "?view=", "?page=", "?offset=", "/services/",
                        "/leistung/", "/leistungen/", "/autor/",
                        "/author/", "/profile/", "/team/",
                        "/standorte/", "/locations/", "/karriere/",
                        "/career/"
                    ]
                    if any(pattern in link_url.lower()
                           for pattern in skip_patterns):
                        continue

                    # 7. Skip URLs that are image parameters (like ?t=a-s&...)
                    if "?t=a-s" in link_url or link_url.count("?") > 2:
                        continue

                    # 8. Prefer longer text (likely real articles)
                    if len(link_text.strip()) > 8:
                        # Jina doesn't provide dates from listings
                        articles.append((link_url, link_text.strip(), "unknown"))

                return articles if articles else None
            return None
        except Exception:
            return None

    def discover(
        self,
        url: str,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        date_extraction_pattern: Any = None,
        max_age_days: int | None = None,
        rss_feed_url: str | None = None,
        sitemap_url: str | None = None,
        discovery_method: str = "auto",
        rss_date_extraction: str = "both",
        prefilter_keywords: list[str] | None = None,
        browser_actions: list[dict[str, Any]] | None = None,
        item_selector: str | None = None,
    ) -> list[tuple[str, str, str]]:
        """Discover articles from a web source URL.


        When discovery_method is "auto" (default), tries methods in order:
        0. RSS/Atom feeds (98-100% date extraction success)
        1. Local direct (HTTP + BeautifulSoup) - supports date_extraction_pattern
        2. Browser (Playwright) - supports date_extraction_pattern, browser_actions

        When a specific method is set, only that method is used:
        - "rss": Only RSS/Atom feed discovery
        - "sitemap": Only XML sitemap discovery (via trafilatura)
        - "static": Only HTTP + BeautifulSoup (no JS rendering)
        - "browser" / "playwright": Only headless browser (Playwright)

        Optionally applies extraction rules to filter results.
        Attempts to extract publication dates from listing page metadata.

        Args:
            url: URL of listing page to scan
            include_patterns: Optional URL patterns that articles must match
            exclude_patterns: Optional URL patterns to exclude
            date_extraction_pattern: Optional DateExtractionPattern from config
            max_age_days: Optional maximum age of articles in days.
            rss_feed_url: Optional explicit RSS/Atom feed URL from config.
            sitemap_url: Optional explicit sitemap URL from config.
            discovery_method: Discovery method to use
                ("auto", "rss", "sitemap", "static", "browser", "playwright")
            browser_actions: Optional list of actions to perform in browser (click, wait, etc.)

        Returns:
            List of (article_url, article_title, published_date) tuples
            published_date is ISO 8601 string or "unknown"
        """
        method = discovery_method.lower()

        # --- Specific method: sitemap only ---
        if method == "sitemap":
            articles = self._discover_with_sitemap(url, sitemap_url, max_age_days)
            if articles:
                filtered = self._apply_extraction_rules(articles, include_patterns, exclude_patterns)
                if filtered:
                    logger.info(f"Sitemap discovery succeeded for {url}: {len(filtered)} articles")
                    return filtered
            logger.warning(f"Sitemap discovery failed or returned no results for {url}")
            return []

        # --- Specific method: RSS only ---
        if method == "rss":
            articles, stopped_due_to_age, had_valid_feed, empty_reason = self._discover_with_rss_feed(
                url,
                max_age_days,
                rss_feed_url=rss_feed_url,
                rss_date_extraction=rss_date_extraction,
                prefilter_keywords=prefilter_keywords,
            )
            if articles:
                filtered = self._apply_extraction_rules(articles, include_patterns, exclude_patterns)
                if filtered:
                    logger.info(f"RSS discovery succeeded for {url}")
                    return filtered
            if (not had_valid_feed) and (not stopped_due_to_age):
                logger.warning(f"RSS discovery failed for {url} (method=rss)")
            return []
        
        # --- Specific method: static only ---
        if method == "static":
            articles, stopped_due_to_age = self._discover_with_local(url, date_extraction_pattern, max_age_days)
            if articles:
                filtered = self._apply_extraction_rules(articles, include_patterns, exclude_patterns)
                if filtered:
                    return filtered
            if not stopped_due_to_age:
                logger.warning(f"Static discovery failed for {url} (method=static)")
            return []
        
        # --- Specific method: browser/playwright only ---
        if method in ("browser", "playwright"):
            articles = self._discover_with_browser(url, date_extraction_pattern, max_age_days, browser_actions=browser_actions, item_selector=item_selector)
            if articles:
                return self._apply_extraction_rules(articles, include_patterns, exclude_patterns)
            logger.warning(f"Browser discovery failed for {url} (method={method})")
            return []
        
        # --- Auto: full fallback chain (default) ---
        # PRIORITY 0: Try RSS/Atom feed discovery (most reliable for dates)
        articles, stopped_due_to_age, had_valid_feed, empty_reason = self._discover_with_rss_feed(
            url,
            max_age_days,
            rss_feed_url=rss_feed_url,
            rss_date_extraction=rss_date_extraction,
            prefilter_keywords=prefilter_keywords,
        )
        
        if articles:
            filtered = self._apply_extraction_rules(
                articles, include_patterns, exclude_patterns
            )
            if filtered:
                logger.info(f"RSS/Atom feed discovery succeeded for {url}")
                return filtered
            
            if stopped_due_to_age:
                logger.debug(f"RSS/Atom feed found items (filtered) and stopped due to age. Skipping other methods.")
                return []
        elif stopped_due_to_age:
            logger.debug(f"RSS/Atom feed stopped due to age limit. Skipping other methods.")
            return []
        
        # PRIORITY 1: Try local direct discovery (HTTP + BeautifulSoup)
        # Only if no browser actions are required (local fetch can't interact)
        if not browser_actions:
            articles, stopped_due_to_age = self._discover_with_local(url, date_extraction_pattern, max_age_days)
            
            if articles:
                filtered = self._apply_extraction_rules(
                    articles, include_patterns, exclude_patterns
                )
                if filtered:
                    return filtered
                    
                if stopped_due_to_age:
                    logger.debug(f"Local discovery found items (filtered) and stopped due to age. Skipping browser fallback.")
                    return []
                    
                logger.debug(f"Local discovery found items but rules filtered them all. Trying browser fallback...")
            elif stopped_due_to_age:
                logger.debug(f"Local discovery stopped due to age limit. Skipping browser fallback.")
                return []
        else:
            logger.debug("Skipping local discovery because browser_actions are configured.")

        # PRIORITY 2: Try browser crawling (Playwright)
        articles = self._discover_with_browser(url, date_extraction_pattern, max_age_days, browser_actions=browser_actions, item_selector=item_selector)
        if articles:
            return self._apply_extraction_rules(
                articles, include_patterns, exclude_patterns
            )


        logger.warning(f"Discovery failed for {url} (local methods exhausted)")
        return []

    def _discover_with_local(
        self, url: str, date_extraction_pattern: Any = None, max_age_days: int | None = None
    ) -> tuple[list[tuple[str, str, str]] | None, bool]:
        """Discover articles using local HTTP + BeautifulSoup (fallback).

        Cost-free fallback when Jina/Firecrawl APIs fail or have no credits.
        Also attempts to extract publication dates from listing elements.

        Args:
            url: URL of listing page to scan
            date_extraction_pattern: Optional DateExtractionPattern from config
            max_age_days: Optional maximum age of articles in days for early stopping

        Returns:
            Tuple containing:
            1. List of (article_url, article_title, published_date) tuples, or None if discovery failed
            2. Boolean indicating if discovery stopped early due to age limit
        """
        try:
            import random
            import requests
            from bs4 import BeautifulSoup
            from urllib.parse import urljoin, urlparse
            from datetime import datetime, timedelta, timezone

            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            ]

            headers = {
                "User-Agent": random.choice(user_agents),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5,de;q=0.3",
                "Accept-Encoding": "gzip, deflate",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }

            logger.debug(f"  Attempting local discovery for {url}")
            response = requests.get(url, headers=headers, timeout=15, allow_redirects=True)

            if response.status_code != 200:
                logger.debug(f"  Local discovery failed: HTTP {response.status_code}")
                return None, False

            soup = BeautifulSoup(response.text, "html.parser")
            articles_by_url: dict[str, tuple[str, str]] = {}
            article_order: list[str] = []

            parsed_base = urlparse(url)
            base_domain = f"{parsed_base.scheme}://{parsed_base.netloc}"
            
            # Calculate date cutoff if max_age_days is provided
            cutoff_date = None
            if max_age_days is not None:
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)

            stopped_due_to_age = False
            links_without_date = 0  # Track unique links that failed date extraction
            
            # Try to extract a page-level fallback date (for articles without individual dates)
            page_level_date = _extract_page_level_date(soup)
            if page_level_date != "unknown":
                logger.debug(f"Found page-level fallback date: {page_level_date}")

            for a_tag in soup.find_all("a", href=True):
                link_url = a_tag["href"]
                link_text = a_tag.get_text(strip=True)

                # Resolve relative URLs
                if link_url.startswith("/"):
                    link_url = base_domain + link_url
                elif not link_url.startswith("http"):
                    link_url = urljoin(url, link_url)

                # Skip pagination links (page 2+, offsets)
                if _is_pagination_url(link_url):
                    continue

                # Filter criteria
                if not link_text or len(link_text.strip()) < 8:
                    continue
                if link_url.startswith("#") or link_url == url:
                    continue

                # Skip media files
                media_ext = [".jpg", ".jpeg", ".png", ".gif", ".pdf", ".mp3", ".mp4", ".webp"]
                if any(link_url.lower().endswith(ext) for ext in media_ext):
                    continue

                # Skip nav/category pages
                skip_patterns = [
                    "/category/", "/tag/", "/tags/", "/archive/",
                    "/autor/", "/author/", "/search/", "/kontakt",
                    "/impressum", "/datenschutz", "/privacy", "/login",
                    "/register", "?page=", "?offset=", "/team/",
                ]
                if any(pattern in link_url.lower() for pattern in skip_patterns):
                    continue

                # Skip nav keywords in text
                nav_keywords = ["login", "menu", "search", "contact", "impressum",
                                "datenschutz", "cookie", "home"]
                if any(kw in link_text.lower() for kw in nav_keywords):
                    continue

                published_date = _extract_link_context_date(
                    a_tag, date_extraction_pattern, page_level_date
                )
                
                _upsert_article_candidate(
                    articles_by_url,
                    article_order,
                    link_url,
                    link_text,
                    published_date,
                )

                # Check for early exit if article is too old
                if cutoff_date and published_date != "unknown":
                    try:
                        # Parse published_date (ISO format from extract_date_from_listing_element)
                        dt = datetime.fromisoformat(published_date.replace("Z", "+00:00"))
                        if dt < cutoff_date:
                            logger.info(
                                f"  Encountered article older than {max_age_days} days ({published_date}). "
                                "Stopping discovery on this page."
                            )
                            # Assuming chronological order, subsequent articles are also too old
                            stopped_due_to_age = True
                            break
                    except Exception:
                        pass

            articles = [
                (article_url, articles_by_url[article_url][0], articles_by_url[article_url][1])
                for article_url in article_order
            ]
            links_without_date = sum(1 for _, _, date in articles if date == "unknown")

            # Log summary of discovery results
            logger.info(
                f"Local discovery found {len(articles)} articles ({links_without_date} without extractable dates)"
            )
            
            # If we stopped due to age, we consider it a success even if results are empty or filtered
            if stopped_due_to_age:
                return articles, True
                
            return (articles, False) if articles else (None, False)

        except Exception as e:
            logger.debug(f"  Local discovery failed: {e}")
            return None, False

    def _discover_with_browser(
        self, url: str, date_extraction_pattern: Any = None, max_age_days: int | None = None, browser_actions: list[dict[str, Any]] | None = None, item_selector: str | None = None
    ) -> list[tuple[str, str, str]] | None:
        """Discover articles using headless browser (Playwright).

        Final fallback when all other methods fail.
        Attempts to extract publication dates from listing elements.

        Args:
            url: URL of listing page to scan
            date_extraction_pattern: Optional config.DateExtractionPattern
            max_age_days: Optional maximum age of articles in days
            browser_actions: Optional list of actions to perform in browser
            item_selector: Optional CSS selector for items (if not using <a> tags)

        Returns:
            List of (article_url, article_title, published_date) tuples, or None if discovery
            fails. published_date is ISO 8601 string or "unknown"
        """
        try:
            from playwright.sync_api import sync_playwright
            from urllib.parse import urljoin, urlparse
        except ImportError:
            logger.debug("  Playwright not available for browser discovery")
            return None

        try:
            logger.debug(f"  Attempting browser discovery for {url}")

            try:
                with sync_playwright() as p:  # pragma: no cover
                    browser = None
# ... implementation ...                    

                    launch_errors = []

                    # Strategy 1: Default bundled chromium
                    try:
                        browser = p.chromium.launch(
                            headless=True,
                            args=["--no-sandbox", "--disable-setuid-sandbox"]
                        )
                    except Exception as e:
                        launch_errors.append(f"Bundled chromium: {e}")

                    # Strategy 2: Microsoft Edge (common on Windows fallback)
                    if not browser:
                        try:
                            # Only try Edge if finding bundled failed - likely Windows environment issue
                            logger.debug("  Browser: Bundled chromium failed, trying MS Edge...")
                            browser = p.chromium.launch(
                                channel="msedge",
                                headless=True,
                                args=["--no-sandbox", "--disable-setuid-sandbox"]
                            )
                        except Exception as e:
                            launch_errors.append(f"MS Edge: {e}")

                    # Strategy 3: Google Chrome (fallback)
                    if not browser:
                        try:
                            logger.debug("  Browser: MS Edge failed, trying Google Chrome...")
                            browser = p.chromium.launch(
                                channel="chrome",
                                headless=True,
                                args=["--no-sandbox", "--disable-setuid-sandbox"]
                            )
                        except Exception as e:
                            launch_errors.append(f"Google Chrome: {e}")

                    if not browser:
                        # Reraise with details if all failed
                        raise RuntimeError(f"All browser launch attempts failed: {'; '.join(launch_errors)}")

                    try:
                        context = browser.new_context(viewport={"width": 1920, "height": 1080})
                        page = context.new_page()
                        page.goto(url, timeout=30000, wait_until="domcontentloaded")
                        page.wait_for_timeout(2000)

                        # Execute browser actions if configured
                        if browser_actions:
                            logger.info(f"Executing {len(browser_actions)} browser actions for {url}")
                            for action in browser_actions:
                                action_type = action.get("type")
                                if action_type == "click_and_wait":
                                    selector = action.get("selector")
                                    timeout = action.get("timeout", 2000)
                                    if selector:
                                        try:
                                            logger.debug(f"  Action: Click '{selector}' and wait {timeout}ms")
                                            # Wait for selector to be visible first
                                            page.wait_for_selector(selector, state="visible", timeout=10000)
                                            # Click and wait for network/rendering
                                            page.click(selector)
                                            page.wait_for_timeout(timeout)
                                        except Exception as e:
                                            logger.warning(f"  Browser action failed ({selector}): {e}")
                                elif action_type == "select_option":
                                    selector = action.get("selector")
                                    value = action.get("value")
                                    label = action.get("label")
                                    timeout = action.get("timeout", 2000)
                                    if selector:
                                        try:
                                            logger.debug(f"  Action: Select option in '{selector}'")
                                            # Wait for selector to be attached (even if hidden)
                                            page.wait_for_selector(selector, state="attached", timeout=10000)
                                            # Use force=True to handle hidden/customized selects
                                            page.select_option(selector, value=value, label=label, force=True)
                                            page.wait_for_timeout(timeout)
                                        except Exception as e:
                                            logger.warning(f"  Browser action select failed ({selector}): {e}")
                                elif action_type == "wait":
                                    timeout = action.get("timeout", 1000)
                                    logger.debug(f"  Action: Wait {timeout}ms")
                                    page.wait_for_timeout(timeout)

                        # Get page HTML for date extraction
                        page_html = page.content()
                        soup = BeautifulSoup(page_html, "html.parser")
                        from datetime import datetime, timedelta, timezone

                        # Calculate date cutoff if max_age_days is provided
                        cutoff_date = None
                        if max_age_days is not None:
                            cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)

                        articles_by_url: dict[str, tuple[str, str]] = {}
                        article_order: list[str] = []

                        parsed_base = urlparse(url)
                        base_domain = f"{parsed_base.scheme}://{parsed_base.netloc}"
                        
                        # Try to extract a page-level fallback date
                        page_level_date = _extract_page_level_date(soup)
                        if page_level_date != "unknown":
                            logger.debug(f"Found page-level fallback date: {page_level_date}")

                        # Determine elements to process
                        if item_selector:
                            logger.debug(f"Using item_selector: {item_selector}")
                            elements_to_process = soup.select(item_selector)
                            logger.debug(f"Found {len(elements_to_process)} elements matching selector")
                        else:
                            elements_to_process = soup.find_all("a", href=True)

                        for element in elements_to_process:
                            link_url = ""
                            link_text = ""
                            date_context_element = element

                            if item_selector:
                                # Try to find a link inside the element
                                a_tag = element.find("a", href=True)
                                if a_tag:
                                    link_url = a_tag["href"]
                                    link_text = a_tag.get_text(strip=True)
                                else:
                                    # Handle cases like Hogan Lovells where link is button/JS
                                    # Extract title from element text or specific class
                                    title_elem = element.select_one(".title, h1, h2, h3, h4, .headline")
                                    if title_elem:
                                        link_text = title_elem.get_text(strip=True)
                                    else:
                                        link_text = element.get_text(strip=True)[:100]

                                    # Attempt to extract ID for synthetic URL (specific logic for Hogan)
                                    import re
                                    btn = element.find("button", onclick=True)
                                    if btn:
                                        match = re.search(r"showNTArticlePopup\((\d+)\)", btn["onclick"])
                                        if match:
                                            article_id = match.group(1)
                                            link_url = f"{url}#{article_id}"
                                    
                                    if not link_url and element.get("id"):
                                        link_url = f"{url}#{element['id']}"
                            else:
                                # Normal <a> tag processing
                                link_url = element["href"]
                                link_text = element.get_text(strip=True)

                            if not link_url:
                                continue

                            # Resolve relative URLs
                            if link_url.startswith("/"):
                                link_url = base_domain + link_url
                            elif not link_url.startswith("http"):
                                link_url = urljoin(url, link_url)

                            # Skip pagination links (page 2+, offsets)
                            if _is_pagination_url(link_url):
                                continue

                            # Filter criteria
                            if not link_text or len(link_text.strip()) < 8:
                                continue
                            if link_url.startswith("#") and not item_selector: 
                                continue
                            if link_url == url:
                                continue

                            media_ext = [".jpg", ".jpeg", ".png", ".gif", ".pdf", ".mp3", ".mp4", ".webp"]
                            if any(link_url.lower().endswith(ext) for ext in media_ext):
                                continue

                            skip_patterns = [
                                "/category/", "/tag/", "/archive/", "/autor/",
                                "/author/", "/search/", "/kontakt", "/impressum",
                                "/datenschutz", "/login", "?page=",
                                "?offset=", "/team/",
                            ]
                            if any(pattern in link_url.lower() for pattern in skip_patterns):
                                continue
                                
                            # Skip nav keywords in text
                            nav_keywords = ["login", "search", "contact", "impressum",
                                            "datenschutz", "cookie", "home"]
                            if any(kw in link_text.lower() for kw in nav_keywords):
                                continue

                            published_date = _extract_link_context_date(
                                date_context_element, date_extraction_pattern, page_level_date
                            )
                            
                            _upsert_article_candidate(
                                articles_by_url,
                                article_order,
                                link_url,
                                link_text,
                                published_date,
                            )

                            # Check for early exit if article is too old
                            if cutoff_date and published_date != "unknown":
                                try:
                                    # Parse published_date (ISO format)
                                    dt = datetime.fromisoformat(published_date.replace("Z", "+00:00"))
                                    if dt < cutoff_date:
                                        logger.info(
                                            f"  Encountered article older than {max_age_days} days ({published_date}). "
                                            "Stopping discovery on this page."
                                        )
                                        break
                                except Exception:
                                    pass

                        articles = [
                            (article_url, articles_by_url[article_url][0], articles_by_url[article_url][1])
                            for article_url in article_order
                        ]

                        logger.info(f"Browser discovery found {len(articles)} articles")
                        return articles if articles else None
                    finally:
                        browser.close()
            except OSError as e:
                # Handle Windows Group Policy and permission errors
                if hasattr(e, 'winerror'):
                    if e.winerror == 1260:
                        logger.warning(
                            f"  Browser discovery blocked by Windows Group Policy. "
                            f"This is a system-level restriction: {e}"
                        )
                    else:
                        logger.warning(f"  Browser discovery blocked by OS: {e}")
                else:
                    logger.warning(f"  Browser discovery OS error: {e}")
                return None


        except TimeoutError as e:
            logger.warning(f"  Browser discovery timeout after 30s for {url}: {e}")
            return None
        except OSError as e:
            # Handle Windows Group Policy and permission errors at the top level
            if hasattr(e, 'winerror'):
                if e.winerror == 1260:
                    logger.warning(
                        f"  Browser discovery blocked by Windows Group Policy. "
                        f"This is a system-level restriction: {e}"
                    )
                else:
                    logger.warning(f"  Browser discovery blocked by OS: {e}")
            else:
                logger.warning(f"  Browser discovery OS error: {e}")
            return None
        except Exception as e:
            logger.debug(
                f"  Browser discovery failed for {url}: {type(e).__name__}: {e}"
            )
            return None


class EmailDiscoverer:
    """Discovers content from configured Outlook folders.

    Treats emails as "newsletters" or listing pages: parses the email body
    to find links to articles, extracting each link as a separate 'web'
    candidate item. This enables proper deduplication against web sources.

    Implements email discovery as specified in specs/core/DISCOVERY.md.
    """

    def extract_links(
        self, html_content: str, base_url: str = ""
    ) -> list[tuple[str, str]]:
        """Extract links and their text from HTML content.

        Args:
            html_content: The HTML body of the email.
            base_url: Base URL for resolving relative links (optional).

        Returns:
            List of (url, text) tuples.
        """
        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html_content, "html.parser")
            links: list[tuple[str, str]] = []

            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)

                # Ensure href is a string
                url_str = str(href) if href else ""

                # Skip pagination links (page 2+, offsets)
                if _is_pagination_url(url_str):
                    continue

                # Filter criteria for real articles in emails:
                # 1. Minimum text length
                if len(text) <= 5:
                    continue

                # 2. Skip protocol-only links
                if url_str.startswith("mailto:") or url_str.startswith("javascript:"):
                    continue

                # 3. Skip image files, media, and non-content URLs
                media_extensions = [".jpg", ".jpeg", ".png",
                                    ".gif", ".pdf", ".mp3", ".mp4", ".webp"]
                if any(url_str.lower().endswith(ext) for ext in media_extensions):
                    continue

                # 4. Skip obvious category/tag/archive pages
                skip_patterns = ["/category/", "/tag/", "/tags/", "/archive/", "/archiv/",
                                 "/themen/", "/kategorien/", "/kompetenzen/", "/topics/",
                                 "/taxonomy/", "/search/", "/results/", "?page=", "?offset="]
                if any(pattern in url_str.lower() for pattern in skip_patterns):
                    continue

                # 5. Skip URLs that end with just / (navigation)
                if url_str.rstrip().endswith("/") and url_str.count("/") <= 3:
                    continue

                # 6. Skip image parameter URLs
                if "?t=a-s" in url_str or (url_str.count("?") > 2):
                    continue

                # 7. Skip very short URLs (usually navigation)
                if len(url_str) < 15:
                    continue

                links.append((url_str, text))

            return links
        except ImportError:
            # Fallback if bs4 not available (though it should be)
            import re

            fallback_links: list[tuple[str, str]] = []
            # Simple regex for href handling (very basic)
            pattern = re.compile(
                r'<a\s+(?:[^>]*?\s+)?href="([^"]*)"[^>]*>(.*?)</a>',
                re.IGNORECASE | re.DOTALL,
            )
            for match in pattern.finditer(html_content):
                url, text = match.groups()
                # Remove inner tags
                clean_text = re.sub(r"<[^>]+>", "", text).strip()

                # Apply same filters as BeautifulSoup path
                if len(clean_text) <= 5:
                    continue
                if url.startswith("mailto:") or url.startswith("javascript:"):
                    continue

                media_extensions = [".jpg", ".jpeg", ".png",
                                    ".gif", ".pdf", ".mp3", ".mp4", ".webp"]
                if any(url.lower().endswith(ext) for ext in media_extensions):
                    continue

                skip_patterns = ["/category/", "/tag/", "/tags/", "/archive/", "/archiv/",
                                 "/themen/", "/kategorien/", "/kompetenzen/", "/topics/",
                                 "/taxonomy/", "/search/", "/results/", "?page=", "?offset="]
                if any(p in url.lower() for p in skip_patterns):
                    continue

                if url.rstrip().endswith("/") and url.count("/") <= 3:
                    continue

                if "?t=a-s" in url or url.count("?") > 2:
                    continue

                if len(url) < 15:
                    continue

                fallback_links.append((url, clean_text))
            return fallback_links
        except Exception:
            return []

    def discover(
        self, email_body: str, email_subject: str, email_id: str, sender: str,
        email_folder_source: str = "", email_archive_folder: str | None = None,
        email_sent_date: str | None = None
    ) -> list[ContentItem]:
        """Discover articles from a single email by parsing its links.

        Per specs/core/EMAIL_ARCHIVAL.md, stores email archival metadata
        (email_id, folder_source, archive_folder) for later processing.

        Per specs/core/DISCOVERY.md, extracts publication date from email sent date.

        Args:
            email_body: HTML content of the email
            email_subject: Subject line of the email
            email_id: Unique ID of the email (O365 message object_id)
            sender: Sender address
            email_folder_source: Source folder path (e.g., "Inbox/Newsletters")
            email_archive_folder: Archive folder path if configured
            email_sent_date: ISO 8601 timestamp of when the email was sent

        Returns:
            List of candidate ContentItems (of type="web") found in the email
        """
        candidates: list[ContentItem] = []
        links = self.extract_links(email_body)

        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        for url, text in links:
            # Create a 'web' candidate item for each link
            # This allows it to be deduplicated against web sources
            source_key = url

            item = ContentItem(
                id=source_key,
                source_type="web",  # Treat as web content
                source_key=source_key,
                title=text,  # Use anchor text as title (cleaned at output time)
                summary="",
                content="",
                categories=[],  # Will be assigned during categorization
                published_at=email_sent_date or "unknown",  # Use email sent date, fallback to unknown
                discovered_at=now,
                extracted_at="",
                source_url=url,
                # Metadata to trace back to email
                email_subject=email_subject,
                email_sender=sender,
                # Email archival metadata (per specs/core/EMAIL_ARCHIVAL.md)
                email_id=email_id,
                email_folder_source=email_folder_source,
                email_archive_folder=email_archive_folder,
            )
            candidates.append(item)

        return candidates


class Discoverer:
    """Orchestrates content discovery from all configured sources.

    Implements discovery stage as specified in specs/core/DISCOVERY.md.
    """

    def __init__(
            self,
            config: Configuration,
            state_store_manager: Any = None) -> None:
        """Initialize the Discoverer.

        Args:
            config: Pipeline configuration (includes service toggles)
            state_store_manager: Optional StateStoreManager for State Store checks during discovery
        """
        self.config = config
        # Calculate keywords from categories
        unique_keywords: set[str] = set()
        for cat in config.categories:
            unique_keywords.update(cat.keywords)
        self.keywords = list(unique_keywords)
        
        self.state_store_manager = state_store_manager
        self.web_discoverer = WebDiscoverer()
        self.email_discoverer = EmailDiscoverer()

    def _should_skip_by_state_store(self, source_key: str) -> bool:
        """Check if item should be skipped based on State Store status.

        Per SPEC_CHANGES.md:
        - If source_key found with status "success" → Skip (already processed)
        - If source_key found with status "extraction_failed", "summarization_failed",
          or "categorization_failed" → Include (retry failed items)
        - If source_key not found → Include (new item)

        Args:
            source_key: The source_key to check

        Returns:
            True if the item should be skipped, False otherwise
        """
        if not self.state_store_manager:
            # No state store manager provided, don't skip
            return False

        record = self.state_store_manager.get_record(source_key)
        if record is None:
            # Not found in state store, don't skip
            return False

        # Skip only if status is "success"
        if record.status == "success":
            return True

        # Include for retry (extraction_failed, summarization_failed,
        # categorization_failed)
        return False

    def classify_url(self, url: str) -> str:
        """Classify if URL is likely a listings page.
        
        Phase 3: Pre-classify URLs to detect likely listings pages using heuristics.
        This can be used to log information or prioritize processing.
        
        Args:
            url: URL to classify
        
        Returns:
            One of:
            - "likely_listings": URL appears to be a listings page
            - "unknown": Cannot determine from URL patterns
        """
        if not url:
            return "unknown"
        
        url_lower = url.lower()
        
        # Heuristics for listings pages
        listings_indicators = [
            "/news", "/articles", "/blog", "/archive",
            "/alerts", "/listings", "/list", "/feed",
            "/resources", "/updates", "/announcements",
            "?page=", "?start=", "?page_num=", "?offset="
        ]
        
        for indicator in listings_indicators:
            if indicator in url_lower:
                logger.debug(
                    f"URL classified as likely_listings: {url} "
                    f"(matched indicator: {indicator})"
                )
                return "likely_listings"
        
        return "unknown"

    def _matches_keywords(self, text: str) -> bool:
        """Check if text matches any of the configured keywords.

        Uses case-insensitive substring matching with OR logic:
        Text matches if it contains ANY keyword.

        Args:
            text: Text to check against keywords

        Returns:
            True if text matches any keyword
        """
        text_lower = text.lower()
        for keyword in self.keywords:
            if keyword.lower() in text_lower:
                return True
        return False

    def discover(self, last_run_timestamp: str | None = None) -> list[ContentItem]:
        """Discover candidate items from all configured sources.

        For each web source, applies keyword filtering. If a web source has
        associated topics, items are tagged with those topics during discovery.
        
        Phase 3: Also logs listings page classification (inline vs linked articles).

        Args:
            last_run_timestamp: ISO 8601 timestamp of last successful run.
                                Used for incremental email discovery.

        Returns:
            List of candidate ContentItems from web and email sources
        """
        candidates: list[ContentItem] = []

        # Discover from web sources
        for web_source in self.config.web_sources:
            try:
                # Log listings type (inline or linked)
                article_type = (
                    "inline articles" if web_source.listings_type == "inline"
                    else "linked articles"
                )
                logger.info(
                    f"Processing listings page: {web_source.url} "
                    f"({article_type})"
                )
                
                # Extract extraction rules if provided
                include_patterns = None
                exclude_patterns = None
                if web_source.extraction_rules:
                    include_patterns = (
                        web_source.extraction_rules.include_patterns
                    )
                    exclude_patterns = (
                        web_source.extraction_rules.exclude_patterns
                    )

                # Extract date extraction pattern if provided
                date_pattern = web_source.date_extraction_pattern

                # Build keyword list for RSS pre-filter when discovery_method is "rss".
                # Uses the keywords of all categories assigned to this web source.
                rss_prefilter_keywords: list[str] | None = None
                if web_source.discovery_method == "rss":
                    source_cat_names = set(web_source.categories or [])
                    rss_prefilter_keywords = [
                        kw
                        for cat in self.config.categories
                        if cat.name in source_cat_names
                        for kw in cat.keywords
                    ] or None  # None if no keywords configured

                articles = self.web_discoverer.discover(
                    web_source.url,
                    include_patterns=include_patterns,
                    exclude_patterns=exclude_patterns,
                    date_extraction_pattern=date_pattern,
                    max_age_days=self.config.article_max_age_days,
                    rss_feed_url=web_source.rss_feed_url,
                    sitemap_url=web_source.sitemap_url,
                    discovery_method=web_source.discovery_method,
                    rss_date_extraction=web_source.rss_date_extraction,
                    prefilter_keywords=rss_prefilter_keywords,
                    browser_actions=web_source.browser_actions,
                    item_selector=web_source.item_selector,
                )
                for article_url, article_title, published_date in articles:
                    # Filter out the source URL itself — it's a hub/listing page, not an article
                    if (
                        web_source.listings_type != "inline"
                        and _normalize_url_for_compare(article_url) == _normalize_url_for_compare(web_source.url)
                    ):
                        logger.debug(f"  Skipping source URL itself: {article_url}")
                        continue

                    # Filter out sub-pages of the source URL that share the same path prefix
                    # (e.g., /resources/esg-litigation-guide/generate-report is a tool page, not an article)
                    source_path = urlsplit(web_source.url).path.rstrip("/")
                    article_path = urlsplit(article_url).path.rstrip("/")
                    if (
                        web_source.listings_type == "inline"
                        and article_path.startswith(source_path + "/")
                        and urlsplit(article_url).netloc == urlsplit(web_source.url).netloc
                    ):
                        logger.debug(f"  Skipping sub-page of source URL: {article_url}")
                        continue

                    # NEW: Apply freshness filtering using config.article_max_age_days (Step 3 in DISCOVERY.md)
                    if published_date and published_date != "unknown":
                        try:
                            from datetime import timedelta
                            published_time = datetime.fromisoformat(
                                published_date.replace('Z', '+00:00')
                            )
                            article_age = datetime.now(timezone.utc) - published_time
                            max_age_days = self.config.article_max_age_days
                            if article_age > timedelta(days=max_age_days):
                                logger.info(
                                    f"  Filtering out old article: {article_url} "
                                    f"(age: {article_age.days}d {article_age.seconds//3600}h, max: {max_age_days}d)"
                                )
                                continue
                        except (ValueError, AttributeError):
                            # If date is unparseable, treat as unknown and continue
                            logger.debug(
                                f"  Unparseable date, treating as unknown: {article_url} "
                                f"(published_at: {published_date})"
                            )
                            published_date = "unknown"

                    # Fallback: infer date from article URL itself if discovery could not find one
                    if published_date == "unknown":
                        # 1) Use configured regex patterns (if any) against the URL, honoring date_format
                        if date_pattern and getattr(date_pattern, "regex_patterns", None):
                            for pattern in date_pattern.regex_patterns:
                                try:
                                    match = re.search(pattern, article_url, re.IGNORECASE)
                                    if match:
                                        date_str = match.group(0)
                                        inferred_date = _normalize_discovered_date(date_str, date_pattern.date_format)
                                        if inferred_date != "unknown":
                                            try:
                                                from datetime import timedelta
                                                published_time = datetime.fromisoformat(
                                                    inferred_date.replace('Z', '+00:00')
                                                )
                                                article_age = datetime.now(timezone.utc) - published_time
                                                max_age_days = self.config.article_max_age_days
                                                if article_age > timedelta(days=max_age_days):
                                                    logger.info(
                                                        f"  Filtering out old article (from URL regex): {article_url} "
                                                        f"(age: {article_age.days}d {article_age.seconds//3600}h, max: {max_age_days}d)"
                                                    )
                                                    inferred_date = "unknown"
                                                    break
                                                published_date = inferred_date
                                                break
                                            except ValueError:
                                                inferred_date = "unknown"
                                except Exception:
                                    continue

                        # 2) Generic YYYY/MM/DD or YYYY-MM-DD in URL if still unknown
                        if published_date == "unknown":
                            url_date_match = re.search(r"(20\d{2})[/-](\d{2})[/-](\d{2})", article_url)
                            if url_date_match:
                                year, month, day = url_date_match.groups()
                                inferred_date = f"{year}-{month}-{day}T00:00:00Z"
                                try:
                                    from datetime import timedelta
                                    published_time = datetime.fromisoformat(
                                        inferred_date.replace('Z', '+00:00')
                                    )
                                    article_age = datetime.now(timezone.utc) - published_time
                                    max_age_days = self.config.article_max_age_days
                                    if article_age > timedelta(days=max_age_days):
                                        logger.info(
                                            f"  Filtering out old article (from URL date): {article_url} "
                                            f"(age: {article_age.days}d {article_age.seconds//3600}h, max: {max_age_days}d)"
                                        )
                                        continue
                                    # Keep the inferred date so downstream stages see it
                                    published_date = inferred_date
                                except ValueError:
                                    # If we cannot parse the inferred date, leave as unknown
                                    pass
                    
                    # Note: Keyword filtering now happens in Categorization stage
                    # after full content extraction (per CATEGORIZATION.md)

                    # Check State Store (Step 5)
                    source_key = f"web:{article_url}"
                    if self._should_skip_by_state_store(source_key):
                        logger.debug(f"  Skipping {source_key} (already processed)")
                        continue

                    # Pre-assign categories from source config
                    # These will be validated during Categorization stage
                    source_categories = web_source.categories or []

                    item = ContentItem(
                        id=article_url,
                        source_type="web",
                        source_key=source_key,
                        title=article_title,
                        summary="",  # Will be populated later
                        content="",  # Will be populated later
                        categories=source_categories,  # Pre-assigned from source config
                        published_at=published_date,  # Extracted from listing page, or "unknown"
                        discovered_at=datetime.now(timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z"),
                        extracted_at="",
                        source_url=article_url,
                    )
                    candidates.append(item)
            except Exception as e:
                logger.error(f"  Web discovery failed for {web_source.url}: {e}")

        # Discover from email sources (optional - only if configured)
        if self.config.email_folders:
            try:
                candidates.extend(self._discover_emails(last_run_timestamp))
            except ValueError as e:
                logger.warning(f"  Email discovery skipped: {e}")
            except Exception as e:
                logger.warning(f"  Email discovery failed: {e}")

        return candidates

    def _discover_emails(self, last_run_timestamp: str | None) -> list[ContentItem]:
        """Discover articles from configured Outlook folders.

        Args:
            last_run_timestamp: ISO 8601 timestamp for filtering new emails.

        Returns:
            List of candidate items found in emails.

        Raises:
            ValueError: If Azure/Outlook configuration is incomplete
        """
        email_candidates: list[ContentItem] = []

        try:
            from O365 import Account

            # Get Azure configuration
            try:
                client_id, client_secret, tenant_id, refresh_token = (
                    ConfigLoader.get_azure_config()
                )
            except ValueError as e:
                # Re-raise to be caught by outer try-except in discover()
                error_msg = (
                    "Azure/Outlook configuration incomplete "
                    "(required for email sources): "
                    f"{str(e)}"
                )
                raise ValueError(error_msg) from e

            # Initialize account
            # We use a custom token backend if refresh_token is provided via env
            credentials = (client_id, client_secret)
            account = Account(credentials, tenant_id=tenant_id)

            if refresh_token:
                # If refresh token is in env, manually inject it into the token backend
                # This avoids requiring the o365_token.txt file
                token = {
                    'refresh_token': refresh_token,
                    'access_token': None,  # Will be refreshed
                    'expires_at': 0  # Expired
                }
                account.connection.token_backend.save_token(token)

            if not account.is_authenticated:
                logger.error("  Outlook authentication failed. Skipping email discovery.")
                return []

            mailbox = account.mailbox()

            for email_folder_config in self.config.email_folders:
                folder_path = email_folder_config.folder_path
                archive_folder = email_folder_config.archive_folder

                try:
                    logger.info(f"  Scanning email folder: {folder_path}")
                    folder = self._resolve_folder(mailbox, folder_path)
                    if not folder:
                        logger.warning(f"  Could not find folder: {folder_path}")
                        continue

                    # Build query for date filtering
                    query = None
                    if last_run_timestamp:
                        # O365 library uses receivedDateTime for filtering
                        # Format: receivedDateTime ge 2023-01-01T00:00:00Z
                        query = folder.q().greater_equal('receivedDateTime', last_run_timestamp)

                    # Fetch emails
                    messages = folder.get_messages(
                        limit=100, query=query, download_attachments=False)

                    for message in messages:
                        # Extract email sent date
                        email_sent_date = "unknown"
                        if hasattr(message, 'sent') and message.sent:
                            try:
                                # O365 message.sent is a datetime object
                                email_sent_date = message.sent.isoformat().replace("+00:00", "Z")
                            except Exception:
                                email_sent_date = "unknown"
                        
                        # Discover links from email
                        items = self.email_discoverer.discover(
                            email_body=message.body,
                            email_subject=message.subject,
                            email_id=message.object_id,
                            sender=message.sender.address,
                            email_folder_source=folder_path,
                            email_archive_folder=archive_folder,
                            email_sent_date=email_sent_date,
                        )

                        # Add all discovered items to candidates (no keyword filtering in discovery)
                        # Per SPEC_CHANGES.md, keyword filtering happens in extraction stage
                        for item in items:
                            # Check State Store first
                            if self._should_skip_by_state_store(item.source_key):
                                logger.debug(f"  Skipping {item.source_key} (already processed)")
                                continue
                            email_candidates.append(item)

                except Exception as e:
                    logger.error(f"  Error processing folder {folder_path}: {e}")

        except ImportError:
            logger.error("  O365 library not installed. Skipping email discovery.")
        except Exception as e:
            logger.error(f"  Email discovery failed: {e}")

        return email_candidates

    def _resolve_folder(self, mailbox: Any, folder_path: str) -> Any:
        """Resolve a folder path (e.g., 'Inbox/Subfolder') to an O365 folder object.

        Args:
            mailbox: O365 mailbox object.
            folder_path: Path to the folder.

        Returns:
            O365 folder object or None.
        """
        parts = folder_path.strip('/').split('/')
        current_folder = None

        # Start with root folders
        for part in parts:
            if current_folder is None:
                current_folder = mailbox.get_folder(folder_name=part)
            else:
                current_folder = current_folder.get_folder(folder_name=part)

            if not current_folder:
                return None

        return current_folder
