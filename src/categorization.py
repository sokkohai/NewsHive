"""Categorization stage implementation.

Implements category assignment with keyword validation as specified in
specs/core/CATEGORIZATION.md.
"""

import logging

from .config import Configuration, WebSource
from .models import ContentItem

logger = logging.getLogger(__name__)


class Categorizer:
    """Assigns category tags to articles with keyword validation.

    Implements categorization as specified in specs/core/CATEGORIZATION.md.
    Categories are assigned based on the web_source configuration, and validated
    by matching article content against category keywords.
    """

    def __init__(self, config: Configuration):
        """Initialize the Categorizer.

        Args:
            config: Configuration object with web_sources and categories
        """
        self.config = config
        # Build a lookup map: URL -> WebSource for efficient lookup
        self.url_to_source: dict[str, WebSource] = {}
        for ws in config.web_sources:
            self.url_to_source[ws.url] = ws
            # Also map without trailing slash for flexibility
            if ws.url.endswith('/'):
                self.url_to_source[ws.url.rstrip('/')] = ws
            else:
                self.url_to_source[ws.url + '/'] = ws

    def _matches_keywords(self, text: str, keywords: list[str]) -> tuple[bool, list[str]]:
        """Check if text matches any of the keywords.

        Uses case-insensitive substring matching with OR logic:
        Text matches if it contains ANY keyword.

        Args:
            text: Text to check against keywords
            keywords: List of keywords to match

        Returns:
            Tuple of (matches, matched_keywords) where:
            - matches: True if text matches any keyword
            - matched_keywords: List of keywords that matched
        """
        if not text or not keywords:
            return False, []
        text_lower = text.lower()
        matched: list[str] = []
        for keyword in keywords:
            if keyword.lower() in text_lower:
                matched.append(keyword)
        return len(matched) > 0, matched

    def _find_source_for_item(self, item: ContentItem) -> WebSource | None:
        """Find the web source configuration for a content item.
        
        Handles various source_key formats including listings URLs.
        """
        # Extract URL from source_key
        if item.source_key.startswith("web:"):
            url = item.source_key[4:]
        elif item.source_key.startswith("listings:"):
            # Format: listings:https://example.com/page#1
            url = item.source_key[9:]
            # Remove fragment (#1, #2, etc.)
            if '#' in url:
                url = url.split('#')[0]
        else:
            return None

        # Try exact match first
        if url in self.url_to_source:
            return self.url_to_source[url]

        # Try matching by URL prefix (for article pages from listing sources)
        for source_url, source in self.url_to_source.items():
            if url.startswith(source_url.rstrip('/')):
                return source

        return None

    def categorize(self, item: ContentItem) -> list[str]:
        """Assign category tags to a content item and validate with keyword matching.

        Categories are assigned from the source's categories field, then validated
        by checking if the article content matches any category keyword.
        Items that don't match any keywords from their assigned categories will
        be filtered out during processing.
        
        Also tracks which keywords matched for the item.

        Args:
            item: ContentItem with content to validate

        Returns:
            List of category names that matched the article content
        """
        source = self._find_source_for_item(item)

        # Determine which categories to validate:
        # 1. Prefer the source config's categories (authoritative)
        # 2. Fall back to pre-assigned item.categories (set during Discovery)
        #    This handles RSS-discovered articles whose URLs don't share the
        #    listing page's URL prefix and therefore fail source lookup.
        if source and source.categories:
            category_names = source.categories
        elif item.categories:
            logger.debug(
                f"Source not found for {item.source_key} via URL lookup, "
                f"using pre-assigned categories from Discovery: {item.categories}"
            )
            category_names = item.categories
        else:
            logger.debug(f"Source not found for {item.source_key} and no pre-assigned categories")
            return []

        # Get content to match against
        content = item.content or ""
        if not content:
            # Fallback to title if no content
            content = item.title or ""

        matched_categories: list[str] = []
        all_matched_keywords: list[str] = []
        
        for category_name in category_names:
            category = self.config.get_category(category_name)
            if not category:
                logger.warning(f"Category '{category_name}' not found in config")
                continue

            matches, matched_keywords = self._matches_keywords(content, category.keywords)
            if matches:
                matched_categories.append(category_name)
                # Track all matched keywords across all categories
                all_matched_keywords.extend(matched_keywords)
                logger.debug(
                    f"Category '{category_name}' matched for {item.source_key} with keywords: {matched_keywords}"
                )
            else:
                logger.debug(
                    f"Category '{category_name}' did not match (checked {len(category.keywords)} keywords) for {item.source_key}"
                )

        # Store matched keywords in the item
        if all_matched_keywords:
            # Deduplicate while preserving order
            seen = set()
            item.keywords = []
            for kw in all_matched_keywords:
                if kw.lower() not in seen:
                    item.keywords.append(kw)
                    seen.add(kw.lower())
        else:
            item.keywords = None

        return matched_categories

    def process(
        self, items: list[ContentItem]
    ) -> tuple[list[ContentItem], list[tuple[str, str]]]:
        """Process multiple items and assign categories with keyword validation.

        Items receive categories from their source configuration, but are
        filtered out if their content doesn't match any category keywords.

        Args:
            items: List of ContentItems to categorize

        Returns:
            Tuple of (categorized_items, filtered_items):
            - categorized_items: Items with at least one matching category
            - filtered_items: Items where no category keywords matched
        """
        categorized_items: list[ContentItem] = []
        filtered_items: list[tuple[str, str]] = []

        for item in items:
            categories = self.categorize(item)
            
            if categories:
                # Item matched at least one category - include it
                item.categories = categories
                categorized_items.append(item)
                logger.debug(
                    f"Item {item.source_key} categorized: {categories}"
                )
            else:
                # No matching categories - filter out
                filtered_items.append(
                    (item.source_key, "No category keywords matched content")
                )
                logger.debug(
                    f"Item {item.source_key} filtered: no category keywords matched"
                )

        logger.info(
            f"Categorization complete: {len(categorized_items)} passed, "
            f"{len(filtered_items)} filtered"
        )

        return categorized_items, filtered_items
