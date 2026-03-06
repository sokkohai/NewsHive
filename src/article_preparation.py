"""Stage 2.5: Article Text Preparation.

Ensures that only article-body-focused text (not navigation/boilerplate fragments)
is sent to Stage 3 (Unified Enrichment).
"""

import logging
import re
from typing import Literal

from .models import ContentItem
from .config import Configuration

logger = logging.getLogger(__name__)

# Preparation status constants
STATUS_PASS = "PASS"
STATUS_WARN = "WARN"
STATUS_FAIL = "FAIL"

class ArticlePreparer:
    """Deterministic local text preparation."""

    def __init__(self, config: Configuration):
        self.config = config
        # Load preparation config with defaults
        prep_config = config.article_text_preparation
        
        if prep_config is None:
             # Default if not present in config (feature enabled by default per spec)
             from .config import ArticlePreparationConfig
             prep_config = ArticlePreparationConfig()

        self.enabled = prep_config.enabled
        self.min_prepared_chars = prep_config.min_prepared_chars
        self.min_article_ratio = prep_config.min_article_ratio
        self.max_repeated_line_ratio = prep_config.max_repeated_line_ratio
        self.warn_margin = prep_config.warn_margin

    def prepare(self, item: ContentItem) -> tuple[bool, str | None]:
        """Run text preparation on content item.

        Returns:
            (success, filtered_reason)
            If success is True, item.content is updated in-place.
            If success is False, item should be filtered.
        """
        if not self.enabled:
            return True, None

        raw_content = item.content or ""
        if not raw_content.strip():
            logger.warning(f"Empty content for item {item.source_key}")
            return False, "article_text_empty"

        prepared_text, status, notes = self._process_text(raw_content)

        # Update item with preparation metadata (transient, mostly for logging)
        # We don't have explicit fields on ContentItem for these yet, checking DATA_MODEL.md...
        # The spec says "optional internal metadata". We can log it.
        
        if status == STATUS_FAIL:
            logger.info(f"Article preparation FAILED for {item.source_key}: {notes}")
            return False, "article_text_insufficient"

        if status == STATUS_WARN:
            logger.warning(f"Article preparation WARNING for {item.source_key}: {notes}")

        # Update content with clean version
        original_len = len(raw_content)
        new_len = len(prepared_text)
        logger.debug(f"Prepared text for {item.source_key}: {original_len} -> {new_len} chars ({status})")
        
        item.content = prepared_text
        return True, None

    def _process_text(self, text: str) -> tuple[str, str, str]:
        """Core deterministic logic: clean, block-score, select."""
        
        # 1. Normalize lines
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return "", STATUS_FAIL, "No content lines found"

        # 2. Score blocks (simple heuristic: generic boilerplate detection)
        # We want to keep "good" paragraphs.
        # Good = decent length, ends with punctuation, not a link list.
        
        kept_lines = []
        original_char_count = len(text)
        
        # Simple heuristic: navigation menus often have many short lines or links
        # Real articles have longer paragraphs.
        
        for line in lines:
            # Keep if it looks like a sentence or a header
            if len(line) > 60:
                kept_lines.append(line)
            elif line.endswith(".") or line.endswith("?") or line.endswith("!"):
                 kept_lines.append(line)
            # Short lines without punctuation (menu items) are dropped by default 
            # unless they look like headers (e.g. UPPERCASE or Title Case) - strict for now
            elif line.isupper() and len(line) > 10:
                kept_lines.append(line)

        prepared_text = "\n\n".join(kept_lines)
        prepared_len = len(prepared_text)

        # 3. Ratio check
        if original_char_count == 0:
             return "", STATUS_FAIL, "Empty original text"
             
        ratio = prepared_len / original_char_count

        notes = f"ratio={ratio:.2f}, chars={prepared_len}"

        if prepared_len < self.min_prepared_chars:
            return prepared_text, STATUS_FAIL, f"Too short ({prepared_len} < {self.min_prepared_chars})"

        if ratio < self.min_article_ratio:
            return prepared_text, STATUS_FAIL, f"Low ratio ({ratio:.2f} < {self.min_article_ratio})"

        # Check for warning zone
        if ratio < (self.min_article_ratio + self.warn_margin):
            return prepared_text, STATUS_WARN, notes

        return prepared_text, STATUS_PASS, notes
