"""Bundesfinanzhof (BFH) pattern extractor."""

import logging
import re

from bs4 import BeautifulSoup

from .base import PatternExtractor

logger = logging.getLogger(__name__)


class BFHPatternExtractor(PatternExtractor):
    """Extracts Bundesfinanzhof judgment patterns from HTML."""

    PATTERN = r"(Urteil|Beschluss)\s+vom\s+(\d{1,2}\.)\s+(\w+)\s+(\d{4}),\s+([A-Z]+\s+[RLC]+\s+\d+/\d{2})"

    @classmethod
    def matches_source(cls, source) -> bool:
        return "bundesfinanzhof" in source.url.lower()

    def extract(self, html_content: str) -> str | None:
        if not html_content:
            return None

        try:
            soup = BeautifulSoup(html_content, "html.parser")
            h1 = soup.find("h1")
            if not h1:
                logger.debug("No h1 element found in HTML")
                return None

            h1_text = h1.get_text(strip=True)
            match = re.search(self.PATTERN, h1_text)
            if match:
                pattern_text = match.group(0)
                logger.info("Extracted BFH pattern: %s", pattern_text)
                return pattern_text

            logger.debug("No BFH pattern found in h1 text: %s", h1_text)
            return None
        except Exception as exc:
            logger.error("Error extracting BFH pattern from HTML: %s", exc, exc_info=True)
            return None