"""Inline listing extractor for Hogan Lovells resource hubs."""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from .base import InlineListingExtractor


class HoganLovellsInlineListingExtractor(InlineListingExtractor):
    """Handle Hogan Lovells inline listings that use popup IDs."""

    URL_PATTERN = re.compile(
        r"^https?://digital-client-solutions\.hoganlovells\.com/resources/.+",
        re.IGNORECASE,
    )
    POPUP_PATTERN = re.compile(r"showNTArticlePopup\((\d+)\)", re.IGNORECASE)

    @classmethod
    def matches_url(cls, url: str) -> bool:
        return bool(cls.URL_PATTERN.match(url))

    def extract_inline_candidates(self, html_content: str, url: str) -> list[dict]:
        soup = BeautifulSoup(html_content, "html.parser")
        candidates: list[dict] = []

        for panel in soup.select(".article-panel, .item"):
            button = panel.find("button", onclick=True)
            if button is None:
                continue

            onclick = str(button.get("onclick", ""))
            match = self.POPUP_PATTERN.search(onclick)
            if not match:
                continue

            article_id = match.group(1)
            title_elem = panel.select_one(".title, h1, h2, h3, h4")
            title = title_elem.get_text(" ", strip=True) if title_elem else panel.get_text(" ", strip=True)
            title = title[:180].strip()
            if not title:
                title = f"Hogan Lovells Article {article_id}"

            date_elem = panel.select_one("time, .date")
            date_value = "unknown"
            if date_elem is not None:
                date_value = date_elem.get("datetime") or date_elem.get_text(" ", strip=True) or "unknown"

            candidates.append(
                {
                    "url": f"{url}#{article_id}",
                    "title": title,
                    "date": date_value,
                    "extraction_method": "hogan_lovells_inline",
                    "confidence": 0.9,
                }
            )

        return candidates
