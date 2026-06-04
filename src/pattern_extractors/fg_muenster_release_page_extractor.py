"""Pattern extractor for FG Muenster decision release pages."""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from ..date_utils import parse_date_to_utc
from .base import ListingPageExtractor


class FGMuensterReleasePageExtractor(ListingPageExtractor):
    """Expand dated FG Muenster release pages into individual decisions."""

    URL_PATTERN = re.compile(
        r"^https?://www\.fg-muenster\.nrw\.de/behoerde/presse/entscheidungen/\d{2}_\d{2}_\d{4}/index\.php$"
    )
    NRWE_URL_FRAGMENT = "nrwe.justiz.nrw.de/fgs/muenster/"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        return bool(cls.URL_PATTERN.match(url))

    def extract_articles(self, html_content: str, url: str) -> list[dict]:
        soup = BeautifulSoup(html_content, "html.parser")
        release_date = self._extract_release_date(soup, url)
        results: list[dict] = []

        for item in soup.find_all("li"):
            link = item.find("a", href=True)
            if link is None:
                continue

            href = str(link.get("href", "")).strip()
            if self.NRWE_URL_FRAGMENT not in href:
                continue

            title = link.get_text(" ", strip=True)
            title = re.sub(r"\(https?://[^)]+\)", "", title).strip()
            if not title:
                continue

            teaser = item.find("div", class_="teaser")
            content = teaser.get_text(" ", strip=True) if teaser else ""
            if not content:
                content = title

            category = None
            if "-" in content:
                category = content.split("-", 1)[0].strip() or None

            results.append(
                {
                    "title": title,
                    "date": release_date,
                    "content": content,
                    "url": href,
                    "category": category,
                    "extraction_method": "fg_muenster_release_page",
                    "confidence": 0.95,
                }
            )

        return self._deduplicate(results)

    def _extract_release_date(self, soup: BeautifulSoup, url: str) -> str:
        h1 = soup.find("h1")
        if h1:
            match = re.search(r"(\d{2}\.\d{2}\.\d{4})", h1.get_text(" ", strip=True))
            if match:
                normalized = self._normalize_date(match.group(1))
                if normalized != "unknown":
                    return normalized

        match = re.search(r"/(\d{2})_(\d{2})_(\d{4})/index\.php$", url)
        if match:
            day, month, year = match.groups()
            return self._normalize_date(f"{day}.{month}.{year}")

        return "unknown"

    def _normalize_date(self, raw: str) -> str:
        dt = parse_date_to_utc(raw, fallback_dayfirst=True)
        if dt is None:
            return "unknown"
        return dt.isoformat().replace("+00:00", "Z")

    def _deduplicate(self, articles: list[dict]) -> list[dict]:
        seen: set[str] = set()
        unique: list[dict] = []
        for article in articles:
            key = f"{article.get('url','')}|{article.get('title','')}"
            if key in seen:
                continue
            seen.add(key)
            unique.append(article)
        return unique
