import json
from types import SimpleNamespace
from unittest.mock import patch

from src.extraction import WebExtractor


class _FakeListingExtractor:
    @classmethod
    def matches_url(cls, url: str) -> bool:
        return "special-listing" in url

    def extract_articles(self, html_content: str, url: str) -> list[dict]:
        return [
            {
                "title": "Pattern Listing Article",
                "date": "2026-06-04T00:00:00Z",
                "content": "Extracted from specialized listing extractor",
                "url": f"{url}#item-1",
                "confidence": 0.95,
            }
        ]


def test_web_extractor_uses_pattern_listing_extractor_when_available():
    extractor = WebExtractor(config=None)

    with patch("src.extraction.get_listing_extractor_for_url", return_value=_FakeListingExtractor()):
        with patch("src.extraction.requests.get") as mock_get:
            mock_get.return_value = SimpleNamespace(status_code=200, text="<html>dummy</html>")

            result = extractor.extract("https://example.com/special-listing")

    assert result is not None
    content, method = result
    assert method == "listings"

    payload = json.loads(content)
    assert payload["count"] == 1
    assert payload["articles"][0]["title"] == "Pattern Listing Article"
