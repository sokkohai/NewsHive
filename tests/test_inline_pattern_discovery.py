from types import SimpleNamespace
from unittest.mock import patch

from src.config import Category, Configuration, WebSource
from src.discovery import Discoverer


class _FakeInlineExtractor:
    @classmethod
    def matches_url(cls, url: str) -> bool:
        return "inline-source" in url

    def extract_inline_candidates(self, html_content: str, url: str) -> list[dict]:
        return [
            {
                "url": "#item-42",
                "title": "Inline Synthetic Article",
                "date": "2026-06-04T10:00:00Z",
            }
        ]


def test_discoverer_adds_inline_candidates_from_pattern_extractor():
    config = Configuration(
        pipeline_version="1.0",
        web_sources=[
            WebSource(
                url="https://example.com/inline-source",
                categories=["CCCI"],
                listings_type="inline",
                pattern_extraction={"enabled": True},
            )
        ],
        categories=[Category(name="CCCI", keywords=["test"])],
    )

    discoverer = Discoverer(config)

    with patch.object(discoverer.web_discoverer, "discover", return_value=[]):
        with patch("src.discovery.get_inline_listing_extractor_for_url", return_value=_FakeInlineExtractor()):
            with patch("src.discovery.requests.get") as mock_get:
                mock_get.return_value = SimpleNamespace(status_code=200, text="<html>inline</html>")

                candidates = discoverer.discover()

    assert len(candidates) == 1
    assert candidates[0].source_url.endswith("#item-42")
    assert candidates[0].title == "Inline Synthetic Article"
