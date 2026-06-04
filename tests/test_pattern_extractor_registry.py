from src.config import WebSource
from src.pattern_extractors import (
    InlineListingExtractor,
    ListingPageExtractor,
    get_extractor_for_source,
    get_inline_listing_extractor_for_url,
    get_listing_extractor_for_url,
)


class _TestListingExtractor(ListingPageExtractor):
    @classmethod
    def matches_url(cls, url: str) -> bool:
        return "registry-listing" in url

    def extract_articles(self, html_content: str, url: str) -> list[dict]:
        return []


class _TestInlineExtractor(InlineListingExtractor):
    @classmethod
    def matches_url(cls, url: str) -> bool:
        return "registry-inline" in url

    def extract_inline_candidates(self, html_content: str, url: str) -> list[dict]:
        return []


def test_article_pattern_registry_discovers_bfh_extractor():
    source = WebSource(
        url="https://www.bundesfinanzhof.de/de/entscheidungen/entscheidungen-online/",
        categories=["Tax"],
        pattern_extraction={"enabled": True},
    )

    extractor = get_extractor_for_source(source)

    assert extractor is not None
    assert extractor.__class__.__name__ == "BFHPatternExtractor"


def test_article_pattern_registry_respects_enabled_flag():
    source = WebSource(
        url="https://www.bundesfinanzhof.de/de/entscheidungen/entscheidungen-online/",
        categories=["Tax"],
        pattern_extraction={"enabled": False},
    )

    assert get_extractor_for_source(source) is None


def test_listing_pattern_registry_discovers_matching_extractor():
    extractor = get_listing_extractor_for_url("https://example.com/registry-listing")

    assert extractor is not None
    assert extractor.__class__.__name__ == "_TestListingExtractor"


def test_inline_pattern_registry_discovers_matching_extractor():
    extractor = get_inline_listing_extractor_for_url("https://example.com/registry-inline")

    assert extractor is not None
    assert extractor.__class__.__name__ == "_TestInlineExtractor"
