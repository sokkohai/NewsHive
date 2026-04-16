"""Pattern extraction strategies for source-specific content parsing."""

from .base import PatternExtractor
from .bfh_extractor import BFHPatternExtractor

__all__ = ["PatternExtractor", "BFHPatternExtractor", "get_extractor_for_source"]


def get_extractor_for_source(source):
    """Get the appropriate pattern extractor for a source."""
    if not source.pattern_extraction or not source.pattern_extraction.get("enabled"):
        return None

    if "bundesfinanzhof" in source.url.lower():
        return BFHPatternExtractor()

    return None