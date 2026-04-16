"""Base class for pattern extractors."""

from abc import ABC, abstractmethod


class PatternExtractor(ABC):
    """Abstract base class for source-specific pattern extractors."""

    @abstractmethod
    def extract(self, html_content: str) -> str | None:
        """Extract pattern from HTML content."""
        raise NotImplementedError