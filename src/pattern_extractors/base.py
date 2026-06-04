"""Base classes for source-specific extractor registries."""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from typing import Any, ClassVar


class _RegistryMixin(ABC):
    """Common registry behavior for extractor base classes."""

    priority: ClassVar[int] = 100
    _registry: ClassVar[list[type[Any]]] = []

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not inspect.isabstract(cls) and cls not in cls._registry:
            cls._registry.append(cls)

    @classmethod
    def registered_extractors(cls) -> tuple[type[Any], ...]:
        return tuple(
            sorted(
                cls._registry,
                key=lambda extractor_cls: (
                    getattr(extractor_cls, "priority", 100),
                    extractor_cls.__module__,
                    extractor_cls.__name__,
                ),
            )
        )


class PatternExtractor(_RegistryMixin, ABC):
    """Abstract base class for article-level source-specific pattern extraction."""

    _registry: ClassVar[list[type["PatternExtractor"]]] = []

    @classmethod
    def matches_source(cls, source: Any) -> bool:
        return False

    @abstractmethod
    def extract(self, html_content: str) -> str | None:
        """Extract pattern from HTML content."""
        raise NotImplementedError


class ListingPageExtractor(_RegistryMixin, ABC):
    """Abstract base class for specialized listing page extraction."""

    _registry: ClassVar[list[type["ListingPageExtractor"]]] = []

    @classmethod
    def matches_url(cls, url: str) -> bool:
        return False

    @abstractmethod
    def extract_articles(self, html_content: str, url: str) -> list[dict[str, Any]]:
        """Extract structured articles from listing page HTML."""
        raise NotImplementedError


class InlineListingExtractor(_RegistryMixin, ABC):
    """Abstract base class for inline listing URL/title extraction."""

    _registry: ClassVar[list[type["InlineListingExtractor"]]] = []

    @classmethod
    def matches_url(cls, url: str) -> bool:
        return False

    @abstractmethod
    def extract_inline_candidates(self, html_content: str, url: str) -> list[dict[str, Any]]:
        """Extract inline listing candidates from source HTML."""
        raise NotImplementedError