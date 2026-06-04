"""Pattern extraction strategies for source-specific parsing."""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from pkgutil import iter_modules
from typing import TypeVar

from .base import InlineListingExtractor, ListingPageExtractor, PatternExtractor

TExtractor = TypeVar("TExtractor")

_BASE_MODULES = {"base"}
_EXTRACTOR_MODULES_LOADED = False


def _ensure_extractor_modules_loaded() -> None:
    global _EXTRACTOR_MODULES_LOADED
    if _EXTRACTOR_MODULES_LOADED:
        return

    for module_info in iter_modules(__path__):
        module_name = module_info.name
        if module_name in _BASE_MODULES or module_name.startswith("_"):
            continue
        import_module(f"{__name__}.{module_name}")

    for extractor_cls in _registered_extractor_classes():
        globals()[extractor_cls.__name__] = extractor_cls

    _EXTRACTOR_MODULES_LOADED = True


def _registered_extractor_classes() -> tuple[type[object], ...]:
    return (
        *PatternExtractor.registered_extractors(),
        *ListingPageExtractor.registered_extractors(),
        *InlineListingExtractor.registered_extractors(),
    )


def _get_matching_extractor(
    extractor_classes: tuple[type[TExtractor], ...],
    matcher: Callable[[type[TExtractor]], bool],
) -> TExtractor | None:
    for extractor_cls in extractor_classes:
        if matcher(extractor_cls):
            return extractor_cls()
    return None


def get_extractor_for_source(source):
    """Get the appropriate article pattern extractor for a source."""
    if not source.pattern_extraction or not source.pattern_extraction.get("enabled"):
        return None

    _ensure_extractor_modules_loaded()
    return _get_matching_extractor(
        PatternExtractor.registered_extractors(),
        lambda extractor_cls: extractor_cls.matches_source(source),
    )


def get_listing_extractor_for_url(url: str) -> ListingPageExtractor | None:
    """Get a specialized listing page extractor for a URL."""
    _ensure_extractor_modules_loaded()
    return _get_matching_extractor(
        ListingPageExtractor.registered_extractors(),
        lambda extractor_cls: extractor_cls.matches_url(url),
    )


def get_inline_listing_extractor_for_url(url: str) -> InlineListingExtractor | None:
    """Get an inline listing extractor for a URL."""
    _ensure_extractor_modules_loaded()
    return _get_matching_extractor(
        InlineListingExtractor.registered_extractors(),
        lambda extractor_cls: extractor_cls.matches_url(url),
    )


_ensure_extractor_modules_loaded()

__all__ = [
    "PatternExtractor",
    "ListingPageExtractor",
    "InlineListingExtractor",
    "get_extractor_for_source",
    "get_listing_extractor_for_url",
    "get_inline_listing_extractor_for_url",
]
__all__.extend(extractor_cls.__name__ for extractor_cls in _registered_extractor_classes())