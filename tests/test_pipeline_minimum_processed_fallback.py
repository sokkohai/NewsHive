"""Tests for minimum processed fallback using top-scored filtered items."""

from types import SimpleNamespace
from typing import Any, cast

from src.models import ContentItem
from src.pipeline import Pipeline


def _mk_item(source_key: str, score: int | None) -> ContentItem:
    return ContentItem(
        id=source_key,
        source_type="web",
        source_key=source_key,
        title="title",
        summary="summary",
        content="content",
        categories=["CCCI"],
        published_at="unknown",
        discovered_at="",
        extracted_at="",
        relevance_score=score,
        relevance_level="Niedrig" if score is None or score < 5 else "Mittel",
    )


def test_fallback_promotes_top_scored_low_relevance_items() -> None:
    pipeline = Pipeline.__new__(Pipeline)

    kept = _mk_item("web:kept", 10)
    low_a = _mk_item("web:low-a", 1)
    low_b = _mk_item("web:low-b", 8)
    low_c = _mk_item("web:low-c", 5)

    enriched_items = [kept]
    extracted_items = [kept, low_a, low_b, low_c]
    enrichment_filtered = [
        ("web:low-a", "Niedrig"),
        ("web:low-b", "Niedrig"),
        ("web:low-c", "Niedrig"),
    ]

    promoted, remaining_filtered = pipeline._apply_minimum_processed_fallback(
        enriched_items,
        extracted_items,
        enrichment_filtered,
    )

    assert [item.source_key for item in promoted] == [
        "web:kept",
        "web:low-b",
        "web:low-c",
    ]
    assert remaining_filtered == [("web:low-a", "Niedrig")]


def test_fallback_not_applied_when_already_three_processed() -> None:
    pipeline = Pipeline.__new__(Pipeline)

    enriched_items = [
        _mk_item("web:one", 10),
        _mk_item("web:two", 9),
        _mk_item("web:three", 8),
    ]
    extracted_items = [*enriched_items, _mk_item("web:low", 4)]
    enrichment_filtered = [("web:low", "Niedrig")]

    promoted, remaining_filtered = pipeline._apply_minimum_processed_fallback(
        enriched_items,
        extracted_items,
        enrichment_filtered,
    )

    assert [item.source_key for item in promoted] == [
        "web:one",
        "web:two",
        "web:three",
    ]
    assert remaining_filtered == [("web:low", "Niedrig")]


def test_fallback_handles_missing_scores_and_tie_breaks_by_source_key() -> None:
    pipeline = Pipeline.__new__(Pipeline)

    kept = _mk_item("web:kept", 10)
    tie_b = _mk_item("web:tie-b", 4)
    tie_a = _mk_item("web:tie-a", 4)
    no_score = _mk_item("web:no-score", None)

    enriched_items = [kept]
    extracted_items = [kept, tie_b, tie_a, no_score]
    enrichment_filtered = [
        ("web:no-score", "Niedrig"),
        ("web:tie-b", "Niedrig"),
        ("web:tie-a", "Niedrig"),
    ]

    promoted, remaining_filtered = pipeline._apply_minimum_processed_fallback(
        enriched_items,
        extracted_items,
        enrichment_filtered,
    )

    assert [item.source_key for item in promoted] == [
        "web:kept",
        "web:tie-a",
        "web:tie-b",
    ]
    assert remaining_filtered == [("web:no-score", "Niedrig")]


def test_fallback_only_uses_low_relevance_and_deduplicates_candidates() -> None:
    pipeline = Pipeline.__new__(Pipeline)

    kept = _mk_item("web:kept", 9)
    low = _mk_item("web:low", 6)
    medium = _mk_item("web:medium", 7)

    enriched_items = [kept]
    extracted_items = [kept, low, medium]
    enrichment_filtered = [
        ("web:low", "Niedrig"),
        ("web:low", "Niedrig"),
        ("web:medium", "Mittel"),
    ]

    promoted, remaining_filtered = pipeline._apply_minimum_processed_fallback(
        enriched_items,
        extracted_items,
        enrichment_filtered,
    )

    assert [item.source_key for item in promoted] == ["web:kept", "web:low"]
    assert remaining_filtered == [("web:medium", "Mittel")]


def test_preparation_fallback_promotes_short_articles_when_below_minimum() -> None:
    pipeline = Pipeline.__new__(Pipeline)

    kept = _mk_item("web:kept", 9)
    short_long = _mk_item("web:short-long", 4)
    short_long.content = "x" * 550
    short_short = _mk_item("web:short-short", 3)
    short_short.content = "x" * 213

    prepared_items = [kept]
    preparation_failures: list[tuple[ContentItem, str | None]] = [
        (short_short, "article_text_insufficient"),
        (short_long, "article_text_insufficient"),
    ]

    promoted, remaining_failures = pipeline._apply_preparation_fallback(
        prepared_items,
        preparation_failures,
    )

    assert [item.source_key for item in promoted] == [
        "web:kept",
        "web:short-long",
        "web:short-short",
    ]
    assert remaining_failures == []


def test_preparation_fallback_ignores_empty_articles() -> None:
    pipeline = Pipeline.__new__(Pipeline)

    kept = _mk_item("web:kept", 9)
    empty = _mk_item("web:empty", 0)
    empty.content = "   "

    promoted, remaining_failures = pipeline._apply_preparation_fallback(
        [kept],
        [(empty, "article_text_insufficient")],
    )

    assert [item.source_key for item in promoted] == ["web:kept"]
    assert remaining_failures == [(empty, "article_text_insufficient")]


# ---------------------------------------------------------------------------
# Tests for _apply_output_caps (Stage 4.7)
# ---------------------------------------------------------------------------

def _mk_item_pa(
    source_key: str,
    score: int | None,
    practice_area: str = "Wirtschafts- und Steuerstrafrecht",
    category: str = "CCCI",
) -> ContentItem:
    return ContentItem(
        id=source_key,
        source_type="web",
        source_key=source_key,
        title="title",
        summary="summary",
        content="content",
        categories=[category],
        published_at="2026-03-17T08:00:00Z",
        discovered_at="",
        extracted_at="",
        relevance_score=score,
        relevance_level="Niedrig" if score is None or score < 5 else "Mittel",
        relevance_practice_area=practice_area,
    )


def _make_pipeline_with_caps(
    max_pa: int | None = None,
    max_total: int | None = None,
) -> Pipeline:
    pipeline = Pipeline.__new__(Pipeline)
    pipeline.config = cast(Any, SimpleNamespace(
        max_articles_per_practice_area=max_pa,
        max_articles_total=max_total,
    ))
    return pipeline


def test_output_caps_no_limits_returns_all() -> None:
    pipeline = _make_pipeline_with_caps()
    items = [_mk_item_pa(f"web:{i}", score=i) for i in range(20)]
    result = pipeline._apply_output_caps(items)
    assert len(result) == 20


def test_output_caps_per_pa_keeps_top_scored() -> None:
    """Per-PA cap keeps only the 2 highest-scored items per practice area."""
    pipeline = _make_pipeline_with_caps(max_pa=2)
    items = [
        _mk_item_pa("web:a", 15, "Wirtschafts- und Steuerstrafrecht"),
        _mk_item_pa("web:b", 12, "Wirtschafts- und Steuerstrafrecht"),
        _mk_item_pa("web:c", 10, "Wirtschafts- und Steuerstrafrecht"),  # dropped
        _mk_item_pa("web:d", 14, "Compliance & Regulatorik"),
        _mk_item_pa("web:e", 11, "Compliance & Regulatorik"),
        _mk_item_pa("web:f",  8, "Compliance & Regulatorik"),  # dropped
    ]
    result = pipeline._apply_output_caps(items)
    keys = {item.source_key for item in result}
    assert keys == {"web:a", "web:b", "web:d", "web:e"}


def test_output_caps_per_pa_tie_break_by_source_key() -> None:
    """Equal scores are broken by source_key ascending."""
    pipeline = _make_pipeline_with_caps(max_pa=2)
    items = [
        _mk_item_pa("web:z", 10, "Wirtschafts- und Steuerstrafrecht"),
        _mk_item_pa("web:a", 10, "Wirtschafts- und Steuerstrafrecht"),
        _mk_item_pa("web:m", 10, "Wirtschafts- und Steuerstrafrecht"),
    ]
    result = pipeline._apply_output_caps(items)
    keys = {item.source_key for item in result}
    # Tie-break: a < m < z → keep a and m
    assert keys == {"web:a", "web:m"}


def test_output_caps_global_total_keeps_top_scored() -> None:
    """Global cap keeps only the 3 highest-scored items across all practice areas."""
    pipeline = _make_pipeline_with_caps(max_total=3)
    items = [
        _mk_item_pa("web:h1", 15, "Wirtschafts- und Steuerstrafrecht"),
        _mk_item_pa("web:h2", 14, "Compliance & Regulatorik"),
        _mk_item_pa("web:h3", 13, "Interne Untersuchungen"),
        _mk_item_pa("web:l1", 10, "Wirtschafts- und Steuerstrafrecht"),
        _mk_item_pa("web:l2",  8, "Compliance & Regulatorik"),
    ]
    result = pipeline._apply_output_caps(items)
    keys = {item.source_key for item in result}
    assert keys == {"web:h1", "web:h2", "web:h3"}


def test_output_caps_combination_pa_then_global() -> None:
    """PA cap runs first; global cap applies to survivors."""
    # PA cap = 2 per area, global cap = 3 total
    pipeline = _make_pipeline_with_caps(max_pa=2, max_total=3)
    items = [
        _mk_item_pa("web:a1", 15, "Wirtschafts- und Steuerstrafrecht"),
        _mk_item_pa("web:a2", 12, "Wirtschafts- und Steuerstrafrecht"),
        _mk_item_pa("web:a3",  9, "Wirtschafts- und Steuerstrafrecht"),  # dropped by PA cap
        _mk_item_pa("web:b1", 14, "Compliance & Regulatorik"),
        _mk_item_pa("web:b2", 11, "Compliance & Regulatorik"),
        _mk_item_pa("web:b3",  7, "Compliance & Regulatorik"),  # dropped by PA cap
    ]
    # After PA cap: a1(15), a2(12), b1(14), b2(11) → 4 items
    # After global cap(3): a1(15), b1(14), a2(12) → 3 items
    result = pipeline._apply_output_caps(items)
    keys = {item.source_key for item in result}
    assert keys == {"web:a1", "web:b1", "web:a2"}


def test_output_caps_none_score_treated_as_lowest() -> None:
    """Items with None score sort below all numeric scores."""
    pipeline = _make_pipeline_with_caps(max_pa=2)
    items = [
        _mk_item_pa("web:high", 10, "Wirtschafts- und Steuerstrafrecht"),
        _mk_item_pa("web:mid",   5, "Wirtschafts- und Steuerstrafrecht"),
        _mk_item_pa("web:none", None, "Wirtschafts- und Steuerstrafrecht"),
    ]
    result = pipeline._apply_output_caps(items)
    keys = {item.source_key for item in result}
    assert keys == {"web:high", "web:mid"}


def test_output_caps_below_limit_returns_all() -> None:
    """When total items < cap, all items are returned."""
    pipeline = _make_pipeline_with_caps(max_pa=5, max_total=15)
    items = [_mk_item_pa(f"web:{i}", score=i) for i in range(3)]
    result = pipeline._apply_output_caps(items)
    assert len(result) == 3


def test_output_caps_multiple_categories_independent_pa_caps() -> None:
    """PA cap applies independently per (category, practice_area) pair."""
    pipeline = _make_pipeline_with_caps(max_pa=1)
    items = [
        _mk_item_pa("web:ccci-1", 15, "Wirtschafts- und Steuerstrafrecht", "CCCI"),
        _mk_item_pa("web:ccci-2", 10, "Wirtschafts- und Steuerstrafrecht", "CCCI"),  # dropped
        _mk_item_pa("web:esg-1",  14, "Wirtschafts- und Steuerstrafrecht", "ESG"),
        _mk_item_pa("web:esg-2",   9, "Wirtschafts- und Steuerstrafrecht", "ESG"),  # dropped
    ]
    result = pipeline._apply_output_caps(items)
    keys = {item.source_key for item in result}
    assert keys == {"web:ccci-1", "web:esg-1"}


class _DummyStateStore:
    def __init__(self) -> None:
        self.cap_dropped_calls: list[tuple[str, str, str | None, str | None]] = []

    def add_cap_dropped(
        self,
        source_key: str,
        processed_at: str,
        article_date: str | None = None,
        reason: str | None = None,
    ) -> None:
        self.cap_dropped_calls.append((source_key, processed_at, article_date, reason))


def test_output_caps_persists_cap_dropped_items() -> None:
    pipeline = _make_pipeline_with_caps(max_pa=1)
    pipeline.state_store = cast(Any, _DummyStateStore())

    items = [
        _mk_item_pa("web:keep", 15, "Wirtschafts- und Steuerstrafrecht"),
        _mk_item_pa("web:drop", 10, "Wirtschafts- und Steuerstrafrecht"),
    ]

    result = pipeline._apply_output_caps(items, execution_timestamp="2026-04-02T10:00:00Z")
    keys = {item.source_key for item in result}

    assert keys == {"web:keep"}
    state_store = cast(_DummyStateStore, pipeline.state_store)
    assert len(state_store.cap_dropped_calls) == 1
    source_key, processed_at, _, reason = state_store.cap_dropped_calls[0]
    assert source_key == "web:drop"
    assert processed_at == "2026-04-02T10:00:00Z"
    assert reason == "cap_practice_area"

