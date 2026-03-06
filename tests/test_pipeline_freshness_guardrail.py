"""Tests for pipeline-level freshness guardrail after discovery."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from src.models import ContentItem
from src.pipeline import Pipeline


def _mk_item(source_key: str, published_at: str, source_url: str) -> ContentItem:
    return ContentItem(
        id=source_key,
        source_type="web",
        source_key=source_key,
        title="t",
        summary="",
        content="",
        categories=["CCCI"],
        published_at=published_at,
        discovered_at="",
        extracted_at="",
        source_url=source_url,
    )


def test_filter_fresh_candidates_keeps_only_recent_items() -> None:
    pipeline = Pipeline.__new__(Pipeline)
    pipeline.config = SimpleNamespace(article_max_age_days=3)

    now = datetime.now(timezone.utc)
    fresh = (now - timedelta(hours=12)).isoformat().replace("+00:00", "Z")
    stale = (now - timedelta(days=5)).isoformat().replace("+00:00", "Z")

    candidates = [
        _mk_item("web:fresh", fresh, "https://example.com/fresh"),
        _mk_item("web:stale", stale, "https://example.com/stale"),
        _mk_item("web:unknown", "unknown", "https://example.com/no-date"),
    ]

    filtered, stale_count, undated_count = pipeline._filter_fresh_candidates(candidates)

    assert [item.source_key for item in filtered] == ["web:fresh"]
    assert stale_count == 1
    assert undated_count == 1


def test_filter_fresh_candidates_infers_date_from_url() -> None:
    pipeline = Pipeline.__new__(Pipeline)
    pipeline.config = SimpleNamespace(article_max_age_days=3)

    today = datetime.now(timezone.utc)
    recent_url = f"https://example.com/{today.year}/{today.month:02d}/{today.day:02d}/x"

    candidates = [
        _mk_item("web:from-url", "unknown", recent_url),
    ]

    filtered, stale_count, undated_count = pipeline._filter_fresh_candidates(candidates)

    assert [item.source_key for item in filtered] == ["web:from-url"]
    assert stale_count == 0
    assert undated_count == 0
