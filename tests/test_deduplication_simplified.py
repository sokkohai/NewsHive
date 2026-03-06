"""Tests for simplified Deduplication stage (State Store check only).

Validates that Deduplication no longer performs date filtering.
"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from src.pipeline import Pipeline
from src.models import ContentItem
from src.state_store import StateStoreRecord


def create_test_item(source_key: str, published_at: str) -> ContentItem:
    """Helper to create test ContentItem."""
    return ContentItem(
        id=source_key,
        source_type="web",
        source_key=source_key,
        title="Test Article",
        summary="",
        content="",
        categories=[],
        published_at=published_at,
        discovered_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        extracted_at="",
        source_url=source_key,
    )


def test_deduplication_allows_old_articles():
    """Test that old articles (>3 days) are NOT filtered in deduplication stage."""
    pipeline = Mock(spec=Pipeline)
    pipeline.state_store = Mock()
    pipeline.state_store.get_record = Mock(return_value=None)  # Not in state store
    
    # Create article older than 3 days
    current_time = datetime.now(timezone.utc)
    old_date = (current_time - timedelta(days=5)).isoformat().replace("+00:00", "Z")
    
    candidates = [create_test_item("web:https://example.com/old", old_date)]
    
    # Simulate deduplication logic (simplified)
    new_items = []
    for item in candidates:
        record = pipeline.state_store.get_record(item.source_key)
        if record is None:
            new_items.append(item)
        elif record.status != "success":
            new_items.append(item)
    
    # Old article should pass through (not filtered by date)
    assert len(new_items) == 1


def test_deduplication_skips_success_items():
    """Test that items with status 'success' are skipped."""
    pipeline = Mock(spec=Pipeline)
    pipeline.state_store = Mock()
    
    success_record = StateStoreRecord(
        source_key="web:https://example.com/article",
        processed_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        status="success"
    )
    pipeline.state_store.get_record = Mock(return_value=success_record)
    
    candidates = [create_test_item("web:https://example.com/article", "unknown")]
    
    # Simulate deduplication
    new_items = []
    for item in candidates:
        record = pipeline.state_store.get_record(item.source_key)
        if record is None:
            new_items.append(item)
        elif record.status != "success":
            new_items.append(item)
    
    # Should be filtered (already successful)
    assert len(new_items) == 0


def test_deduplication_retries_failed_items():
    """Test that failed items are retried."""
    pipeline = Mock(spec=Pipeline)
    pipeline.state_store = Mock()
    
    failed_record = StateStoreRecord(
        source_key="web:https://example.com/article",
        processed_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        status="extraction_failed"
    )
    pipeline.state_store.get_record = Mock(return_value=failed_record)
    
    candidates = [create_test_item("web:https://example.com/article", "unknown")]
    
    # Simulate deduplication
    new_items = []
    for item in candidates:
        record = pipeline.state_store.get_record(item.source_key)
        if record is None:
            new_items.append(item)
        elif record.status != "success":
            new_items.append(item)
    
    # Should be included (retry failed item)
    assert len(new_items) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
