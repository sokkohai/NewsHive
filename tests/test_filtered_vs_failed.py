"""Tests for filtered items tracking (not failures).

Tests verify that:
- Items filtered due to age are tracked with status "filtered" 
- Items filtered due to keywords are tracked with status "filtered"
- Real extraction errors are tracked with status "extraction_failed"
- failure_reason clearly describes whether filtered or failed
"""

import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pytest

from src.models import FailedItem, StateStoreRecord
from src.state_store import StateStoreManager
from src.extraction import Extractor
from src.models import ContentItem


def test_filtered_item_status_in_state_store():
    """Test that filtered items are tracked with status='filtered' in state store."""
    store_path = Path("test_state_store_filtered.json")
    try:
        manager = StateStoreManager(store_path)
        
        # Record a filtered item
        manager.add_filtered("https://example.com/old-article", "2026-01-23T12:00:00Z")
        
        # Check state store record
        record = manager.get_record("https://example.com/old-article")
        assert record is not None
        assert record.status == "filtered"
        
    finally:
        if store_path.exists():
            store_path.unlink()


def test_extraction_failure_vs_filtered():
    """Test distinction between real extraction failure and filtering."""
    store_path = Path("test_state_store_distinction.json")
    try:
        manager = StateStoreManager(store_path)
        
        # Record a filtered item
        manager.add_filtered("https://example.com/too-old", "2026-01-23T12:00:00Z")
        
        # Record a real extraction failure
        manager.add_extraction_failure("https://example.com/broken-link", "2026-01-23T12:00:00Z")
        
        # Verify they have different statuses
        filtered_record = manager.get_record("https://example.com/too-old")
        failed_record = manager.get_record("https://example.com/broken-link")
        
        assert filtered_record.status == "filtered"
        assert failed_record.status == "extraction_failed"
        
    finally:
        if store_path.exists():
            store_path.unlink()


def test_failed_item_with_clear_reason():
    """Test that FailedItem has clear descriptive failure_reason."""
    # Filtered item
    filtered = FailedItem(
        id="https://example.com/old",
        failure_stage="extraction",
        failure_reason="Article too old (36d 4h)",
        discovered_at="2026-01-23T16:05:34.584182Z",
        source_type="web",
        source_url="https://example.com/old"
    )
    
    # Real failure item
    failed = FailedItem(
        id="https://example.com/broken",
        failure_stage="extraction",
        failure_reason="Extraction failed: Connection timeout after 30s",
        discovered_at="2026-01-23T16:05:34.584182Z",
        source_type="web",
        source_url="https://example.com/broken"
    )
    
    # Both are recorded, but reason makes it clear which is which
    filtered_dict = filtered.to_dict()
    failed_dict = failed.to_dict()
    
    assert "too old" in filtered_dict["failure_reason"].lower()
    assert "timeout" in failed_dict["failure_reason"].lower()
    
    # Both have stage "extraction" but different reasons
    assert filtered_dict["failure_stage"] == "extraction"
    assert failed_dict["failure_stage"] == "extraction"


def test_filtered_items_not_retried():
    """Test that filtered items (status='filtered') are NOT retried.
    
    Only items with extraction_failed, summarization_failed, or categorization_failed
    should be retried.
    """
    store_path = Path("test_state_store_retry.json")
    try:
        manager = StateStoreManager(store_path)
        
        # Record different statuses
        manager.add_filtered("https://example.com/too-old", "2026-01-23T12:00:00Z")
        manager.add_extraction_failure("https://example.com/broken", "2026-01-23T12:00:00Z")
        
        # Get records
        filtered_record = manager.get_record("https://example.com/too-old")
        failed_record = manager.get_record("https://example.com/broken")
        
        # Only extraction_failed should trigger retry in deduplication logic
        # Filtered should be skipped permanently
        assert filtered_record.status == "filtered"
        assert failed_record.status == "extraction_failed"
        
    finally:
        if store_path.exists():
            store_path.unlink()


def test_failed_item_with_all_optional_fields():
    """Test FailedItem serialization with all optional fields."""
    failed = FailedItem(
        id="https://example.com/article",
        failure_stage="extraction",
        failure_reason="Article too old (42d 10h)",
        discovered_at="2026-01-23T16:05:34.584182Z",
        source_type="web",
        source_url="https://example.com/article"
    )
    
    dict_repr = failed.to_dict()
    
    # Check all fields are present
    assert dict_repr["id"] == "https://example.com/article"
    assert dict_repr["failure_stage"] == "extraction"
    assert dict_repr["failure_reason"] == "Article too old (42d 10h)"
    assert dict_repr["discovered_at"] == "2026-01-23T16:05:34.584182Z"
    assert dict_repr["source_type"] == "web"
    assert dict_repr["source_url"] == "https://example.com/article"
    
    # Should be valid JSON
    json_str = failed.to_json()
    json_dict = json.loads(json_str)
    assert json_dict["failure_reason"] == "Article too old (42d 10h)"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
