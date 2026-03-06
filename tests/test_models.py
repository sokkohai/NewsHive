"""Tests for data models.

Tests ContentItem, Envelope, FailedItem, StateStore, and StateStoreRecord
as specified in specs/core/DATA_MODEL.md and specs/core/DEDUPLICATION.md.
"""

import json

import pytest

from src.models import (
    ContentItem,
    Envelope,
    FailedItem,
    StateStore,
    StateStoreRecord,
)


class TestContentItem:
    """Test ContentItem data model."""

    def test_create_minimal_content_item(self):
        """Test creating a ContentItem with required fields only."""
        item = ContentItem(
            id="https://example.com/article",
            source_type="web",
            source_key="web:https://example.com/article",
            title="Test Article",
            summary="A short summary",
            content="Full article content here",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-08T10:00:00Z",
            extracted_at="2026-01-08T10:02:00Z",
        )

        assert item.id == "https://example.com/article"
        assert item.source_type == "web"
        assert item.title == "Test Article"
        assert item.categories == []
        assert item.author is None

    def test_create_content_item_with_optional_fields(self):
        """Test creating a ContentItem with optional fields."""
        item = ContentItem(
            id="https://example.com/article",
            source_type="web",
            source_key="web:https://example.com/article",
            title="Test Article",
            summary="A short summary",
            content="Full article content",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-08T10:00:00Z",
            extracted_at="2026-01-08T10:02:00Z",
            author="John Doe",
            source_url="https://example.com/article",
            extraction_method="firecrawl",
            links=["https://example.com", "https://other.com"],
        )

        assert item.author == "John Doe"
        assert item.extraction_method == "firecrawl"
        assert len(item.links) == 2

    def test_content_item_to_dict_excludes_none(self):
        """Test that to_dict() excludes None optional fields."""
        item = ContentItem(
            id="https://example.com/article",
            source_type="web",
            source_key="web:https://example.com/article",
            title="Test Article",
            summary="Summary",
            content="Content",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-08T10:00:00Z",
            extracted_at="2026-01-08T10:02:00Z",
            author=None,
        )

        d = item.to_dict()
        assert "author" not in d
        assert "id" in d
        assert "title" in d

    def test_content_item_to_json(self):
        """Test ContentItem to_json() produces valid JSON."""
        item = ContentItem(
            id="https://example.com/article",
            source_type="web",
            source_key="web:https://example.com/article",
            title="Test Article",
            summary="Summary",
            content="Content",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-08T10:00:00Z",
            extracted_at="2026-01-08T10:02:00Z",
        )

        json_str = item.to_json()
        parsed = json.loads(json_str)
        assert parsed["id"] == "https://example.com/article"
        assert parsed["title"] == "Test Article"

    def test_email_content_item(self):
        """Test ContentItem for email source."""
        item = ContentItem(
            id="AAMkADMwNTQzNTU4LTZjODAtNDQ5ZC04OTA4LTg0YjQ0NzVhODczMQBGAAA=",
            source_type="email",
            source_key="AAMkADMwNTQzNTU4LTZjODAtNDQ5ZC04OTA4LTg0YjQ0NzVhODczMQBGAAA=",
            title="Email Subject",
            summary="Summary of email",
            content="Email body content",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-08T08:15:00Z",
            extracted_at="2026-01-08T08:16:00Z",
            email_sender="legal@regulations.eu",
            email_subject="Email Subject",
            email_folder="Inbox",
        )

        assert item.source_type == "email"
        assert item.email_sender == "legal@regulations.eu"


class TestFailedItem:
    """Test FailedItem data model."""

    def test_create_failed_item(self):
        """Test creating a FailedItem."""
        failed = FailedItem(
            id="https://example.com/article",
            failure_stage="extraction",
            failure_reason="Timeout during extraction",
            discovered_at="2026-01-08T10:00:00Z",
        )

        assert failed.id == "https://example.com/article"
        assert failed.failure_stage == "extraction"

    def test_failed_item_to_dict(self):
        """Test FailedItem to_dict()."""
        failed = FailedItem(
            id="https://example.com/article",
            failure_stage="summarization",
            failure_reason="API error",
            discovered_at="2026-01-08T10:00:00Z",
        )

        d = failed.to_dict()
        assert d["id"] == "https://example.com/article"
        assert d["failure_stage"] == "summarization"
        assert d["failure_reason"] == "API error"
        assert d["discovered_at"] == "2026-01-08T10:00:00Z"


class TestEnvelope:
    """Test Envelope data model."""

    def test_create_empty_envelope(self):
        """Test creating an empty Envelope."""
        envelope = Envelope()
        assert len(envelope.items) == 0
        assert len(envelope.failed_items) == 0
        assert envelope.generated_at  # Should have auto-generated timestamp
        assert envelope.pipeline_version == "1.0"

    def test_envelope_with_items(self):
        """Test Envelope with items."""
        item = ContentItem(
            id="https://example.com/article",
            source_type="web",
            source_key="web:https://example.com/article",
            title="Article",
            summary="Summary",
            content="Content",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-08T10:00:00Z",
            extracted_at="2026-01-08T10:02:00Z",
        )

        envelope = Envelope(items=[item])
        assert len(envelope.items) == 1

    def test_envelope_to_json(self):
        """Test Envelope to_json()."""
        item = ContentItem(
            id="https://example.com/article",
            source_type="web",
            source_key="web:https://example.com/article",
            title="Article",
            summary="Summary",
            content="Content",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-08T10:00:00Z",
            extracted_at="2026-01-08T10:02:00Z",
        )

        envelope = Envelope(items=[item])
        json_str = envelope.to_json()
        parsed = json.loads(json_str)
        assert len(parsed["items"]) == 1
        assert parsed["items"][0]["title"] == "Article"
        assert "generated_at" in parsed
        assert "pipeline_version" in parsed
        # failed_items should not be in JSON if empty
        assert "failed_items" not in parsed


class TestStateStoreRecord:
    """Test StateStoreRecord data model."""

    def test_create_success_record(self):
        """Test creating a success StateStoreRecord."""
        record = StateStoreRecord(
            source_key="https://example.com/article",
            processed_at="2026-01-08T10:00:00Z",
            status="success",
        )

        assert record.source_key == "https://example.com/article"
        assert record.status == "success"

    def test_create_failure_records(self):
        """Test creating failure StateStoreRecords."""
        statuses = [
            "extraction_failed",
            "summarization_failed",
            "categorization_failed",
        ]

        for status in statuses:
            record = StateStoreRecord(
                source_key="https://example.com/article",
                processed_at="2026-01-08T10:00:00Z",
                status=status,
            )
            assert record.status == status

    def test_invalid_status_raises_error(self):
        """Test that invalid status raises ValueError."""
        with pytest.raises(ValueError):
            StateStoreRecord(
                source_key="https://example.com/article",
                processed_at="2026-01-08T10:00:00Z",
                status="invalid_status",
            )

    def test_record_to_dict(self):
        """Test StateStoreRecord to_dict()."""
        record = StateStoreRecord(
            source_key="https://example.com/article",
            processed_at="2026-01-08T10:00:00Z",
            status="success",
        )

        d = record.to_dict()
        assert d["source_key"] == "https://example.com/article"
        assert d["status"] == "success"


class TestStateStore:
    """Test StateStore data model."""

    def test_create_state_store(self):
        """Test creating a StateStore."""
        store = StateStore(last_run_timestamp="2026-01-08T12:00:00Z")
        assert store.last_run_timestamp == "2026-01-08T12:00:00Z"
        assert len(store.items) == 0

    def test_add_record(self):
        """Test adding a record to StateStore."""
        store = StateStore(last_run_timestamp="2026-01-08T12:00:00Z")
        record = StateStoreRecord(
            source_key="https://example.com/article",
            processed_at="2026-01-08T10:00:00Z",
            status="success",
        )

        store.add_record(record)
        assert len(store.items) == 1
        assert store.has_processed("https://example.com/article")

    def test_has_processed(self):
        """Test has_processed() method."""
        store = StateStore(last_run_timestamp="2026-01-08T12:00:00Z")
        assert not store.has_processed("https://example.com/article")

        record = StateStoreRecord(
            source_key="https://example.com/article",
            processed_at="2026-01-08T10:00:00Z",
            status="success",
        )
        store.add_record(record)

        assert store.has_processed("https://example.com/article")

    def test_get_record(self):
        """Test get_record() method."""
        store = StateStore(last_run_timestamp="2026-01-08T12:00:00Z")
        record = StateStoreRecord(
            source_key="https://example.com/article",
            processed_at="2026-01-08T10:00:00Z",
            status="success",
        )
        store.add_record(record)

        retrieved = store.get_record("https://example.com/article")
        assert retrieved is not None
        assert retrieved.status == "success"

    def test_state_store_to_dict(self):
        """Test StateStore to_dict()."""
        store = StateStore(last_run_timestamp="2026-01-08T12:00:00Z")
        record = StateStoreRecord(
            source_key="https://example.com/article",
            processed_at="2026-01-08T10:00:00Z",
            status="success",
        )
        store.add_record(record)

        d = store.to_dict()
        assert d["last_run_timestamp"] == "2026-01-08T12:00:00Z"
        assert len(d["items"]) == 1

    def test_state_store_from_dict(self):
        """Test StateStore from_dict()."""
        data = {
            "last_run_timestamp": "2026-01-08T12:00:00Z",
            "items": [
                {
                    "source_key": "https://example.com/article",
                    "processed_at": "2026-01-08T10:00:00Z",
                    "status": "success",
                }
            ],
        }

        store = StateStore.from_dict(data)
        assert store.last_run_timestamp == "2026-01-08T12:00:00Z"
        assert len(store.items) == 1
        assert store.has_processed("https://example.com/article")

    def test_state_store_from_dict_missing_last_run(self):
        """Test StateStore from_dict() with missing last_run_timestamp."""
        data = {"items": []}

        store = StateStore.from_dict(data)
        # Should have a default timestamp
        assert store.last_run_timestamp

    def test_state_store_to_json(self):
        """Test StateStore to_json()."""
        store = StateStore(last_run_timestamp="2026-01-08T12:00:00Z")
        record = StateStoreRecord(
            source_key="https://example.com/article",
            processed_at="2026-01-08T10:00:00Z",
            status="success",
        )
        store.add_record(record)

        json_str = store.to_json()
        parsed = json.loads(json_str)
        assert parsed["last_run_timestamp"] == "2026-01-08T12:00:00Z"
