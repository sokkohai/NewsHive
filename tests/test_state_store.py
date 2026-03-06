"""Tests for state store management.

Tests StateStoreManager as specified in specs/core/DEDUPLICATION.md.
"""

import json
import tempfile
from pathlib import Path

import pytest

from src.state_store import StateStoreManager


class TestStateStoreManager:
    """Test StateStoreManager."""

    def test_create_new_state_store(self):
        """Test creating a new state store when file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"
            manager = StateStoreManager(store_path)

            assert manager.store is not None
            assert not store_path.exists()  # Not saved yet

    def test_load_existing_state_store(self):
        """Test loading an existing state store file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"

            # Create initial state store
            initial_data = {
                "last_run_timestamp": "2026-01-08T12:00:00Z",
                "items": [
                    {
                        "source_key": "https://example.com/article",
                        "processed_at": "2026-01-08T10:00:00Z",
                        "status": "success",
                    }
                ],
            }

            with open(store_path, "w") as f:
                json.dump(initial_data, f)

            # Load it
            manager = StateStoreManager(store_path)
            assert manager.store.last_run_timestamp == "2026-01-08T12:00:00Z"
            assert len(manager.store.items) == 1

    def test_state_store_corrupted_file(self):
        """Test loading corrupted state store file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"

            with open(store_path, "w") as f:
                f.write("invalid json {")

            with pytest.raises(ValueError):
                StateStoreManager(store_path)

    def test_has_processed(self):
        """Test has_processed() method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"
            manager = StateStoreManager(store_path)

            assert not manager.has_processed("https://example.com/article")

            manager.add_success("https://example.com/article", "2026-01-08T10:00:00Z")
            assert manager.has_processed("https://example.com/article")

    def test_get_record(self):
        """Test get_record() method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"
            manager = StateStoreManager(store_path)

            manager.add_success("https://example.com/article", "2026-01-08T10:00:00Z")
            record = manager.get_record("https://example.com/article")

            assert record is not None
            assert record.status == "success"

    def test_add_success(self):
        """Test adding a success record."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"
            manager = StateStoreManager(store_path)

            manager.add_success("https://example.com/article", "2026-01-08T10:00:00Z")

            record = manager.get_record("https://example.com/article")
            assert record.status == "success"

    def test_add_extraction_failure(self):
        """Test adding an extraction failure record."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"
            manager = StateStoreManager(store_path)

            manager.add_extraction_failure("https://example.com/article", "2026-01-08T10:00:00Z")

            record = manager.get_record("https://example.com/article")
            assert record.status == "extraction_failed"

    def test_add_summarization_failure(self):
        """Test adding a summarization failure record."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"
            manager = StateStoreManager(store_path)

            manager.add_summarization_failure("https://example.com/article", "2026-01-08T10:00:00Z")

            record = manager.get_record("https://example.com/article")
            assert record.status == "summarization_failed"

    def test_add_categorization_failure(self):
        """Test adding a categorization failure record."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"
            manager = StateStoreManager(store_path)

            manager.add_categorization_failure(
                "https://example.com/article", "2026-01-08T10:00:00Z"
            )

            record = manager.get_record("https://example.com/article")
            assert record.status == "categorization_failed"

    def test_update_last_run(self):
        """Test updating last run timestamp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"
            manager = StateStoreManager(store_path)

            initial_timestamp = manager.get_last_run_timestamp()
            new_timestamp = "2026-01-09T12:00:00Z"
            manager.update_last_run(new_timestamp)

            assert manager.get_last_run_timestamp() == new_timestamp
            assert manager.get_last_run_timestamp() != initial_timestamp

    def test_save_state_store(self):
        """Test persisting state store to file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"
            manager = StateStoreManager(store_path)

            manager.add_success("https://example.com/article", "2026-01-08T10:00:00Z")
            manager.save()

            assert store_path.exists()

            # Verify file contents
            with open(store_path) as f:
                data = json.load(f)

            assert len(data["items"]) == 1
            assert data["items"][0]["source_key"] == "https://example.com/article"

    def test_persistence_across_instances(self):
        """Test that state persists across StateStoreManager instances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "state_store.json"

            # Create first manager and add a record
            manager1 = StateStoreManager(store_path)
            manager1.add_success("https://example.com/article1", "2026-01-08T10:00:00Z")
            manager1.save()

            # Create second manager and verify it loads the data
            manager2 = StateStoreManager(store_path)
            assert manager2.has_processed("https://example.com/article1")

    def test_default_state_store_path(self):
        """Test default state store path."""
        # This test uses the default path
        manager = StateStoreManager()
        assert manager.path == StateStoreManager.DEFAULT_STATE_STORE_PATH
