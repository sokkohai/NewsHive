#!/usr/bin/env python3
"""Tests for output_versioning module.

Tests the timestamped results file versioning and cleanup functionality
as specified in specs/core/OUTPUT.md.
"""

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import TestCase

from src.output_versioning import ResultsVersioning


class TestResultsVersioning(TestCase):
    """Tests for ResultsVersioning class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.archive_dir = Path(self.temp_dir.name) / "archive"
        # Monkey-patch the archive dir for tests
        ResultsVersioning.ARCHIVE_DIR = str(self.archive_dir)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_get_timestamp_filename_with_iso_string(self) -> None:
        """Test filename generation from ISO 8601 timestamp string."""
        iso_string = "2026-01-16T14:30:25Z"
        filename = ResultsVersioning.get_timestamp_filename(iso_string)

        self.assertEqual(filename, "results_2026-01-16T143025Z.json")

    def test_get_timestamp_filename_with_datetime(self) -> None:
        """Test filename generation from datetime object."""
        dt = datetime(2026, 1, 16, 14, 30, 25, tzinfo=timezone.utc)
        filename = ResultsVersioning.get_timestamp_filename(dt)

        self.assertEqual(filename, "results_2026-01-16T143025Z.json")

    def test_get_timestamp_filename_with_none(self) -> None:
        """Test filename generation with current time (None)."""
        filename = ResultsVersioning.get_timestamp_filename()

        # Check format: results_YYYY-MM-DDTHHHMMSSZ.json
        self.assertRegex(
            filename, r"^results_\d{4}-\d{2}-\d{2}T\d{6}Z\.json$"
        )

    def test_get_archive_path_without_filename(self) -> None:
        """Test getting archive directory path."""
        path = ResultsVersioning.get_archive_path()

        self.assertEqual(path, self.archive_dir)

    def test_get_archive_path_with_filename(self) -> None:
        """Test getting full path to a specific file."""
        filename = "results_2026-01-16T143025Z.json"
        path = ResultsVersioning.get_archive_path(filename)

        self.assertEqual(path, self.archive_dir / filename)

    def test_write_results_creates_directory(self) -> None:
        """Test that write_results creates archive directory if needed."""
        self.assertFalse(self.archive_dir.exists())

        items = [{"source_key": "test1", "title": "Test 1"}]
        timestamp = "2026-01-16T14:30:25Z"

        result_path = ResultsVersioning.write_results(items, timestamp)

        self.assertTrue(self.archive_dir.exists())
        self.assertIsNotNone(result_path)

    def test_write_results_writes_correct_structure(self) -> None:
        """Test that write_results writes correct JSON structure."""
        items = [
            {"source_key": "test1", "title": "Test 1"},
            {"source_key": "test2", "title": "Test 2"},
        ]
        timestamp = "2026-01-16T14:30:25Z"

        result_path = ResultsVersioning.write_results(items, timestamp)

        self.assertTrue(result_path.exists())

        with open(result_path) as f:
            data = json.load(f)

        self.assertEqual(data["execution_timestamp"], timestamp)
        self.assertEqual(data["item_count"], 2)
        self.assertEqual(len(data["items"]), 2)
        self.assertEqual(data["items"][0]["source_key"], "test1")

    def test_write_results_with_empty_items(self) -> None:
        """Test write_results with empty items list."""
        items = []
        timestamp = "2026-01-16T14:30:25Z"

        result_path = ResultsVersioning.write_results(items, timestamp)

        self.assertTrue(result_path.exists())

        with open(result_path) as f:
            data = json.load(f)

        self.assertEqual(data["item_count"], 0)
        self.assertEqual(len(data["items"]), 0)

    def test_list_available_results_empty_archive(self) -> None:
        """Test listing results when archive is empty."""
        results = ResultsVersioning.list_available_results()

        self.assertEqual(results, [])

    def test_list_available_results_with_files(self) -> None:
        """Test listing available results files."""
        # Create some test files
        items = [{"source_key": "test", "title": "Test"}]

        for i in range(3):
            hours_ago = 24 * i
            dt = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
            iso_str = dt.strftime("%Y-%m-%dT%H%M%SZ")
            timestamp = f"2026-01-16T{iso_str.split('T')[1]}"
            ResultsVersioning.write_results(items, timestamp)

        results = ResultsVersioning.list_available_results()

        self.assertGreater(len(results), 0)
        # Check structure
        for result in results:
            self.assertIn("filename", result)
            self.assertIn("timestamp", result)
            self.assertIn("datetime", result)
            self.assertIn("item_count", result)
            self.assertIn("size_bytes", result)

    def test_list_available_results_sorted_newest_first(self) -> None:
        """Test that results are sorted with newest first."""
        items = [{"source_key": "test", "title": "Test"}]

        # Create files with different timestamps
        timestamps = [
            "2026-01-14T100000Z",
            "2026-01-16T143025Z",
            "2026-01-15T090010Z",
        ]

        for timestamp in timestamps:
            ResultsVersioning.write_results(items, timestamp)

        results = ResultsVersioning.list_available_results()

        # Check that results are sorted newest first
        self.assertEqual(len(results), 3)
        self.assertIn("2026-01-16", results[0]["filename"])  # Newest first

    def test_cleanup_old_results_deletes_files_older_than_retention(self) -> None:
        """Test that cleanup deletes files older than retention period."""
        items = [{"source_key": "test", "title": "Test"}]

        # Create old file (35 days old)
        old_date = datetime.now(timezone.utc) - timedelta(days=35)
        old_iso = old_date.strftime("%Y-%m-%dT%H%M%SZ")
        old_filename = f"results_{old_iso}.json"
        old_path = self.archive_dir / old_filename
        old_path.parent.mkdir(parents=True, exist_ok=True)
        with open(old_path, "w") as f:
            json.dump({"items": items}, f)

        # Create recent file (5 days old)
        recent_date = datetime.now(timezone.utc) - timedelta(days=5)
        recent_iso = recent_date.strftime("%Y-%m-%dT%H%M%SZ")
        ResultsVersioning.write_results(items, recent_iso)

        # Run cleanup with 30-day retention
        deleted = ResultsVersioning.cleanup_old_results(retention_days=30)

        self.assertEqual(deleted, 1)
        self.assertFalse(old_path.exists())

    def test_cleanup_old_results_keeps_recent_files(self) -> None:
        """Test that cleanup does not delete recent files."""
        items = [{"source_key": "test", "title": "Test"}]

        # Create file 10 days old
        recent_date = datetime.now(timezone.utc) - timedelta(days=10)
        recent_iso = recent_date.strftime("%Y-%m-%dT%H%M%SZ")
        ResultsVersioning.write_results(items, recent_iso)

        initial_files = list(self.archive_dir.glob("results_*.json"))
        deleted = ResultsVersioning.cleanup_old_results(retention_days=30)

        final_files = list(self.archive_dir.glob("results_*.json"))

        self.assertEqual(deleted, 0)
        self.assertEqual(len(initial_files), len(final_files))

    def test_cleanup_old_results_with_no_archive(self) -> None:
        """Test cleanup when archive directory doesn't exist."""
        # Use a path that doesn't exist
        ResultsVersioning.ARCHIVE_DIR = str(self.archive_dir / "nonexistent")

        deleted = ResultsVersioning.cleanup_old_results()

        self.assertEqual(deleted, 0)

    def test_load_results_from_file(self) -> None:
        """Test loading results from a specific file."""
        items = [
            {"source_key": "test1", "title": "Test 1"},
            {"source_key": "test2", "title": "Test 2"},
        ]
        timestamp = "2026-01-16T14:30:25Z"

        filename = ResultsVersioning.get_timestamp_filename(timestamp)
        ResultsVersioning.write_results(items, timestamp)

        loaded_data = ResultsVersioning.load_results_from_file(filename)

        self.assertIsNotNone(loaded_data)
        self.assertEqual(loaded_data["execution_timestamp"], timestamp)
        self.assertEqual(loaded_data["item_count"], 2)

    def test_load_results_from_file_not_found(self) -> None:
        """Test loading results from non-existent file."""
        loaded_data = ResultsVersioning.load_results_from_file("nonexistent.json")

        self.assertIsNone(loaded_data)

    def test_get_latest_results(self) -> None:
        """Test getting the most recent results file."""
        items = [{"source_key": "test", "title": "Test"}]

        # Create multiple files
        timestamps = [
            "2026-01-14T100000Z",
            "2026-01-16T143025Z",
            "2026-01-15T090010Z",
        ]

        for timestamp in timestamps:
            ResultsVersioning.write_results(items, timestamp)

        latest = ResultsVersioning.get_latest_results()

        self.assertIsNotNone(latest)
        # Should be the 2026-01-16 file
        self.assertIn("2026-01-16T143025Z", latest["execution_timestamp"])

    def test_get_latest_results_empty_archive(self) -> None:
        """Test getting latest results when archive is empty."""
        latest = ResultsVersioning.get_latest_results()

        self.assertIsNone(latest)


if __name__ == "__main__":
    import unittest
    unittest.main()
