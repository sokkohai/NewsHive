"""Integration tests for CLI module."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.cli import _run_pipeline
import argparse


class TestCLIPipeline(unittest.TestCase):
    """Test CLI pipeline command."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self.temp_dir.name) / "config.json"
        self.state_path = Path(self.temp_dir.name) / "state_store.json"

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_pipeline_missing_config(self):
        """Test that missing config file returns exit code 2."""
        args = argparse.Namespace(
            config=str(self.config_path / "nonexistent.json"),
            state_store=str(self.state_path),
            results=str(Path(self.temp_dir.name) / "results.json"),
            logs=str(Path(self.temp_dir.name) / "newshive.log"),
            log_level="INFO",
        )

        exit_code = _run_pipeline(args)
        self.assertEqual(exit_code, 2)

    def test_pipeline_invalid_json(self):
        """Test that invalid JSON in config returns exit code 2."""
        self.config_path.write_text("{invalid json}")

        args = argparse.Namespace(
            config=str(self.config_path),
            state_store=str(self.state_path),
            results=str(Path(self.temp_dir.name) / "results.json"),
            logs=str(Path(self.temp_dir.name) / "newshive.log"),
            log_level="INFO",
        )

        exit_code = _run_pipeline(args)
        self.assertEqual(exit_code, 2)

    def test_pipeline_valid_config(self):
        """Test that valid config passes validation."""
        config_data = {
            "pipeline_version": "1.0",
            "web_sources": [
                {
                    "url": "https://example.com",
                    "categories": ["test"],
                    "listings_type": "linked"
                }
            ],
            "email_folders": [],
            "keywords": ["test"],
            "categories": [{"name": "test", "keywords": ["test"]}],
        }
        self.config_path.write_text(json.dumps(config_data))

        args = argparse.Namespace(
            config=str(self.config_path),
            state_store=str(self.state_path),
            results=str(Path(self.temp_dir.name) / "results.json"),
            logs=str(Path(self.temp_dir.name) / "newshive.log"),
            log_level="INFO",
        )

        with patch("src.cli.Pipeline") as mock_pipeline:
            mock_instance = MagicMock()
            mock_instance.run.return_value = MagicMock(items=[], failed_items=[])
            mock_pipeline.return_value = mock_instance

            exit_code = _run_pipeline(args)
            self.assertEqual(exit_code, 0)


class TestCLIExitCodes(unittest.TestCase):
    """Test CLI exit code behavior."""

    def test_exit_code_0_on_success(self):
        """Test exit code 0 on successful execution."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_data = {
                "pipeline_version": "1.0",
                "web_sources": [
                    {
                        "url": "https://example.com",
                        "categories": ["test"],
                        "listings_type": "linked"
                    }
                ],
                "email_folders": [],
                "keywords": ["test"],
                "categories": [{"name": "test", "keywords": ["test"]}],
            }
            config_path.write_text(json.dumps(config_data))

            args = argparse.Namespace(
                config=str(config_path),
                state_store=str(Path(temp_dir) / "state.json"),
                results=str(Path(temp_dir) / "results.json"),
                logs=str(Path(temp_dir) / "newshive.log"),
                log_level="INFO",
            )

            with patch("src.cli.Pipeline") as mock_pipeline:
                mock_instance = MagicMock()
                mock_instance.run.return_value = MagicMock(
                    items=[], failed_items=[]
                )
                mock_pipeline.return_value = mock_instance

                exit_code = _run_pipeline(args)
                self.assertEqual(exit_code, 0)

    def test_exit_code_2_on_config_error(self):
        """Test exit code 2 on configuration error."""
        args = argparse.Namespace(
            config="/nonexistent/path/config.json",
            state_store="./state.json",
            results="./results.json",
            logs="./newshive.log",
            log_level="INFO",
        )

        exit_code = _run_pipeline(args)
        self.assertEqual(exit_code, 2)


if __name__ == "__main__":
    unittest.main()
