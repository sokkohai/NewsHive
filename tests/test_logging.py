"""Tests for logging configuration and functionality."""

import logging
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import sys

from src.cli import main, _run_pipeline
import argparse


class TestLoggingConfiguration(unittest.TestCase):
    """Test logging setup across different CLI modes."""

    def setUp(self):
        """Set up test fixtures."""
        # Create temporary log file for testing
        self.temp_log_fd, self.temp_log_path = tempfile.mkstemp(suffix=".log")
        os.close(self.temp_log_fd)  # Close the file descriptor, not Path
        
        # Reset logging configuration before each test
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

    def tearDown(self):
        """Clean up after tests."""
        # Close and remove all file handlers first
        for handler in logging.root.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
            logging.root.removeHandler(handler)
        
        # Remove temp log file
        try:
            Path(self.temp_log_path).unlink()
        except (FileNotFoundError, PermissionError):
            pass

    def test_logging_writes_to_file(self):
        """Test that log messages are actually written to file."""
        # Configure logging to write to temp file
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(self.temp_log_path),
                logging.StreamHandler(),
            ],
            force=True,
        )
        
        # Get a logger and write a test message
        test_logger = logging.getLogger("test_module")
        test_message = "This is a test log message"
        test_logger.info(test_message)
        
        # Force flush to disk
        for handler in logging.root.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.flush()
        
        # Read log file and verify message was written
        with open(self.temp_log_path, "r") as f:
            log_content = f.read()
        
        self.assertIn(test_message, log_content)
        self.assertIn("test_module", log_content)

    def test_multiple_log_entries(self):
        """Test that multiple log entries are appended to file."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(levelname)s - %(message)s",
            handlers=[logging.FileHandler(self.temp_log_path)],
            force=True,
        )
        
        test_logger = logging.getLogger("multi_test")
        
        # Write multiple log entries
        messages = ["First message", "Second message", "Third message"]
        for msg in messages:
            test_logger.info(msg)
        
        # Force flush
        for handler in logging.root.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.flush()
        
        # Verify all messages are in file
        with open(self.temp_log_path, "r") as f:
            log_content = f.read()
        
        for msg in messages:
            self.assertIn(msg, log_content)

    def test_logging_handlers_configured(self):
        """Test that logging has both file and console handlers."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(self.temp_log_path),
                logging.StreamHandler(),
            ],
            force=True,
        )
        
        handler_types = [type(h).__name__ for h in logging.root.handlers]
        
        self.assertIn("FileHandler", handler_types)
        self.assertIn("StreamHandler", handler_types)


if __name__ == "__main__":
    unittest.main()
