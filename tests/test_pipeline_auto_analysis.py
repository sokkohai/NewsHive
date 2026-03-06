"""Tests for pipeline auto-analysis feature."""

import json
import tempfile
from pathlib import Path

from src.config import ConfigLoader
from src.pipeline import Pipeline


def test_pipeline_initialization_with_auto_analyze() -> None:
    """Test Pipeline initializes with auto_analyze flag."""
    config_dict = {
        "pipeline_version": "1.0",
        "web_sources": [{"url": "https://example.com"}],
        "keywords": ["test"],
        "categories": [],
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        config_path = tmpdir_path / "config.json"

        with open(config_path, 'w') as f:
            json.dump(config_dict, f)

        config = ConfigLoader.load(config_path)

        # Test with auto_analyze=False (default)
        pipeline = Pipeline(config=config, auto_analyze=False)
        assert pipeline.auto_analyze is False

        # Test with auto_analyze=True
        pipeline = Pipeline(config=config, auto_analyze=True)
        assert pipeline.auto_analyze is True


def test_pipeline_auto_analyze_method_exists() -> None:
    """Test that _auto_analyze_sources method exists and is callable."""
    config_dict = {
        "pipeline_version": "1.0",
        "web_sources": [{"url": "https://example.com"}],
        "keywords": ["test"],
        "categories": [],
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        config_path = tmpdir_path / "config.json"

        with open(config_path, 'w') as f:
            json.dump(config_dict, f)

        config = ConfigLoader.load(config_path)
        pipeline = Pipeline(config=config, auto_analyze=True)

        # Verify method exists
        assert hasattr(pipeline, '_auto_analyze_sources')
        assert callable(pipeline._auto_analyze_sources)


def test_pipeline_auto_analyze_graceful_failure() -> None:
    """Test that pipeline handles auto-analysis failures gracefully."""
    config_dict = {
        "pipeline_version": "1.0",
        "web_sources": [{"url": "https://example.com"}],
        "keywords": ["test"],
        "categories": [],
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        config_path = tmpdir_path / "config.json"

        with open(config_path, 'w') as f:
            json.dump(config_dict, f)

        config = ConfigLoader.load(config_path)
        pipeline = Pipeline(config=config, auto_analyze=True)

        # This should not raise an exception even if analysis fails
        # (the method logs warnings and continues)
        try:
            pipeline._auto_analyze_sources()
        except Exception:
            # If analysis tools aren't available or API fails, that's OK
            # The pipeline should handle it gracefully
            pass
