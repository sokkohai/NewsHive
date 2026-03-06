"""Test the complete email integration end-to-end."""

import os
import pytest
from unittest.mock import patch

from src.pipeline import Pipeline
from src.config import Configuration, EmailFolder, Category


class TestEmailIntegration:
    """Test email integration functionality."""

    def test_email_pipeline_configuration(self):
        """Test email pipeline configuration validation."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[EmailFolder("Inbox")],
            categories=[Category(name="compliance", keywords=[])]
        )
        
        assert len(config.email_folders) == 1
        assert config.email_folders[0].folder_path == "Inbox"
        assert len(config.categories) == 1

    @patch.dict(os.environ, {
        "AZURE_CLIENT_ID": "test-client-id",
        "AZURE_CLIENT_SECRET": "test-client-secret", 
        "AZURE_TENANT_ID": "test-tenant-id",
        "AZURE_REFRESH_TOKEN": "test-refresh-token"
    })
    def test_email_pipeline_initialization(self):
        """Test email pipeline initialization with Azure credentials."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[EmailFolder("Inbox")],
            categories=[Category(name="compliance", keywords=[])]
        )
        
        pipeline = Pipeline(config=config)
        assert pipeline.discoverer is not None
        assert len(pipeline.config.email_folders) == 1

    @patch.dict(os.environ, {
        "AZURE_CLIENT_ID": "test-client-id",
        "AZURE_CLIENT_SECRET": "test-client-secret", 
        "AZURE_TENANT_ID": "test-tenant-id"
    })
    def test_email_discovery_without_o365(self):
        """Test email discovery gracefully handles missing O365 authentication."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[EmailFolder("Inbox")],
            categories=[Category(name="test", keywords=[])]
        )
        
        pipeline = Pipeline(config=config)
        
        # This should handle authentication failure gracefully
        candidates = pipeline.discoverer._discover_emails("2026-01-09T00:00:00Z")
        assert isinstance(candidates, list)
        assert len(candidates) == 0  # Expected with mock credentials
