"""Test email archival functionality per specs/core/EMAIL_ARCHIVAL.md."""

import os
import pytest
from unittest.mock import patch, MagicMock, call

from src.config import Configuration, Category, EmailFolder
from src.models import ContentItem
from src.pipeline import Pipeline
from datetime import datetime, timezone


class TestEmailFolderConfig:
    """Test EmailFolder configuration model."""

    def test_email_folder_from_dict_with_archive(self):
        """Test EmailFolder with archive_folder configuration."""
        data = {
            "folder_path": "Inbox/Newsletters",
            "archive_folder": "Inbox/processed_newsletter"
        }
        folder = EmailFolder.from_dict(data)
        assert folder.folder_path == "Inbox/Newsletters"
        assert folder.archive_folder == "Inbox/processed_newsletter"

    def test_email_folder_from_dict_without_archive(self):
        """Test EmailFolder without archive_folder (archival disabled)."""
        data = {"folder_path": "Inbox/Alerts"}
        folder = EmailFolder.from_dict(data)
        assert folder.folder_path == "Inbox/Alerts"
        assert folder.archive_folder is None

    def test_email_folder_to_dict(self):
        """Test EmailFolder serialization."""
        folder = EmailFolder("Inbox/Newsletters", "Inbox/processed_newsletter")
        result = folder.to_dict()
        assert result["folder_path"] == "Inbox/Newsletters"
        assert result["archive_folder"] == "Inbox/processed_newsletter"


class TestConfigurationEmailFolders:
    """Test Configuration with email folder archival."""

    def test_config_with_email_folder_objects(self):
        """Test Configuration accepts EmailFolder objects."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[
                EmailFolder("Inbox/Newsletters", "Inbox/processed_newsletter")
            ],
            categories=[],
        )
        assert len(config.email_folders) == 1
        assert config.email_folders[0].folder_path == "Inbox/Newsletters"
        assert config.email_folders[0].archive_folder == "Inbox/processed_newsletter"

    def test_config_from_dict_new_format(self):
        """Test new EmailFolder dict format with archive_folder."""
        data = {
            "pipeline_version": "1.0",
            "web_sources": [],
            "email_folders": [
                {
                    "folder_path": "Inbox/Newsletters",
                    "archive_folder": "Inbox/processed_newsletter"
                }
            ],
            "categories": [{"name": "test", "keywords": []}]
        }
        config = Configuration.from_dict(data)
        assert len(config.email_folders) == 1
        assert config.email_folders[0].folder_path == "Inbox/Newsletters"
        assert config.email_folders[0].archive_folder == "Inbox/processed_newsletter"

    def test_config_validation_email_folder_missing_path(self):
        """Test validation fails if folder_path is empty."""
        from src.config import ConfigError
        data = {
            "pipeline_version": "1.0",
            "web_sources": [],
            "email_folders": [{"folder_path": ""}],
            "keywords": ["test"],
            "categories": []
        }
        with pytest.raises(ConfigError):
            Configuration.from_dict(data)


class TestContentItemArchivalFields:
    """Test ContentItem with email archival fields."""

    def test_content_item_with_archival_fields(self):
        """Test ContentItem stores archival metadata."""
        item = ContentItem(
            id="url1",
            source_type="web",
            source_key="url1",
            title="Test",
            summary="",
            content="",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-15T00:00:00Z",
            extracted_at="",
            email_id="msg-123",
            email_folder_source="Inbox/Newsletters",
            email_archive_folder="Inbox/processed_newsletter",
        )
        assert item.email_id == "msg-123"
        assert item.email_folder_source == "Inbox/Newsletters"
        assert item.email_archive_folder == "Inbox/processed_newsletter"

    def test_content_item_to_dict_includes_archival_fields(self):
        """Test ContentItem.to_dict() includes optional archival fields."""
        item = ContentItem(
            id="url1",
            source_type="web",
            source_key="url1",
            title="Test",
            summary="",
            content="",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-15T00:00:00Z",
            extracted_at="",
            email_id="msg-123",
            email_folder_source="Inbox/Newsletters",
            email_archive_folder="Inbox/processed_newsletter",
        )
        result = item.to_dict()
        assert result["email_id"] == "msg-123"
        assert result["email_folder_source"] == "Inbox/Newsletters"
        assert result["email_archive_folder"] == "Inbox/processed_newsletter"

    def test_content_item_to_dict_omits_none_archival_fields(self):
        """Test ContentItem.to_dict() omits None archival fields."""
        item = ContentItem(
            id="url1",
            source_type="web",
            source_key="url1",
            title="Test",
            summary="",
            content="",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-15T00:00:00Z",
            extracted_at="",
            email_id=None,
            email_folder_source=None,
            email_archive_folder=None,
        )
        result = item.to_dict()
        assert "email_id" not in result
        assert "email_folder_source" not in result
        assert "email_archive_folder" not in result


class TestEmailArchivalMove:
    """Test email move functionality in Pipeline."""

    @patch("src.pipeline.Pipeline._resolve_folder")
    @patch("O365.Account")
    @patch.dict(os.environ, {
        "AZURE_CLIENT_ID": "test-id",
        "AZURE_CLIENT_SECRET": "test-secret",
        "AZURE_TENANT_ID": "test-tenant",
    })
    def test_move_email_success(self, mock_account_class, mock_resolve_folder):
        """Test successful email move to archive."""
        # Setup mock
        mock_account = MagicMock()
        mock_account_class.return_value = mock_account
        mock_account.is_authenticated = True
        
        mock_mailbox = MagicMock()
        mock_account.mailbox.return_value = mock_mailbox
        
        mock_message = MagicMock()
        mock_mailbox.get_message.return_value = mock_message
        
        mock_archive_folder = MagicMock()
        mock_archive_folder.folder_id = "archive-id"
        mock_resolve_folder.return_value = mock_archive_folder

        # Create pipeline and item
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[],
            categories=[Category(name="test", keywords=["test"])],
        )
        pipeline = Pipeline(config=config)

        item = ContentItem(
            id="url1",
            source_type="email",
            source_key="url1",
            title="Test",
            summary="",
            content="",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-15T00:00:00Z",
            extracted_at="",
            email_id="msg-123",
            email_folder_source="Inbox/Newsletters",
            email_archive_folder="Inbox/processed_newsletter",
        )

        # Call move
        pipeline._move_email_with_retry(item)

        # Verify
        mock_mailbox.get_message.assert_called_once_with("msg-123")
        mock_message.move.assert_called_once_with("archive-id")

    @patch("src.pipeline.Pipeline._resolve_folder")
    @patch("O365.Account")
    @patch.dict(os.environ, {
        "AZURE_CLIENT_ID": "test-id",
        "AZURE_CLIENT_SECRET": "test-secret",
        "AZURE_TENANT_ID": "test-tenant",
    })
    def test_move_email_retry_on_failure(self, mock_account_class, mock_resolve_folder):
        """Test retry logic: fails twice, succeeds on third attempt."""
        # Setup mock
        mock_account = MagicMock()
        mock_account_class.return_value = mock_account
        mock_account.is_authenticated = True
        
        mock_mailbox = MagicMock()
        mock_account.mailbox.return_value = mock_mailbox
        
        mock_message = MagicMock()
        mock_mailbox.get_message.return_value = mock_message
        
        # Fail twice, then succeed
        mock_message.move.side_effect = [
            Exception("Network error"),
            Exception("Timeout"),
            None  # Success
        ]
        
        mock_archive_folder = MagicMock()
        mock_archive_folder.folder_id = "archive-id"
        mock_resolve_folder.return_value = mock_archive_folder

        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[],
            categories=[Category(name="test", keywords=["test"])],
        )
        pipeline = Pipeline(config=config)

        item = ContentItem(
            id="url1",
            source_type="email",
            source_key="url1",
            title="Test",
            summary="",
            content="",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-15T00:00:00Z",
            extracted_at="",
            email_id="msg-123",
            email_folder_source="Inbox/Newsletters",
            email_archive_folder="Inbox/processed_newsletter",
        )

        # Call move
        pipeline._move_email_with_retry(item)

        # Verify move was called 3 times
        assert mock_message.move.call_count == 3

    @patch("O365.Account")
    @patch.dict(os.environ, {
        "AZURE_CLIENT_ID": "test-id",
        "AZURE_CLIENT_SECRET": "test-secret",
        "AZURE_TENANT_ID": "test-tenant",
    })
    def test_move_email_missing_email_id(self, mock_account_class):
        """Test move skipped when email_id is missing."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[],
            categories=[Category(name="test", keywords=["test"])],
        )
        pipeline = Pipeline(config=config)

        item = ContentItem(
            id="url1",
            source_type="email",
            source_key="url1",
            title="Test",
            summary="",
            content="",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-15T00:00:00Z",
            extracted_at="",
            email_id=None,  # Missing
            email_archive_folder="Inbox/processed_newsletter",
        )

        # Should not raise, just log and return
        pipeline._move_email_with_retry(item)
        mock_account_class.assert_not_called()

    @patch("O365.Account")
    @patch.dict(os.environ, {
        "AZURE_CLIENT_ID": "test-id",
        "AZURE_CLIENT_SECRET": "test-secret",
        "AZURE_TENANT_ID": "test-tenant",
    })
    def test_move_email_missing_archive_folder(self, mock_account_class):
        """Test move skipped when archive_folder is not configured."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[],
            categories=[Category(name="test", keywords=["test"])],
        )
        pipeline = Pipeline(config=config)

        item = ContentItem(
            id="url1",
            source_type="email",
            source_key="url1",
            title="Test",
            summary="",
            content="",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-15T00:00:00Z",
            extracted_at="",
            email_id="msg-123",
            email_archive_folder=None,  # Not configured
        )

        # Should not raise, just log and return
        pipeline._move_email_with_retry(item)
        mock_account_class.assert_not_called()

    @patch("src.pipeline.Pipeline._resolve_folder")
    @patch("O365.Account")
    @patch.dict(os.environ, {
        "AZURE_CLIENT_ID": "test-id",
        "AZURE_CLIENT_SECRET": "test-secret",
        "AZURE_TENANT_ID": "test-tenant",
    })
    def test_move_email_all_retries_fail(self, mock_account_class, mock_resolve_folder):
        """Test logging when all 3 retries fail."""
        # Setup mock
        mock_account = MagicMock()
        mock_account_class.return_value = mock_account
        mock_account.is_authenticated = True
        
        mock_mailbox = MagicMock()
        mock_account.mailbox.return_value = mock_mailbox
        
        mock_message = MagicMock()
        mock_mailbox.get_message.return_value = mock_message
        
        # Fail all attempts
        mock_message.move.side_effect = Exception("Permanent failure")
        
        mock_archive_folder = MagicMock()
        mock_archive_folder.folder_id = "archive-id"
        mock_resolve_folder.return_value = mock_archive_folder

        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[],
            categories=[Category(name="test", keywords=["test"])],
        )
        pipeline = Pipeline(config=config)

        item = ContentItem(
            id="url1",
            source_type="email",
            source_key="url1",
            title="Test",
            summary="",
            content="",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-15T00:00:00Z",
            extracted_at="",
            email_id="msg-123",
            email_folder_source="Inbox/Newsletters",
            email_archive_folder="Inbox/processed_newsletter",
        )

        # Should not raise, logs warning
        pipeline._move_email_with_retry(item)

        # Verify move was called 3 times
        assert mock_message.move.call_count == 3

    @patch("src.pipeline.Pipeline._move_email_with_retry")
    def test_stage_output_calls_move_for_email_items(self, mock_move):
        """Test _stage_output calls move for email items with archive folder."""
        from src.models import Envelope
        
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[],
            categories=[Category(name="test", keywords=["test"])],
        )
        pipeline = Pipeline(config=config)

        # Create mixed items (web and email)
        web_item = ContentItem(
            id="url1",
            source_type="web",
            source_key="url1",
            title="Web Article",
            summary="",
            content="test",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-15T00:00:00Z",
            extracted_at="2026-01-15T00:01:00Z",
        )

        email_item = ContentItem(
            id="url2",
            source_type="email",
            source_key="url2",
            title="Email Newsletter Link",
            summary="",
            content="test",
            categories=[],
            published_at="unknown",
            discovered_at="2026-01-15T00:00:00Z",
            extracted_at="2026-01-15T00:01:00Z",
            email_id="msg-123",
            email_archive_folder="Inbox/processed",
        )

        envelope = Envelope()
        execution_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        # Call _stage_output
        pipeline._stage_output([web_item, email_item], execution_ts, envelope)

        # Verify move was called only for email item
        mock_move.assert_called_once()
        call_args = mock_move.call_args
        assert call_args[0][0].source_type == "email"
        assert call_args[0][0].email_id == "msg-123"
