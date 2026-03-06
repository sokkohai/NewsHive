"""Test email discovery using O365 mocking.

Verifies that Discoverer can interact with the O365 library to fetch emails
and parse links.
"""

import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from src.discovery import Discoverer
from src.config import Configuration, WebSource, Category, EmailFolder


class TestEmailDiscovery(unittest.TestCase):
    """Test cases for email discovery logic."""

    def setUp(self):
        """Set up test configuration."""
        self.config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[EmailFolder("Inbox"), EmailFolder("Inbox/Compliance")],
            categories=[Category(name="compliance", keywords=[])]
        )
        self.discoverer = Discoverer(self.config)

    @patch("src.config.ConfigLoader.get_azure_config")
    @patch("O365.Account")
    def test_discover_emails_success(self, mock_account_cls, mock_get_azure):
        """Test successful email discovery with mocked O365."""
        # 1. Mock Azure config
        mock_get_azure.return_value = ("client_id", "client_secret", "tenant_id", "refresh_token")
        
        # 2. Mock O365 Account and Mailbox
        mock_account = MagicMock()
        mock_account_cls.return_value = mock_account
        mock_account.is_authenticated = True
        
        mock_mailbox = MagicMock()
        mock_account.mailbox.return_value = mock_mailbox
        
        # 3. Mock Folders
        mock_inbox = MagicMock()
        mock_compliance = MagicMock()
        
        # mailbox.get_folder(folder_name='Inbox')
        mock_mailbox.get_folder.return_value = mock_inbox
        # inbox.get_folder(folder_name='Compliance')
        mock_inbox.get_folder.return_value = mock_compliance
        
        # 4. Mock Messages
        mock_message = MagicMock()
        mock_message.subject = "New GDPR Compliance Alert"
        mock_message.body = '<a href="https://example.com/gdpr-article">Read about GDPR</a>'
        mock_message.object_id = "msg_123"
        mock_message.sender.address = "alerts@compliance.com"
        
        # folder.get_messages() returns a generator or list
        mock_inbox.get_messages.return_value = [mock_message]
        mock_compliance.get_messages.return_value = []
        
        # 5. Run discovery
        last_run = "2026-01-08T12:00:00Z"
        candidates = self.discoverer.discover(last_run_timestamp=last_run)
        
        # 6. Verify results
        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.title, "Read about GDPR")
        self.assertEqual(candidate.source_url, "https://example.com/gdpr-article")
        self.assertEqual(candidate.email_subject, "New GDPR Compliance Alert")
        self.assertEqual(candidate.email_sender, "alerts@compliance.com")
        
        # Verify O365 calls
        mock_mailbox.get_folder.assert_called_with(folder_name="Inbox")
        mock_inbox.get_folder.assert_called_with(folder_name="Compliance")
        
    @patch("src.config.ConfigLoader.get_azure_config")
    @patch("O365.Account")
    def test_discover_emails_authentication_failure(self, mock_account_cls, mock_get_azure):
        """Test email discovery when authentication fails."""
        mock_get_azure.return_value = ("client_id", "client_secret", "tenant_id", "refresh_token")
        
        mock_account = MagicMock()
        mock_account_cls.return_value = mock_account
        mock_account.is_authenticated = False
        
        candidates = self.discoverer._discover_emails(None)
        self.assertEqual(len(candidates), 0)

    def test_resolve_folder_path(self):
        """Test internal _resolve_folder helper."""
        mock_mailbox = MagicMock()
        mock_root = MagicMock()
        mock_sub = MagicMock()
        
        mock_mailbox.get_folder.return_value = mock_root
        mock_root.get_folder.return_value = mock_sub
        
        # Test valid path
        folder = self.discoverer._resolve_folder(mock_mailbox, "Inbox/Sub")
        self.assertEqual(folder, mock_sub)
        mock_mailbox.get_folder.assert_called_with(folder_name="Inbox")
        mock_root.get_folder.assert_called_with(folder_name="Sub")
        
        # Test invalid root
        mock_mailbox.get_folder.return_value = None
        folder = self.discoverer._resolve_folder(mock_mailbox, "Invalid/Path")
        self.assertIsNone(folder)


if __name__ == "__main__":
    unittest.main()
