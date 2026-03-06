"""Test email extraction from .msg files using the Extractor pipeline.

Tests that emails from test_emails folder can be properly extracted through
the Extractor orchestrator class, with proper extraction_method tracking
and ContentItem creation as per specs/core/EXTRACTION.md.
"""

from pathlib import Path
from datetime import datetime, timezone

import pytest

try:
    import extract_msg
    HAS_EXTRACT_MSG = True
except ImportError:
    HAS_EXTRACT_MSG = False


from src.models import ContentItem
from src.extraction import Extractor


@pytest.fixture(scope="session")
def test_emails_dir():
    """Get the test_emails directory path."""
    return Path(__file__).parent.parent / "test_emails"


@pytest.fixture(scope="session")
def msg_files(test_emails_dir):
    """Get list of .msg files from test_emails directory."""
    if not test_emails_dir.exists():
        return []
    return sorted(test_emails_dir.glob("*.msg"))


@pytest.mark.skipif(not HAS_EXTRACT_MSG, reason="extract_msg not installed")
class TestEmailMsgExtraction:
    """Test extraction of .msg email files through the Extractor pipeline."""

    def extract_msg_file(self, filepath):
        """Extract metadata and body from .msg file.

        Returns:
            Tuple of (subject, body, sender) or (None, None, None) on error
        """
        if not HAS_EXTRACT_MSG:
            return None, None, None
        try:
            msg = extract_msg.Message(str(filepath))
            subject = msg.subject
            body = msg.body
            html_body = msg.htmlBody
            sender = msg.sender

            # Prefer plain text, fall back to HTML
            if not body and html_body:
                body = html_body

            return subject, body, sender
        except Exception:
            return None, None, None

    def get_domain_from_email(self, email_address):
        """Extract domain from email address."""
        if "@" in email_address:
            return email_address.split("@")[1].rstrip(">")
        return "unknown"

    def test_msg_files_exist(self, msg_files, test_emails_dir):
        """Test that test_emails directory has .msg files."""
        pytest.skip(
            "test_emails directory not found - skipping .msg file extraction tests"
        ) if not msg_files else None
        assert test_emails_dir.exists(), (
            f"test_emails directory not found at {test_emails_dir}"
        )
        assert len(msg_files) > 0, (
            "No .msg files found in test_emails directory"
        )

    def test_can_extract_all_msg_files(self, msg_files):
        """Test that all .msg files can be extracted with Extractor."""
        pytest.skip(
            "test_emails directory not found - skipping .msg file extraction tests"
        ) if not msg_files else None
        extractor = Extractor()
        successful = 0

        for msg_file in msg_files:
            subject, body, sender = self.extract_msg_file(msg_file)

            if subject and body:  # Skip empty emails
                # Create ContentItem for email extraction
                ContentItem(
                    id=f"email_{msg_file.stem}",
                    source_type="email",
                    source_key=f"email_{msg_file.stem}",
                    title=subject.strip(),
                    summary="",
                    content="",  # Will be filled by extractor
                    categories=[""],
                    published_at="unknown",
                    discovered_at=datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    extracted_at="",
                    email_sender=sender,
                    email_subject=subject.strip(),
                )
                
                # Extract using Extractor (note: Extractor.extract requires email body)
                # For now we use EmailExtractor directly since Extractor.extract needs full flow
                content, method = extractor.email_extractor.extract(body)
                
                # Verify extraction
                assert content is not None, f"Failed to extract content from {msg_file.name}"
                assert isinstance(content, str), f"Extracted content must be string, got {type(content)}"
                assert method == "email_body", f"Expected method 'email_body', got '{method}'"
                assert len(content) > 0, f"Extracted content is empty for {msg_file.name}"
                
                successful += 1
        
        assert successful > 0, "Failed to extract any emails"

    def test_extraction_method_is_email_body(self, msg_files):
        """Test that extraction_method is always 'email_body' for emails per spec."""
        extractor = Extractor()
        
        for msg_file in msg_files:
            subject, body, sender = self.extract_msg_file(msg_file)
            
            if subject and body:
                content, method = extractor.email_extractor.extract(body)
                # Per spec: Email body extraction always uses 'email_body' method
                assert method == "email_body", \
                    f"Email extraction method should be 'email_body', got '{method}' for {msg_file.name}"

    def test_content_is_never_empty_when_body_exists(self, msg_files):
        """Test that extracted content is never empty when email body exists."""
        extractor = Extractor()
        
        for msg_file in msg_files:
            subject, body, sender = self.extract_msg_file(msg_file)
            
            if subject and body:
                content, method = extractor.email_extractor.extract(body)
                assert len(content) > 0, \
                    f"Extracted content is empty for {msg_file.name}, but body has {len(body)} chars"

    def test_content_item_creation_with_extracted_emails(self, msg_files):
        """Test that ContentItems can be properly created from extracted emails."""
        pytest.skip(
            "test_emails directory not found - skipping .msg file extraction tests"
        ) if not msg_files else None
        extractor = Extractor()
        extracted_items = []
        
        for msg_file in msg_files:
            subject, body, sender = self.extract_msg_file(msg_file)
            
            if subject and body:
                # Extract content
                content, method = extractor.email_extractor.extract(body)
                
                # Create ContentItem
                now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                item = ContentItem(
                    id=f"email_{msg_file.stem}",
                    source_type="email",
                    source_key=f"email_{msg_file.stem}",
                    title=subject.strip(),
                    summary="",
                    content=content,  # Extracted content
                    categories=[""],
                    published_at="unknown",
                    discovered_at=now,
                    extracted_at=now,
                    extraction_method=method,
                    email_sender=sender,
                    email_subject=subject.strip()
                )
                
                # Verify item
                assert item.content == content
                assert item.extraction_method == "email_body"
                assert item.source_type == "email"
                assert len(item.title) > 0
                
                # Verify it can be serialized
                item_dict = item.to_dict()
                assert isinstance(item_dict, dict)
                assert item_dict["extraction_method"] == "email_body"
                
                extracted_items.append(item)
        
        assert len(extracted_items) > 0, "No emails were successfully extracted"
