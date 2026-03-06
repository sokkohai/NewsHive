"""Test email discovery by parsing test_emails for links.

Verifies that emails are treated as listing pages and articles (links)
are extracted as individual ContentItems.
"""

from pathlib import Path
import pytest

try:
    import extract_msg
    HAS_EXTRACT_MSG = True
except ImportError:
    HAS_EXTRACT_MSG = False

from src.discovery import EmailDiscoverer


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
class TestEmailDiscoveryIntegration:
    """Test discovery of links from .msg email files."""

    def extract_msg_content(self, filepath):
        """Extract content from .msg file."""
        try:
            msg = extract_msg.Message(str(filepath))
            subject = msg.subject
            html_body = msg.htmlBody
            body = msg.body
            sender = msg.sender

            # Use HTML body if available, else text body
            content = html_body if html_body else body
            if isinstance(content, bytes):
                content = content.decode("utf-8", errors="replace")

            return subject, content, sender
        except Exception:
            return None, None, None

    def test_discovery_extracts_links(self, msg_files):
        """Test that EmailDiscoverer extracts separate items from emails."""
        pytest.skip(
            "test_emails directory not found - skipping .msg file extraction tests"
        ) if not msg_files else None
        discoverer = EmailDiscoverer()
        total_candidates = 0
        
        for msg_file in msg_files:
            subject, content, sender = self.extract_msg_content(msg_file)
            
            if not subject or not content:
                continue
                
            # Perform discovery
            candidates = discoverer.discover(
                email_body=content,
                email_subject=subject,
                email_id=msg_file.name,
                sender=sender
            )
            
            # Verify candidates
            assert isinstance(candidates, list)
            
            # Filter for valid candidates
            valid_candidates = [c for c in candidates if c.title and c.source_url]
            total_candidates += len(valid_candidates)
            
            if valid_candidates:
                first = valid_candidates[0]
                assert first.source_type == "web"
                assert first.source_key == first.source_url
                assert first.email_subject == subject
                assert first.email_sender == sender
        
        # We expect to find a significant number of candidates (based on previous run ~84)
        assert total_candidates > 0, "No article candidates discovered from emails"
        print(f"Discovered {total_candidates} total candidates from emails")
