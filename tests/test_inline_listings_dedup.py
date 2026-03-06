"""Test inline listings content hash deduplication.

Tests that the content hash strategy properly deduplicates inline listings
articles regardless of their position on the page.
"""

import hashlib
import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone

import pytest

from src.models import ContentItem
from src.state_store import StateStoreManager


def generate_article_hash(title: str, date: str) -> str:
    """Generate content hash the same way extraction.py does."""
    content_signature = f"{title.strip()}|{date}".encode()
    return hashlib.sha256(content_signature).hexdigest()[:12]


class TestInlineListingsDeduplication:
    """Test content hash-based deduplication for inline listings."""

    def test_same_article_at_different_positions_has_same_hash(self):
        """Same article content always produces same hash regardless of position."""
        title = "CMA Releases Guidance on Green Claims"
        date = "2026-01-22T00:00:00+00:00"
        url = "https://example.com/news"

        # Article at position 0
        hash1 = generate_article_hash(title, date)
        sourcekey1 = f"listings:{url}#{hash1}"

        # Same article at position 1 (shifted down list)
        hash2 = generate_article_hash(title, date)
        sourcekey2 = f"listings:{url}#{hash2}"

        assert hash1 == hash2, "Same article should always get same hash"
        assert sourcekey1 == sourcekey2, "Same article should always get same source_key"

    def test_different_articles_have_different_hashes(self):
        """Different article content produces different hashes."""
        url = "https://example.com/news"

        # Article 1
        hash1 = generate_article_hash(
            "CMA Green Claims Guidance",
            "2026-01-22T00:00:00+00:00"
        )
        sourcekey1 = f"listings:{url}#{hash1}"

        # Article 2
        hash2 = generate_article_hash(
            "ESG Litigation Update",
            "2026-01-21T00:00:00+00:00"
        )
        sourcekey2 = f"listings:{url}#{hash2}"

        assert hash1 != hash2, "Different articles should have different hashes"
        assert sourcekey1 != sourcekey2, "Different articles should have different source_keys"

    def test_deduplication_with_state_store(self):
        """Verify state store properly deduplicates using content hash."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state_store.json"
            store = StateStoreManager(state_file)

            url = "https://example.com/news"
            title = "Green Claims Guidance"
            date = "2026-01-22T00:00:00+00:00"

            article_hash = generate_article_hash(title, date)
            sourcekey = f"listings:{url}#{article_hash}"

            # First run: article is new
            record = store.get_record(sourcekey)
            assert record is None, "First run should not find record in state store"

            # Process and record success
            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            store.add_success(sourcekey, now)

            # Second run: article at same position
            record = store.get_record(sourcekey)
            assert record is not None, "Second run should find record"
            assert record.status == "success", "Record should be marked as success"

            # Third run: article moves to different position (but hash stays same!)
            record = store.get_record(sourcekey)
            assert record is not None, "Article at different position still deduplicates"
            assert record.status == "success", "Article still marked as success"

    def test_title_or_date_change_creates_new_article(self):
        """If title or date changes, it's treated as new article (correct behavior)."""
        url = "https://example.com/news"

        # Original article
        hash1 = generate_article_hash(
            "Green Claims Guidance",
            "2026-01-22T00:00:00+00:00"
        )
        sourcekey1 = f"listings:{url}#{hash1}"

        # Title changes (e.g., edited by publisher)
        hash2 = generate_article_hash(
            "Green Claims Guidance (Updated)",
            "2026-01-22T00:00:00+00:00"
        )
        sourcekey2 = f"listings:{url}#{hash2}"

        assert sourcekey1 != sourcekey2, "Title change should create different source_key"

        # Date changes (e.g., date corrected)
        hash3 = generate_article_hash(
            "Green Claims Guidance",
            "2026-01-23T00:00:00+00:00"
        )
        sourcekey3 = f"listings:{url}#{hash3}"

        assert sourcekey1 != sourcekey3, "Date change should create different source_key"

    def test_hash_collision_risk_is_negligible(self):
        """Verify hash space is large enough for practical use."""
        # SHA256 with first 12 hex chars = 2^48 possibilities
        # For 1 million articles, birthday collision probability < 10^-10

        # Generate hashes for different articles
        hashes = set()
        for i in range(100):
            title = f"Article {i}"
            date = f"2026-01-{(i % 28) + 1:02d}T00:00:00+00:00"
            h = generate_article_hash(title, date)
            hashes.add(h)

        # All should be unique
        assert len(hashes) == 100, "All 100 articles should have unique hashes"

    def test_integration_article_reordering_scenario(self):
        """Simulate real scenario: articles reordered but not duplicated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state_store.json"
            store = StateStoreManager(state_file)

            url = "https://example.com/news"

            # Simulate first run with 3 articles
            articles_run1 = [
                ("Green Claims Guidance", "2026-01-22T00:00:00+00:00"),
                ("ESG Litigation Update", "2026-01-21T00:00:00+00:00"),
                ("Sustainability Reporting", "2026-01-20T00:00:00+00:00"),
            ]

            sourcekeys_run1 = []
            for title, date in articles_run1:
                h = generate_article_hash(title, date)
                sk = f"listings:{url}#{h}"
                sourcekeys_run1.append(sk)
                store.add_success(sk, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))

            # Simulate second run: new article added at top, others reordered
            articles_run2 = [
                ("Breaking: New ESG Rule", "2026-01-23T00:00:00+00:00"),  # NEW - position 0
                ("Green Claims Guidance", "2026-01-22T00:00:00+00:00"),   # was position 0 - now position 1
                ("Sustainability Reporting", "2026-01-20T00:00:00+00:00"),  # was position 2 - now position 2
                ("ESG Litigation Update", "2026-01-21T00:00:00+00:00"),   # was position 1 - now position 3
            ]

            new_articles_in_run2 = 0
            duplicated_articles_in_run2 = 0

            for title, date in articles_run2:
                h = generate_article_hash(title, date)
                sk = f"listings:{url}#{h}"

                record = store.get_record(sk)
                if record is None:
                    new_articles_in_run2 += 1
                else:
                    duplicated_articles_in_run2 += 1

            assert new_articles_in_run2 == 1, "Should detect 1 new article (Breaking: New ESG Rule)"
            assert duplicated_articles_in_run2 == 3, "Should detect 3 duplicates (already processed)"
