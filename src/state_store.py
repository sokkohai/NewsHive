"""State Store persistence for deduplication and state tracking.

Implements State Store logic from specs/core/DEDUPLICATION.md.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .models import StateStore, StateStoreRecord


class StateStoreManager:
    """Manages loading, saving, and updating the persistent State Store.

    Implements State Store persistence as specified in
    specs/core/DEDUPLICATION.md.
    """

    DEFAULT_STATE_STORE_PATH = Path("./data/state_store.json")

    def __init__(self, path: Path | None = None):
        """Initialize the State Store manager.

        Args:
            path: Path to state store file. Defaults to ./data/state_store.json
        """
        self.path = path or self.DEFAULT_STATE_STORE_PATH
        self.store = self._load_or_create()

    def _load_or_create(self) -> StateStore:
        """Load State Store from file or create a new one if it doesn't exist."""
        if self.path.exists():
            try:
                with open(self.path, encoding="utf-8") as f:
                    data = json.load(f)
                return StateStore.from_dict(data)
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                raise ValueError(
                    f"Failed to load State Store from {self.path}: {e}. "
                    "The file may be corrupted. Please fix it or delete it "
                    "to start fresh."
                ) from e
        else:
            # Create new State Store with default look-back period
            default_timestamp = (
                datetime.now(timezone.utc) - timedelta(hours=24)
            ).isoformat().replace("+00:00", "Z")
            return StateStore(last_run_timestamp=default_timestamp)

    def save(self) -> None:
        """Persist the State Store to file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.store.to_dict(), f, ensure_ascii=False, indent=2)

    def has_processed(self, source_key: str) -> bool:
        """Check if an item has been processed before."""
        return self.store.has_processed(source_key)

    def get_record(self, source_key: str) -> StateStoreRecord | None:
        """Get a State Store record by source_key."""
        return self.store.get_record(source_key)

    def add_success(self, source_key: str, processed_at: str, article_date: str | None = None) -> None:
        """Record a successful item processing."""
        record = StateStoreRecord(
            source_key=source_key,
            processed_at=processed_at,
            status="success",
            article_date=article_date,
        )
        self.store.add_record(record)

    def add_filtered(self, source_key: str, processed_at: str, article_date: str | None = None) -> None:
        """Record an item that was filtered out (too old, no keywords match, etc.)
        
        This is NOT a failure - it's expected filtering behavior per spec.
        """
        record = StateStoreRecord(
            source_key=source_key,
            processed_at=processed_at,
            status="filtered",
            article_date=article_date,
        )
        self.store.add_record(record)

    def add_extraction_failure(
        self, source_key: str, processed_at: str, article_date: str | None = None
    ) -> None:
        """Record an extraction failure."""
        record = StateStoreRecord(
            source_key=source_key,
            processed_at=processed_at,
            status="extraction_failed",
            article_date=article_date,
        )
        self.store.add_record(record)

    def add_summarization_failure(
        self, source_key: str, processed_at: str, article_date: str | None = None
    ) -> None:
        """Record a summarization failure."""
        record = StateStoreRecord(
            source_key=source_key,
            processed_at=processed_at,
            status="summarization_failed",
            article_date=article_date,
        )
        self.store.add_record(record)

    def add_categorization_failure(
        self, source_key: str, processed_at: str, article_date: str | None = None
    ) -> None:
        """Record a categorization failure."""
        record = StateStoreRecord(
            source_key=source_key,
            processed_at=processed_at,
            status="categorization_failed",
            article_date=article_date,
        )
        self.store.add_record(record)

    def add_enrichment_failure(
        self, source_key: str, processed_at: str, article_date: str | None = None
    ) -> None:
        """Record an enrichment failure (unified enrichment stage)."""
        record = StateStoreRecord(
            source_key=source_key,
            processed_at=processed_at,
            status="enrichment_failed",
            article_date=article_date,
        )
        self.store.add_record(record)

    def update_last_run(self, timestamp: str) -> None:
        """Update the last execution timestamp."""
        self.store.last_run_timestamp = timestamp

    def get_last_run_timestamp(self) -> str:
        """Get the last execution timestamp."""
        return self.store.last_run_timestamp
