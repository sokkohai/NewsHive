"""Data models for newshive pipeline.

Defines schemas for ContentItem, Envelope, FailedItem, and StateStoreRecord
as specified in specs/core/DATA_MODEL.md.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar

from .text_cleaning import normalize_title


@dataclass
class ContentItem:
    """Represents a single piece of content discovered, extracted, and
    enriched by the pipeline.

    Implements the ContentItem schema from specs/core/DATA_MODEL.md.
    """

    # Required fields
    id: str
    source_type: str  # "email" or "web"
    source_key: str
    title: str
    summary: str
    content: str
    categories: list[str] = field(default_factory=list)  # Category tags
    published_at: str = ""  # ISO 8601 or "unknown"
    discovered_at: str = ""  # ISO 8601
    extracted_at: str = ""  # ISO 8601

    # Optional fields
    author: str | None = None
    source_url: str | None = None
    extraction_method: str | None = None
    links: list[str] | None = None
    email_sender: str | None = None
    email_subject: str | None = None
    email_folder: str | None = None
    # Language consistency field (per specs/core/LANGUAGE_CONSISTENCY.md)
    language_detected: str | None = None  # ISO 639-1 code: "de", "en", "fr", etc.
    # Email archival fields (per specs/core/EMAIL_ARCHIVAL.md)
    email_id: str | None = None
    email_folder_source: str | None = None
    email_archive_folder: str | None = None
    # Keywords that matched for categorization (per specs/core/CATEGORIZATION.md)
    keywords: list[str] | None = None  # Keywords that triggered categorization
    # Relevance scoring fields (per specs/core/RELEVANCE_SCORING.md)
    relevance_score: int | None = None  # Aggregated score 0-15 (or 0-17 with bonus)
    relevance_level: str | None = None  # "Niedrig", "Mittel", or "Hoch"
    relevance_dimensions: dict | None = None  # Per-dimension scores {d1_enforcement, d2_organ, ...}
    relevance_practice_area: str | None = None  # One of PRACTICE_AREAS values (RELEVANCE_SCORING.md)
    # Quality validation result from Unified Enrichment (per specs/core/DATA_MODEL.md)
    validation_status: str | None = None  # "PASS", "WARN", or "FAIL"

    def to_dict(self) -> dict[str, Any]:
        """Convert ContentItem to dictionary, omitting None optional fields."""
        result: dict[str, Any] = {
            "id": self.id,
            "source_type": self.source_type,
            "source_key": self.source_key,
            "title": normalize_title(self.title),  # Clean and limit title at output time
            "summary": self.summary,
            "categories": self.categories,
            "published_at": self.published_at,
            "discovered_at": self.discovered_at,
            "extracted_at": self.extracted_at,
        }

        # Add optional fields only if they are not None
        if self.author is not None:
            result["author"] = self.author
        if self.source_url is not None:
            result["source_url"] = self.source_url
        if self.extraction_method is not None:
            result["extraction_method"] = self.extraction_method
        if self.links is not None:
            result["links"] = self.links
        if self.email_sender is not None:
            result["email_sender"] = self.email_sender
        if self.email_subject is not None:
            result["email_subject"] = self.email_subject
        if self.email_folder is not None:
            result["email_folder"] = self.email_folder
        if self.language_detected is not None:
            result["language_detected"] = self.language_detected
        if self.email_id is not None:
            result["email_id"] = self.email_id
        if self.email_folder_source is not None:
            result["email_folder_source"] = self.email_folder_source
        if self.email_archive_folder is not None:
            result["email_archive_folder"] = self.email_archive_folder
        if self.keywords is not None:
            result["keywords"] = self.keywords
        if self.validation_status is not None:
            result["validation_status"] = self.validation_status
        if self.relevance_score is not None:
            result["relevance_score"] = self.relevance_score
        if self.relevance_level is not None:
            result["relevance_level"] = self.relevance_level
        if self.relevance_dimensions is not None:
            result["relevance_dimensions"] = self.relevance_dimensions
        if self.relevance_practice_area is not None:
            result["relevance_practice_area"] = self.relevance_practice_area

        return result

    def to_webhook_dict(self) -> dict[str, Any]:
        """Convert ContentItem to webhook payload format matching user schema."""
        return {
            "title": normalize_title(self.title),  # Clean and limit title at output time
            "date": self.published_at if self.published_at != "unknown" else self.extracted_at,
            "category": self.categories[0] if self.categories else None,
            "summary": self.summary,
            "keywords": self.keywords,  # Keywords that matched for categorization
            "url": self.source_url,
            "language": self.language_detected,
            "tags": self.categories,
            "raw_html": None,
            "source_type": self.source_type,
            "practice_areas": [self.relevance_practice_area] if self.relevance_practice_area else [],
        }

    def to_json(self) -> str:
        """Convert ContentItem to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


@dataclass
class FailedItem:
    """Represents a content item that failed processing at some stage or was filtered.

    Per specs/core/DEDUPLICATION.md and EXTRACTION.md, filtered items (too old, etc.)
    are tracked separately from genuine failures.

    Implements FailedItem schema from specs/core/DATA_MODEL.md.
    """

    id: str
    failure_stage: str  # "extraction", "summarization", "categorization", "deduplication"
    failure_reason: str  # "Article too old (>3 days)", "Extraction error: ...", etc.
    discovered_at: str | None = None  # ISO 8601 (optional)
    source_type: str | None = None  # "web" or "email" (optional)
    source_url: str | None = None  # Original URL or email info (optional)

    def to_dict(self) -> dict[str, Any]:
        """Convert FailedItem to dictionary, omitting None optional fields."""
        result = {
            "id": self.id,
            "failure_stage": self.failure_stage,
            "failure_reason": self.failure_reason,
        }
        if self.discovered_at is not None:
            result["discovered_at"] = self.discovered_at
        if self.source_type is not None:
            result["source_type"] = self.source_type
        if self.source_url is not None:
            result["source_url"] = self.source_url
        return result

    def to_json(self) -> str:
        """Convert FailedItem to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


@dataclass
class Envelope:
    """Output envelope containing the complete results of a pipeline execution.

    Implements the Envelope schema from specs/core/DATA_MODEL.md.
    """

    items: list[ContentItem] = field(default_factory=list)
    failed_items: list[FailedItem] = field(default_factory=list)
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )
    pipeline_version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        """Convert Envelope to dictionary."""
        result: dict[str, Any] = {
            "generated_at": self.generated_at,
            "pipeline_version": self.pipeline_version,
            "items": [item.to_dict() for item in self.items],
        }
        # Only include failed_items if not empty (per spec)
        if self.failed_items:
            result["failed_items"] = [item.to_dict() for item in self.failed_items]
        return result

    def to_json(self) -> str:
        """Convert Envelope to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


@dataclass
class StateStoreRecord:
    """Represents a record of a processed item in the State Store.

    Implements the StateStoreRecord schema from specs/core/DEDUPLICATION.md.
    """

    source_key: str
    processed_at: str  # ISO 8601 - when the article was processed
    status: str  # "success", "filtered", "extraction_failed", "summarization_failed", "categorization_failed", "enrichment_failed"
    article_date: str | None = None  # ISO 8601 - publication date of the article itself

    VALID_STATUSES: ClassVar = {
        "success",
        "filtered",
        "extraction_failed",
        "summarization_failed",
        "categorization_failed",
        "enrichment_failed",
    }

    def __post_init__(self) -> None:
        """Validate status value."""
        if self.status not in self.VALID_STATUSES:
            raise ValueError(
                f"Invalid status '{self.status}'. "
                f"Must be one of: {self.VALID_STATUSES}"
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert StateStoreRecord to dictionary."""
        result = {
            "source_key": self.source_key,
            "processed_at": self.processed_at,
            "status": self.status,
        }
        if self.article_date is not None:
            result["article_date"] = self.article_date
        return result


@dataclass
class StateStore:
    """In-memory representation of the persistent state store.

    Implements the State Store logic from specs/core/DEDUPLICATION.md.
    """

    last_run_timestamp: str  # ISO 8601
    items: dict[str, StateStoreRecord] = field(default_factory=dict)

    def add_record(self, record: StateStoreRecord) -> None:
        """Add or update a record in the state store."""
        self.items[record.source_key] = record

    def has_processed(self, source_key: str) -> bool:
        """Check if an item has been processed before."""
        return source_key in self.items

    def get_record(self, source_key: str) -> StateStoreRecord | None:
        """Get a record by source_key."""
        return self.items.get(source_key)

    def to_dict(self) -> dict[str, Any]:
        """Convert StateStore to dictionary for JSON serialization."""
        return {
            "last_run_timestamp": self.last_run_timestamp,
            "items": [record.to_dict() for record in self.items.values()],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StateStore":
        """Create a StateStore instance from dictionary (e.g., loaded from
        JSON)."""
        last_run_timestamp = data.get("last_run_timestamp")
        if not last_run_timestamp:
            # Default to 24 hours ago if not specified
            last_run_timestamp = (
                datetime.now(timezone.utc) - timedelta(hours=24)
            ).isoformat().replace("+00:00", "Z")

        store = cls(last_run_timestamp=last_run_timestamp)

        for record_data in data.get("items", []):
            record = StateStoreRecord(
                source_key=record_data["source_key"],
                processed_at=record_data["processed_at"],
                status=record_data["status"],
                article_date=record_data.get("article_date"),
            )
            store.add_record(record)

        return store

    def to_json(self) -> str:
        """Convert StateStore to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
