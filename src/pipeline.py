"""Main pipeline orchestration.

Implements the unified newshive pipeline as specified in
specs/core/PIPELINE.md.
"""

import json
import logging
import re
import requests
import time
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from .config import ConfigLoader, Configuration
from .article_preparation import ArticlePreparer
from .categorization import Categorizer
from .discovery import Discoverer
from .extraction import Extractor
from .models import ContentItem, Envelope, FailedItem
from .output_versioning import ResultsVersioning
from .state_store import StateStoreManager
from .unified_enricher import UnifiedEnricher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class Pipeline:
    """Main newshive pipeline orchestrator.

    Implements the unified pipeline as specified in specs/core/PIPELINE.md.

    Pipeline stages:
    1. Discovery: Identify new content from web and email sources
    2. Deduplication: Filter out previously processed items
    3. Extraction: Retrieve full article content
    4. Summarization: Generate LLM-based summaries
    5. Categorization: Assign topic tags
    6. Output: Produce JSON output and update state
    """

    def __init__(
        self,
        config: Configuration | None = None,
        state_store_path: str | None = None,
        results_path: str | None = None,
        auto_analyze: bool = False,
        site_filter: str | None = None,
    ):
        """Initialize the pipeline.

        Args:
            config: Configuration object. If None, loads from config.json
            state_store_path: Path to state store file. If None, uses default ./data/state_store.json
            results_path: Path to results file. If None, uses default ./data/results.json
            auto_analyze: If True, analyze sources and apply recommendations
                         before discovery stage
            site_filter: Optional site identifier to process only one web source
        """
        # Load configuration
        self.config = config or ConfigLoader.load()
        logger.info(
            f"Loaded configuration version {self.config.pipeline_version}"
        )
        self.auto_analyze = auto_analyze
        
        # Apply site filter if specified
        if site_filter:
            self.config = self._filter_config_by_site(site_filter)
            logger.info(f"Site filter applied: processing only '{site_filter}'")

        # Store results path
        self.results_path = results_path or "./data/results.json"

        # Load/initialize state store
        state_path = Path(state_store_path) if state_store_path else None
        self.state_store = StateStoreManager(state_path)
        logger.info(
            f"Loaded state store with {len(self.state_store.store.items)} "
            "processed items"
        )

        # Initialize pipeline components
        self.discoverer = Discoverer(
            self.config, state_store_manager=self.state_store
        )
        self.extractor = Extractor(
            article_max_age_days=self.config.article_max_age_days,
            config=self.config,
        )
        self.categorizer = Categorizer(self.config)

        # Initialize unified enricher if LLM is configured
        try:
            import os
            llm_provider, llm_model, llm_api_key = ConfigLoader.get_llm_config()
            llm_api_url = os.getenv("LLM_API_URL")
            self.unified_enricher: UnifiedEnricher | None = UnifiedEnricher(
                self.config,
                llm_provider,
                llm_model,
                llm_api_key,
                llm_api_url,
            )
            logger.info(f"Initialized Unified Enricher: {llm_provider}/{llm_model}")
        except ValueError as e:
            logger.warning(f"Unified enrichment disabled: {e}")
            logger.warning("Summarization will be skipped (no LLM configured)")
            self.unified_enricher = None

    def _filter_config_by_site(self, site_filter: str) -> Configuration:
        """Filter configuration to process only a specific web source.
        
        Args:
            site_filter: Site identifier (URL substring, domain, or full URL)
            
        Returns:
            New Configuration with filtered web_sources
            
        Raises:
            ValueError: If no matching source found
        """
        # Normalize site filter to lowercase for case-insensitive matching
        site_filter_lower = site_filter.lower()
        
        # First try exact domain match (e.g., "bakertilly.de", "bakertilly")
        matched_sources = []
        for source in self.config.web_sources:
            source_url_lower = source.url.lower()
            # Check for domain match or URL substring match
            if (site_filter_lower in source_url_lower or 
                site_filter_lower.replace('.', '') in source_url_lower.replace('.', '')):
                matched_sources.append(source)
        
        if not matched_sources:
            available_sites = [s.url for s in self.config.web_sources]
            raise ValueError(
                f"No web source found matching '{site_filter}'. "
                f"Available sources: {', '.join(available_sites)}"
            )
        
        logger.info(f"  Found {len(matched_sources)} matching source(s)")
        for source in matched_sources:
            logger.info(f"    - {source.url}")
        
        # Create new configuration with filtered sources
        return replace(self.config, web_sources=matched_sources)

    def run(self) -> Envelope:
        """Execute the complete pipeline.

        Returns:
            Envelope containing processed items and failed items
        """
        execution_timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        envelope = Envelope(
            generated_at=execution_timestamp,
            pipeline_version=self.config.pipeline_version,
        )

        # Initialize execution counters
        self.stats_processed = 0
        self.stats_failed = 0
        self.stats_filtered = 0

        logger.info("=" * 80)
        logger.info("newshive PIPELINE EXECUTION")
        logger.info(f"Execution timestamp: {execution_timestamp}")
        logger.info("=" * 80)

        try:
            # Pre-stage: Auto-analyze sources (optional)
            if self.auto_analyze:
                logger.info("\n[PRE-STAGE] Auto-analyze sources")
                self._auto_analyze_sources()

            # Stage 1: Discovery
            logger.info("\n[STAGE 1] Discovery")
            candidates = self._stage_discovery()
            logger.info(f"  Discovered {len(candidates)} candidate items")

            # Stage 2: Deduplication
            logger.info("\n[STAGE 2] Deduplication")
            new_items = self._stage_deduplication(candidates)
            skipped = len(candidates) - len(new_items)
            # Track stats - skipped items are filtered (already processed)
            self.stats_filtered += skipped
            logger.info(f"  {len(new_items)} new items (skipped {skipped})")

            if not new_items:
                logger.info("  No new items to process. Pipeline complete.")
                self.state_store.update_last_run(execution_timestamp)
                self.state_store.save()
                
                # Print final summary
                logger.info("\n" + "=" * 80)
                logger.info("EXECUTION SUMMARY")
                logger.info("=" * 80)
                logger.info(f"Processed: {self.stats_processed}")
                logger.info(f"Failed:    {self.stats_failed}")
                logger.info(f"Filtered:  {self.stats_filtered}")
                logger.info("=" * 80)
                
                return envelope

            # Stage 3: Extraction & Preparation
            logger.info("\n[STAGE 3] Extraction & Preparation")
            
            # Part 3a: Extraction
            # Using self._stage_extraction wrapper which handles details
            extracted_items, extraction_failures, filtered_items = (
                self._stage_extraction(new_items)
            )
            # Track stats
            self.stats_failed += len(extraction_failures)
            self.stats_filtered += len(filtered_items)
            
            # Log results
            logger.info(
                f"  Extracted {len(extracted_items)} items "
                f"({len(extraction_failures)} failed, {len(filtered_items)} filtered)"
            )
            # Log processed items
            if extracted_items:
                logger.info(f"  Processed items:")
                for item in extracted_items:
                    logger.info(f"    + {item.source_key}")
            # Log filtered items
            if filtered_items:
                logger.info(f"  Filtered items:")
                for key, reason in filtered_items:
                    logger.info(f"    - {key} ({reason})")
            # Log failed items
            if extraction_failures:
                logger.info(f"  Failed items:")
                for key, reason in extraction_failures:
                    logger.info(f"    x {key} ({reason})")
            
            # Part 3b: Preparation
            if extracted_items:
                preparer = ArticlePreparer(self.config)
                
                prepared_items = []
                preparation_failures = []
                
                for item in extracted_items:
                    success, result = preparer.prepare(item)
                    if success:
                        # item.content is updated in-place
                        prepared_items.append(item)
                    else:
                        # result contains failure reason
                        preparation_failures.append((item, result))
                
                logger.info(
                    f"  Prepared {len(prepared_items)} items "
                    f"({len(preparation_failures)} filtered)"
                )
                # Track stats
                self.stats_filtered += len(preparation_failures)
                
                # Log preparation failures
                if preparation_failures:
                    logger.info(f"  Failed preparation:")
                    for item, reason in preparation_failures:
                        logger.info(f"    x {item.source_key} ({reason})")
                
                # Use updated list for next stage
                extracted_items = prepared_items
                
                # Record preparation failures as skipped/filtered
                execution_timestamp_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                for item, reason in preparation_failures:
                     reason_str = reason or "Unknown preparation failure"
                     logger.info(f"  Filtered (preparation): {item.source_key} - {reason_str}")
                     self.state_store.add_filtered(item.source_key, reason_str, execution_timestamp_str)
            
            if not extracted_items:
                 logger.info("  No items passed extraction & preparation. Pipeline complete.")
                 self.state_store.update_last_run(execution_timestamp)
                 self.state_store.save()
                 
                 # Print final summary
                 logger.info("\n" + "=" * 80)
                 logger.info("EXECUTION SUMMARY")
                 logger.info("=" * 80)
                 logger.info(f"Processed: {self.stats_processed}")
                 logger.info(f"Failed:    {self.stats_failed}")
                 logger.info(f"Filtered:  {self.stats_filtered}")
                 logger.info("=" * 80)
                 
                 return envelope

            # Build article_dates map from new_items for extraction failures

            article_dates = {
                item.source_key: item.published_at 
                for item in new_items if hasattr(item, 'published_at')
            }
            self._record_failures(
                extraction_failures, "extraction_failed", envelope, article_dates=article_dates
            )
            # Filtered items are logged and recorded in state store as "filtered" (not failures)
            for key, reason in filtered_items:
                logger.debug(f"  Filtered: {key} - {reason}")
                self.state_store.add_filtered(key, execution_timestamp)

            if not extracted_items:
                logger.info(
                    "  No items successfully extracted. Pipeline complete."
                )
                self.state_store.update_last_run(execution_timestamp)
                self.state_store.save()
                
                # Print final summary
                logger.info("\n" + "=" * 80)
                logger.info("EXECUTION SUMMARY")
                logger.info("=" * 80)
                logger.info(f"Processed: {self.stats_processed}")
                logger.info(f"Failed:    {self.stats_failed}")
                logger.info(f"Filtered:  {self.stats_filtered}")
                logger.info("=" * 80)
                
                return envelope

            # Stage 4: Unified Enrichment
            if self.unified_enricher is not None:
                logger.info("\n[STAGE 4] Unified Enrichment (Single-Call JSON)")
                
                # We now always use the optimized single-call flow (.process())
                enriched_items, enrichment_results = (
                    self.unified_enricher.process(extracted_items)
                )
                
                # Separate filtered items from failed items
                enrichment_filtered = []
                enrichment_failures = []
                for key, reason in enrichment_results:
                    if reason.startswith("filtered:"):
                        enrichment_filtered.append((key, reason.replace("filtered: ", "")))
                    else:
                        enrichment_failures.append((key, reason))
                
                # Track stats
                self.stats_failed += len(enrichment_failures)
                self.stats_filtered += len(enrichment_filtered)

                logger.info(
                    f"  Enriched {len(enriched_items)} items "
                    f"({len(enrichment_failures)} failed, {len(enrichment_filtered)} filtered)"
                )
                # Log enrichment results
                if enriched_items:
                    logger.info(f"  Processed (Enriched):")
                    for item in enriched_items:
                        logger.info(f"    + {item.source_key} (Score: {item.relevance_score}, Level: {item.relevance_level})")
                if enrichment_filtered:
                    logger.info(f"  Filtered (enrichment):")
                    for key, reason in enrichment_filtered:
                        logger.info(f"    - {key} ({reason})")
                if enrichment_failures:
                    logger.info(f"  Failed enrichment:")
                    for key, reason in enrichment_failures:
                        logger.info(f"    x {key} ({reason})")
                # Build article_dates map from extracted_items for enrichment failures
                article_dates = {
                    item.source_key: item.published_at 
                    for item in extracted_items if hasattr(item, 'published_at')
                }
                self._record_failures(
                    enrichment_failures, "enrichment_failed", envelope, article_dates=article_dates
                )
                # Record filtered items in state store
                for key, reason in enrichment_filtered:
                    self.state_store.add_filtered(key, execution_timestamp)
            else:
                # Fallback path without LLM enrichment: local categorization only
                logger.info("\n[STAGE 4] Categorization (No LLM)")
                categorized_items, categorization_filtered = self.categorizer.process(extracted_items)
                
                # Track stats
                self.stats_filtered += len(categorization_filtered)
                
                logger.info(
                    f"  Categorized {len(categorized_items)} items "
                    f"({len(categorization_filtered)} filtered)"
                )
                # Log categorization failures
                if categorization_filtered:
                    logger.info(f"  Filtered (categorization):")
                
                # Record filtered items in state store
                for item, reason in categorization_filtered:
                    source_key = item.source_key if hasattr(item, 'source_key') else item[0] if isinstance(item, tuple) else item
                    article_date = item.published_at if hasattr(item, 'published_at') else None
                    logger.info(f"    - {source_key} ({reason})")
                    self.state_store.add_filtered(source_key, execution_timestamp, article_date=article_date)
                enriched_items = categorized_items

            if not enriched_items:
                logger.info(
                    "  No items passed enrichment/verification. Pipeline complete."
                )
                self.state_store.update_last_run(execution_timestamp)
                self.state_store.save()
                
                # Print final summary
                logger.info("\n" + "=" * 80)
                logger.info("EXECUTION SUMMARY")
                logger.info("=" * 80)
                logger.info(f"Processed: {self.stats_processed}")
                logger.info(f"Failed:    {self.stats_failed}")
                logger.info(f"Filtered:  {self.stats_filtered}")
                logger.info("=" * 80)
                
                return envelope

            # Stage 5: Output & State Update
            logger.info("\n[STAGE 5] Output & State Update")
            self._stage_output(enriched_items, execution_timestamp, envelope)
            # Track stats
            self.stats_processed += len(enriched_items)
            logger.info(f"  Output {len(enriched_items)} items")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            raise

        # Print final summary
        logger.info("\n" + "=" * 80)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Processed: {self.stats_processed}")
        logger.info(f"Failed:    {self.stats_failed}")
        logger.info(f"Filtered:  {self.stats_filtered}")
        logger.info("=" * 80)

        return envelope

    def _stage_discovery(self) -> list[ContentItem]:
        """Execute Discovery stage.

        Returns:
            List of candidate ContentItems from all sources
        """
        last_run = self.state_store.store.last_run_timestamp
        candidates = self.discoverer.discover(last_run_timestamp=last_run)

        # Log discovery breakdown by source
        if candidates:
            from collections import defaultdict
            sources = defaultdict(int)
            for item in candidates:
                # Extract domain from source_key/source_url
                domain = "unknown"
                if item.source_url:
                    from urllib.parse import urlparse
                    domain = urlparse(item.source_url).netloc
                sources[domain] += 1
            
            logger.info(f"  Found {len(candidates)} articles across {len(sources)} sources:")
            for domain, count in sorted(sources.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"    - {domain}: {count} article(s)")

        # Enforce freshness cut-off globally (post-discovery guardrail)
        filtered, stale_count, undated_count = self._filter_fresh_candidates(candidates)
        dropped = len(candidates) - len(filtered)
        if dropped > 0:
            logger.info(
                f"  Dropped {dropped} items after discovery: "
                f"{stale_count} stale, {undated_count} undated (kept {len(filtered)})"
            )
        return filtered

    def _filter_fresh_candidates(self, candidates: list[ContentItem]) -> tuple[list[ContentItem], int, int]:
        """Keep only candidates newer than config.article_max_age_days.

        Unknown publication dates are discarded to avoid processing large
        backlogs when listings do not expose dates. A lightweight URL-based
        date inference is attempted before dropping unknowns.

        Returns:
            Tuple of (filtered_items, stale_count, undated_count)
        """
        max_age_days = getattr(self.config, "article_max_age_days", 3)
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

        def parse_date(item: ContentItem) -> datetime | None:
            # Prefer published_at if present
            if item.published_at and item.published_at != "unknown":
                try:
                    return datetime.fromisoformat(item.published_at.replace("Z", "+00:00"))
                except Exception:
                    pass

            # Fallback: infer from URL (YYYY-MM-DD or YYYY/MM/DD)
            if item.source_url:
                match = re.search(r"(20\d{2})[-/](\d{2})[-/](\d{2})", item.source_url)
                if match:
                    try:
                        year, month, day = map(int, match.groups())
                        return datetime(year, month, day, tzinfo=timezone.utc)
                    except Exception:
                        pass
            return None

        fresh_items: list[tuple[datetime, ContentItem]] = []
        stale_count = 0
        undated_count = 0
        undated_by_domain: dict[str, int] = {}
        
        for item in candidates:
            dt = parse_date(item)
            if dt is None:
                # Track which domains have undated articles
                from urllib.parse import urlparse
                domain = urlparse(item.source_url).netloc if item.source_url else "unknown"
                undated_by_domain[domain] = undated_by_domain.get(domain, 0) + 1
                
                # Drop undated items to honor strict freshness requirement
                logger.debug(f"Dropping undated candidate: {item.source_key}")
                undated_count += 1
                continue
            if dt < cutoff:
                logger.debug(
                    f"Dropping stale candidate: {item.source_key} (published {dt.isoformat()})"
                )
                stale_count += 1
                continue
            fresh_items.append((dt, item))

        # Log undated article breakdown
        if undated_by_domain:
            logger.info(f"  Undated articles by domain:")
            for domain, count in sorted(undated_by_domain.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"    - {domain}: {count} article(s) missing dates")

        # Sort newest first to keep only freshest downstream
        fresh_items.sort(key=lambda pair: pair[0], reverse=True)
        return [item for _, item in fresh_items], stale_count, undated_count

    def _stage_deduplication(self, candidates: list[ContentItem]) -> list[ContentItem]:
        """Execute Deduplication stage.

        Filters candidates against the state store only. Date filtering
        is now performed in Discovery (first pass) and Extraction (second pass).

        Per updated spec/core/DEDUPLICATION.md:
        - Check State Store for already-processed items
        - New items are processed
        - Previously failed items are retried
        - Already-successful items are skipped
        - NO date filtering (redundant, handled in Discovery & Extraction)

        Args:
            candidates: Candidate items from discovery

        Returns:
            List of new/retry items
        """
        new_items = []

        for item in candidates:
            # Check state store for deduplication only
            record = self.state_store.get_record(item.source_key)

            if record is None:
                # New item - process it
                new_items.append(item)
            elif record.status == "success":
                # Already processed successfully - skip
                logger.debug(
                    f"  Skipping already-processed item: {item.source_key}"
                )
            else:
                # Previously failed (extraction_failed, summarization_failed,
                # or categorization_failed) - retry it
                logger.debug(
                    f"  Retrying previously failed item: {item.source_key} "
                    f"(status: {record.status})"
                )
                new_items.append(item)

        return new_items

    def _stage_extraction(
        self, items: list[ContentItem]
    ) -> tuple[list[ContentItem], list[tuple[str, str]], list[tuple[str, str]]]:
        """Execute Extraction stage.

        Per EXTRACTION.md, this stage:
        1. Extracts article content
        2. Applies keyword filtering to extracted content

        Args:
            items: Items to extract content for

        Returns:
            Tuple of (extracted_items, failed_items, filtered_items)
        """
        return self.extractor.process(items)

    def _record_failures(
        self,
        failures: list[tuple[str, str]],
        stage: str,
        envelope: Envelope,
        article_dates: dict[str, str | None] | None = None,
    ) -> None:
        """Record failed items in state store and envelope.

        Args:
            failures: List of (source_key, error_message) tuples
            stage: Stage name for the failure status
            envelope: Envelope to add failures to
            article_dates: Optional dict mapping source_key to article_date
        """
        if article_dates is None:
            article_dates = {}
            
        execution_timestamp = (
            datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )

        for source_key, error_message in failures:
            article_date = article_dates.get(source_key)
            # Record in state store
            if stage == "extraction_failed":
                self.state_store.add_extraction_failure(
                    source_key, execution_timestamp, article_date=article_date
                )
            elif stage == "summarization_failed":
                self.state_store.add_summarization_failure(
                    source_key, execution_timestamp, article_date=article_date
                )
            elif stage == "categorization_failed":
                self.state_store.add_categorization_failure(
                    source_key, execution_timestamp, article_date=article_date
                )
            elif stage == "enrichment_failed":
                self.state_store.add_enrichment_failure(
                    source_key, execution_timestamp, article_date=article_date
                )
            elif stage == "validation_failed":
                # Validation failures are recorded as enrichment failures
                self.state_store.add_enrichment_failure(
                    source_key, execution_timestamp, article_date=article_date
                )

            # Add to envelope
            failed_item = FailedItem(
                id=source_key,
                failure_stage=stage.replace("_failed", ""),
                failure_reason=error_message,
                discovered_at=execution_timestamp,
            )
            envelope.failed_items.append(failed_item)

    def _stage_output(
        self,
        items: list[ContentItem],
        execution_timestamp: str,
        envelope: Envelope,
    ) -> None:
        """Execute Output & State Update stage with async background operations.

        Per Phase 4 (Async Output Operations):
        - CRITICAL PATH (synchronous blocking): State updates, results writing
        - BACKGROUND (async non-blocking): Webhook sending, email archival

        Args:
            items: Successfully processed items
            execution_timestamp: Timestamp of pipeline execution
            envelope: Envelope to populate
        """
        # Add items to envelope
        envelope.items = items

        # CRITICAL PATH: State updates (must complete before returning)
        logger.info("  Updating state store...")
        for item in items:
            article_date = item.published_at if hasattr(item, 'published_at') else None
            self.state_store.add_success(item.source_key, execution_timestamp, article_date=article_date)

        self.state_store.update_last_run(execution_timestamp)
        self.state_store.save()
        num_items = len(self.state_store.store.items)
        logger.debug(f"  Persisted state store with {num_items} items")

        # CRITICAL PATH: Write versioned results (synchronous)
        try:
            results_data = [item.to_dict() for item in items]
            ResultsVersioning.write_results(results_data, execution_timestamp)
            
            # Cleanup old results (30-day retention)
            ResultsVersioning.cleanup_old_results()
            
            logger.info(f"  Output {len(items)} items")
        except Exception as e:
            logger.error(f"  Failed to write versioned results: {e}")

        # BACKGROUND OPERATIONS: Async non-blocking
        # Import ThreadPoolExecutor for async operations
        from concurrent.futures import ThreadPoolExecutor
        
        executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="pipeline-async")
        
        # Submit webhook sending as background task
        if self.config.webhook_url and items:
            executor.submit(self._send_webhook_async, items)
            logger.debug("  Webhook sending started (async)")
        
        # Submit email archival as background task
        email_items = [item for item in items if item.source_type == "email" and item.email_archive_folder]
        if email_items:
            executor.submit(self._archive_emails_async, email_items)
            logger.debug(f"  Email archival started (async, {len(email_items)} items)")
        
        # Don't wait for background tasks - pipeline continues immediately
        executor.shutdown(wait=False)
        
        logger.info("  State persisted, results written (background tasks running)")

    def _send_webhook_async(self, items: list[ContentItem]) -> None:
        """Send webhook in background (non-blocking).

        Called asynchronously by ThreadPoolExecutor. Errors are logged but
        do not block pipeline completion.

        Args:
            items: Items to send to webhook
        """
        try:
            # Convert items to webhook payload format (array of objects)
            payload = [item.to_webhook_dict() for item in items]

            logger.info(
                f"  [ASYNC] Sending {len(items)} items to webhook: "
                f"{self.config.webhook_url}"
            )
            response = requests.post(
                self.config.webhook_url, 
                json=payload,
                timeout=30
            )

            if 200 <= response.status_code < 300:
                logger.info("  [ASYNC] [OK] Webhook sent successfully")
            else:
                logger.error(
                    f"  [ASYNC] [FAILED] Webhook failed: {response.status_code} "
                    f"{response.text}"
                )
        except Exception as e:
            logger.error(f"  [ASYNC] [ERROR] Webhook error (non-fatal): {e}")

    def _archive_emails_async(self, email_items: list[ContentItem]) -> None:
        """Archive emails in background (non-blocking).

        Called asynchronously by ThreadPoolExecutor. Errors are logged but
        do not block pipeline completion.

        Args:
            email_items: Email items to archive
        """
        try:
            logger.info(f"  [ASYNC] Archiving {len(email_items)} emails...")
            for item in email_items:
                try:
                    self._move_email_with_retry(item)
                except Exception as e:
                    logger.warning(
                        f"  [ASYNC] Email archival failed for {item.email_id}: {e}"
                    )
            logger.info(f"  [ASYNC] [OK] Email archival complete")
        except Exception as e:
            logger.error(f"  [ASYNC] [ERROR] Email archival error (non-fatal): {e}")

    def _move_email_with_retry(self, item: ContentItem) -> None:
        """Move email to archive folder with retry logic.

        Per specs/core/EMAIL_ARCHIVAL.md:
        - Attempts move 3 times with exponential backoff (0s, 1s, 2s)
        - Logs failure but does not raise (non-fatal)
        - Only called for items with source_type="email" and archive_folder set

        Args:
            item: ContentItem with email_id and email_archive_folder set
        """
        # Pre-move validation
        if not item.email_id:
            logger.warning(
                f"  Email archival: email_id missing for item {item.source_key}, "
                "cannot archive"
            )
            return

        if not item.email_archive_folder:
            logger.debug(
                f"  Email archival: No archive folder configured, skipping move "
                "for {item.source_key}"
            )
            return

        try:
            from O365 import Account
            from .config import ConfigLoader

            # Get Azure configuration
            try:
                client_id, client_secret, tenant_id, refresh_token = (
                    ConfigLoader.get_azure_config()
                )
            except ValueError as e:
                logger.warning(
                    f"  Email archival: Azure configuration incomplete, "
                    f"skipping move: {e}"
                )
                return

            # Initialize account
            credentials = (client_id, client_secret)
            account = Account(credentials, tenant_id=tenant_id)

            if refresh_token:
                token = {
                    'refresh_token': refresh_token,
                    'access_token': None,
                    'expires_at': 0
                }
                account.connection.token_backend.save_token(token)

            if not account.is_authenticated:
                logger.warning("  Email archival: Outlook authentication failed, skipping move")
                return

            mailbox = account.mailbox()

            # Retry logic: 3 attempts with backoff (0s, 1s, 2s)
            max_retries = 3
            backoff_delays = [0, 1, 2]

            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        time.sleep(backoff_delays[attempt])

                    # Get the message object
                    message = mailbox.get_message(item.email_id)
                    if not message:
                        logger.warning(
                            f"  Email archival: Message {item.email_id} not found, "
                            f"skipping move"
                        )
                        return

                    # Get the archive folder
                    archive_folder = self._resolve_folder(
                        mailbox, item.email_archive_folder
                    )
                    if not archive_folder:
                        logger.error(
                            f"  Email archival: Archive folder "
                            f"{item.email_archive_folder} not found, skipping move"
                        )
                        return

                    # Move the message
                    message.move(archive_folder.folder_id)

                    logger.info(
                        f"  Email archival: Moved email {item.email_id} "
                        f"to {item.email_archive_folder}"
                    )
                    return

                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.debug(
                            f"  Email archival: Attempt {attempt + 1} failed "
                            f"for {item.email_id}: {e}. Retrying..."
                        )
                    else:
                        logger.warning(
                            f"  Email archival: Failed to move email "
                            f"{item.email_id} to {item.email_archive_folder} "
                            f"after 3 retries: {e}"
                        )
                        return

        except ImportError:
            logger.warning(
                "  Email archival: O365 library not installed, skipping move"
            )
        except Exception as e:
            logger.error(f"  Email archival: Unexpected error: {e}")

    def _resolve_folder(self, mailbox: Any, folder_path: str) -> Any:
        """Resolve a folder path to an O365 folder object.

        Args:
            mailbox: O365 mailbox object
            folder_path: Path to folder (e.g., "Inbox/Archive")

        Returns:
            O365 folder object or None
        """
        try:
            parts = folder_path.strip('/').split('/')
            current_folder = None

            for part in parts:
                if current_folder is None:
                    current_folder = mailbox.get_folder(folder_name=part)
                else:
                    current_folder = current_folder.get_folder(folder_name=part)

                if not current_folder:
                    return None

            return current_folder
        except Exception:
            return None

    def _auto_analyze_sources(self) -> None:
        """Auto-analyze sources and apply extraction rules (Pre-stage).

        When auto_analyze=True, this runs before Discovery to:
        1. Analyze all configured web sources
        2. Apply recommended extraction rules to config
        3. Update discovery behavior with new rules

        Logs analysis results but continues pipeline even if analysis fails.
        """
        try:
            # Import here to avoid circular dependency
            from tools.analyze_source_structure import (
                analyze_all_sources,
                apply_analysis_results,
            )

            logger.info("  Analyzing web source structures...")
            report = analyze_all_sources()

            if report.get("status") == "error":
                logger.warning(
                    f"  Analysis failed: {report.get('error')}"
                )
                return

            successful = sum(
                1 for s in report.get("sources", [])
                if s.get("status") == "success"
            )
            total = report.get("total_sources", 0)
            logger.info(f"  Analyzed {total} sources ({successful} successful)")

            # Apply recommendations
            logger.info("  Applying analysis recommendations to config...")
            apply_analysis_results()

            logger.info(
                "  Auto-analysis complete: extraction rules applied"
            )

        except ImportError:
            logger.warning(
                "  Analysis tools not available. Skipping auto-analysis."
            )
        except Exception as e:
            logger.warning(f"  Auto-analysis failed: {e}")
            logger.info("  Continuing with current configuration")
