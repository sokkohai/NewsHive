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
from .few_shot_examples import FewShotExampleStore
from .llm_deduplicator import LLMDeduplicator
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

        logger.info(
            "Relevance threshold configured: minimum %d articles with Mittel/Hoch relevance "
            "(fills with top-score Niedrig items if needed)",
            self.config.relevance_threshold,
        )

        # Store results path
        self.results_path = results_path or "./data/results.json"
        self.results_archive_dir = str(Path(self.results_path).parent / "results_archive")
        self.examples_path = Path("./article_llm_examples.yaml")
        self.examples_max_in_prompt = 15

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
        self._pending_discovery_drops: list[tuple[ContentItem, str]] = []

        # Initialize unified enricher if LLM is configured
        try:
            import os
            llm_provider, llm_model, llm_api_key = ConfigLoader.get_llm_config()
            llm_api_url = os.getenv("LLM_API_URL")
            few_shot_examples = self._load_examples()
            self.unified_enricher: UnifiedEnricher | None = UnifiedEnricher(
                self.config,
                llm_provider,
                llm_model,
                llm_api_key,
                llm_api_url,
                few_shot_examples=few_shot_examples,
                max_few_shot_examples=self.examples_max_in_prompt,
            )
            logger.info(f"Initialized Unified Enricher: {llm_provider}/{llm_model}")
        except ValueError as e:
            logger.warning(f"Unified enrichment disabled: {e}")
            logger.warning("Summarization will be skipped (no LLM configured)")
            self.unified_enricher = None

        self.llm_deduplicator: LLMDeduplicator | None = None
        try:
            import os
            llm_provider, llm_model, llm_api_key = ConfigLoader.get_llm_config()
            llm_api_url = os.getenv("LLM_API_URL")
            if self.config.llm_deduplication_stage_enabled:
                self.llm_deduplicator = LLMDeduplicator(
                    self.config,
                    llm_provider,
                    llm_model,
                    llm_api_key,
                    llm_api_url,
                )
                logger.info(f"Initialized LLM Deduplicator: {llm_provider}/{llm_model}")
            else:
                logger.info("LLM deduplication stage disabled (config/env)")
        except ValueError:
            pass
        except Exception as e:
            logger.warning(f"LLM deduplication initialization failed: {e}. Deduplication will be skipped.")
            self.llm_deduplicator = None

    def _load_examples(self) -> list:
        """Load curated few-shot examples from article_llm_examples.yaml."""
        store = FewShotExampleStore(self.examples_path)
        examples = store.load()
        if examples:
            logger.info("Loaded %d few-shot examples from %s", len(examples), self.examples_path)
            return examples
        logger.info("No few-shot examples found in %s", self.examples_path)
        return []

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

    def _apply_output_caps(
        self,
        items: list[ContentItem],
        execution_timestamp: str | None = None,
    ) -> list[ContentItem]:
        """Apply per-practice-area and global total caps, keeping highest-scored articles."""
        max_pa = self.config.max_articles_per_practice_area
        max_total = self.config.max_articles_total

        if not hasattr(self, "stats_cap_dropped"):
            self.stats_cap_dropped = 0

        if max_pa is None and max_total is None:
            return items

        def _score_sort_key(item: ContentItem) -> tuple[int, str]:
            score = item.relevance_score
            numeric = int(score) if isinstance(score, (int, float)) and score is not None else -1
            return (-numeric, item.source_key)

        result: list[ContentItem] = items
        cap_ts = execution_timestamp or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        if max_pa is not None:
            from collections import defaultdict
            grouped: dict[tuple[str, str], list[ContentItem]] = defaultdict(list)
            for item in result:
                category = item.categories[0] if item.categories else "Sonstiges"
                practice_area = item.relevance_practice_area or "Sonstiges"
                grouped[(category, practice_area)].append(item)

            capped: list[ContentItem] = []
            for (cat, pa), group in grouped.items():
                sorted_group = sorted(group, key=_score_sort_key)
                kept = sorted_group[:max_pa]
                dropped_items = sorted_group[max_pa:]
                dropped = len(sorted_group) - len(kept)
                if dropped:
                    self.stats_cap_dropped += dropped
                    logger.info(
                        f"  [CAP] Practice-area cap ({max_pa}): dropped {dropped} article(s) "
                        f"from '{cat}' / '{pa}'"
                    )
                    if hasattr(self, "state_store"):
                        for dropped_item in dropped_items:
                            article_date = (
                                dropped_item.published_at
                                if getattr(dropped_item, "published_at", None) not in (None, "", "unknown")
                                else None
                            )
                            self.state_store.add_cap_dropped(
                                dropped_item.source_key,
                                cap_ts,
                                article_date=article_date,
                                reason="cap_practice_area",
                            )
                capped.extend(kept)
            result = capped

        if max_total is not None and len(result) > max_total:
            sorted_result = sorted(result, key=_score_sort_key)
            dropped_items = sorted_result[max_total:]
            dropped_total = len(sorted_result) - max_total
            self.stats_cap_dropped += dropped_total
            result = sorted_result[:max_total]
            logger.info(
                f"  [CAP] Global cap ({max_total}): dropped {dropped_total} article(s) in total"
            )
            if hasattr(self, "state_store"):
                for dropped_item in dropped_items:
                    article_date = (
                        dropped_item.published_at
                        if getattr(dropped_item, "published_at", None) not in (None, "", "unknown")
                        else None
                    )
                    self.state_store.add_cap_dropped(
                        dropped_item.source_key,
                        cap_ts,
                        article_date=article_date,
                        reason="cap_global",
                    )

        return result

    def _apply_minimum_processed_fallback(
        self,
        enriched_items: list[ContentItem],
        extracted_items: list[ContentItem],
        enrichment_filtered: list[tuple[str, str]],
        minimum_items: int = 3,
    ) -> tuple[list[ContentItem], list[tuple[str, str]]]:
        """Fill processed items from low-relevance filtered items up to minimum."""
        if len(enriched_items) >= minimum_items or not enrichment_filtered:
            return enriched_items, enrichment_filtered

        extracted_by_key = {item.source_key: item for item in extracted_items}
        existing_keys = {item.source_key for item in enriched_items}
        seen_keys: set[str] = set()
        candidates: list[ContentItem] = []

        for source_key, reason in enrichment_filtered:
            normalized_reason = reason.strip().lower().removeprefix("filtered:").strip()
            normalized_reason = normalized_reason.removeprefix("relevance_level_too_low:").strip()
            if normalized_reason != "niedrig":
                continue
            if source_key in existing_keys or source_key in seen_keys:
                continue

            candidate = extracted_by_key.get(source_key)
            if candidate is None:
                continue

            seen_keys.add(source_key)
            candidates.append(candidate)

        if not candidates:
            return enriched_items, enrichment_filtered

        def _sort_key(item: ContentItem) -> tuple[int, int, str]:
            score = item.relevance_score
            has_numeric_score = isinstance(score, (int, float))
            numeric_score = int(score) if has_numeric_score and score is not None else -1
            return (0 if has_numeric_score else 1, -numeric_score, item.source_key)

        candidates.sort(key=_sort_key)

        needed = minimum_items - len(enriched_items)
        promoted_items = candidates[:needed]
        if not promoted_items:
            return enriched_items, enrichment_filtered

        promoted_keys = {item.source_key for item in promoted_items}
        remaining_filtered = [
            (source_key, reason)
            for source_key, reason in enrichment_filtered
            if source_key not in promoted_keys
        ]

        logger.info(
            "  Fallback activated: promoted %d low-relevance items to reach %d processed",
            len(promoted_items),
            minimum_items,
        )

        final_count = len(enriched_items) + len(promoted_items)
        if final_count < minimum_items:
            logger.warning(
                f"Relevance threshold {minimum_items} could not be reached. "
                f"Only {final_count} articles available (need {minimum_items - final_count} more)."
            )

        return enriched_items + promoted_items, remaining_filtered

    def _apply_preparation_fallback(
        self,
        prepared_items: list[ContentItem],
        preparation_failures: list[tuple[ContentItem, str | None]],
        minimum_items: int = 3,
    ) -> tuple[list[ContentItem], list[tuple[ContentItem, str | None]]]:
        """Promote preparation-filtered items when too few items remain."""
        if len(prepared_items) >= minimum_items or not preparation_failures:
            return prepared_items, preparation_failures

        candidates = [
            (item, reason)
            for item, reason in preparation_failures
            if reason == "article_text_insufficient" and (item.content or "").strip()
        ]
        if not candidates:
            return prepared_items, preparation_failures

        candidates.sort(
            key=lambda pair: (
                -len(pair[0].content or ""),
                pair[0].source_key,
            )
        )

        needed = minimum_items - len(prepared_items)
        promoted = [item for item, _ in candidates[:needed]]
        if not promoted:
            return prepared_items, preparation_failures

        promoted_keys = {item.source_key for item in promoted}
        remaining_failures = [
            pair
            for pair in preparation_failures
            if pair[0].source_key not in promoted_keys
        ]

        logger.info(
            "  Preparation fallback activated: promoted %d article(s) to reach %d candidates",
            len(promoted),
            minimum_items,
        )

        return prepared_items + promoted, remaining_failures

    def _log_execution_summary(self) -> None:
        """Log a compact execution summary for final pipeline outcome."""
        logger.info("\n" + "=" * 80)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Processed: {self.stats_processed}")
        logger.info(f"Failed:    {self.stats_failed}")
        logger.info(f"Filtered:  {self.stats_filtered}")
        logger.info(f"Deduped:   {self.stats_deduped}")
        logger.info(f"Keyword-filtered: {self.stats_keyword_filtered}")
        logger.info(f"Cap-dropped:      {self.stats_cap_dropped}")
        logger.info("=" * 80)

    def _record_discovery_drops(self, execution_timestamp: str) -> int:
        """Persist pending discovery drops to the state store."""
        pending_drops = getattr(self, "_pending_discovery_drops", [])
        persisted = 0
        for item, reason in pending_drops:
            article_date = item.published_at if getattr(item, "published_at", None) not in (None, "", "unknown") else None
            self.state_store.add_discovery_dropped(
                item.source_key,
                execution_timestamp,
                article_date=article_date,
                reason=reason,
            )
            persisted += 1

        self._pending_discovery_drops = []
        return persisted

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
        self.stats_deduped = 0
        self.stats_keyword_filtered = 0
        self.stats_cap_dropped = 0

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
                
                prepared_items, preparation_failures = self._apply_preparation_fallback(
                    prepared_items,
                    preparation_failures,
                    minimum_items=self.config.relevance_threshold,
                )

                logger.info(
                    f"  Prepared {len(prepared_items)} items "
                    f"({len(preparation_failures)} filtered)"
                )
                # Track stats
                self.stats_filtered += len(preparation_failures)
                
                # Log preparation failures
                if preparation_failures:
                    logger.info(f"  Failed preparation:")
                    for item, failure_reason in preparation_failures:
                        logger.info(f"    x {item.source_key} ({failure_reason})")
                
                # Use updated list for next stage
                extracted_items = prepared_items
                
                # Record preparation failures as skipped/filtered
                execution_timestamp_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                for item, failure_reason in preparation_failures:
                    reason_str = failure_reason or "Unknown preparation failure"
                    logger.info(f"  Filtered (preparation): {item.source_key} - {reason_str}")
                    article_date = item.published_at if hasattr(item, 'published_at') else None
                    self.state_store.add_filtered(item.source_key, execution_timestamp_str, article_date=article_date)
            
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

            article_dates: dict[str, str | None] = {
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
                    elif reason.startswith("relevance_level_too_low:"):
                        enrichment_filtered.append((key, reason.replace("relevance_level_too_low: ", "")))
                    else:
                        enrichment_failures.append((key, reason))

                enriched_items, enrichment_filtered = self._apply_minimum_processed_fallback(
                    enriched_items,
                    extracted_items,
                    enrichment_filtered,
                    minimum_items=self.config.relevance_threshold,
                )
                
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
                enrichment_article_dates: dict[str, str | None] = {
                    item.source_key: item.published_at 
                    for item in extracted_items if hasattr(item, 'published_at')
                }
                self._record_failures(
                    enrichment_failures, "enrichment_failed", envelope, article_dates=enrichment_article_dates
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
                for filtered_item, reason in categorization_filtered:
                    source_key = filtered_item.source_key if hasattr(filtered_item, 'source_key') else filtered_item[0] if isinstance(filtered_item, tuple) else str(filtered_item)
                    article_date = filtered_item.published_at if hasattr(filtered_item, 'published_at') else None
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

            # Stage 4.6: LLM-based Deduplication (similarity detection)
            if (
                self.config.llm_deduplication_stage_enabled
                and self.llm_deduplicator is not None
                and len(enriched_items) > 1
            ):
                logger.info("\n[STAGE 4.6] LLM-based Deduplication (Similarity Detection)")
                before_llm_dedup = len(enriched_items)
                enriched_items = self.llm_deduplicator.deduplicate(enriched_items)
                after_llm_dedup = len(enriched_items)
                if before_llm_dedup > after_llm_dedup:
                    deduped_count = before_llm_dedup - after_llm_dedup
                    self.stats_deduped += deduped_count
                    logger.info(
                        f"  Deduplicated {deduped_count} similar items "
                        f"({after_llm_dedup} unique items remain)"
                    )
                else:
                    logger.info(f"  No similar duplicates found ({after_llm_dedup} items)")
            elif not self.config.llm_deduplication_stage_enabled:
                logger.info("\n[STAGE 4.6] LLM-based Deduplication (disabled)")

            # Stage 4.7: Apply output caps (per practice area + global total)
            enriched_items = self._apply_output_caps(
                enriched_items,
                execution_timestamp=execution_timestamp,
            )

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
            sources: dict[str, int] = defaultdict(int)
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
            discovery_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            persisted_count = self._record_discovery_drops(discovery_ts)
            if persisted_count:
                logger.info(f"  Persisted {persisted_count} discovery-dropped items to state store")
        return filtered

    def _filter_fresh_candidates(self, candidates: list[ContentItem]) -> tuple[list[ContentItem], int, int]:
        """Keep only candidates newer than config.article_max_age_days.

        Unknown publication dates are discarded to avoid processing large
        backlogs when listings do not expose dates. A lightweight URL-based
        date inference is attempted before dropping unknowns.

        Returns:
            Tuple of (filtered_items, stale_count, undated_count)
        """
        if not hasattr(self, "_pending_discovery_drops"):
            self._pending_discovery_drops = []

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
                self._pending_discovery_drops.append((item, "undated"))
                undated_count += 1
                continue
            if dt < cutoff:
                logger.debug(
                    f"Dropping stale candidate: {item.source_key} (published {dt.isoformat()})"
                )
                self._pending_discovery_drops.append((item, "stale"))
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
        - BACKGROUND (async non-blocking): Webhook sending

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
            ResultsVersioning.write_daily_results(
                results_data,
                execution_timestamp,
                archive_dir=self.results_archive_dir,
            )

            # Cleanup old results (30-day retention)
            ResultsVersioning.cleanup_old_results(archive_dir=self.results_archive_dir)

            logger.info(f"  Output {len(items)} items")
        except Exception as e:
            logger.error(f"  Failed to write versioned results: {e}")

        # BACKGROUND OPERATIONS: Async non-blocking
        # Import ThreadPoolExecutor for async operations
        from concurrent.futures import ThreadPoolExecutor

        executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="pipeline-async")

        # Submit webhook sending as background task
        if self.config.webhook_url and self.config.webhook_enabled and items:
            executor.submit(self._send_webhook_async, items)
            logger.debug("  Webhook sending started (async)")
        elif self.config.webhook_url and not self.config.webhook_enabled:
            logger.debug("  Webhook disabled (WEBHOOK_ENABLED=false)")

        if self.config.webhook_url_structured and self.config.webhook_structured_enabled and items:
            executor.submit(self._send_structured_webhook_async, envelope, items)
            logger.debug("  Structured webhook sending started (async)")
        elif self.config.webhook_url_structured and not self.config.webhook_structured_enabled:
            logger.debug("  Structured webhook disabled (WEBHOOK_STRUCTURED_ENABLED=false)")

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

    def _send_structured_webhook_async(self, envelope: Envelope, items: list[ContentItem]) -> None:
        """Send structured webhook grouped by category and practice area."""
        try:
            payload = envelope.to_structured_webhook_dict(
                practice_areas_order=self.config.practice_areas_order
            )
            total_articles = sum(
                len(pa["articles"])
                for cat in payload["categories"]
                for pa in cat["practice_areas"]
            )
            logger.info(
                f"  [ASYNC] Sending structured webhook ({total_articles} articles, "
                f"{len(payload['categories'])} categories) to: "
                f"{self.config.webhook_url_structured}"
            )
            response = requests.post(
                self.config.webhook_url_structured,
                json=payload,
                timeout=30,
            )
            if 200 <= response.status_code < 300:
                logger.info("  [ASYNC] [OK] Structured webhook sent successfully")
            else:
                logger.error(
                    f"  [ASYNC] [FAILED] Structured webhook failed: {response.status_code} "
                    f"{response.text}"
                )
        except Exception as e:
            logger.error(f"  [ASYNC] [ERROR] Structured webhook error (non-fatal): {e}")

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

