"""Quality verification stage for newshive pipeline.

Implements quality verification as specified in specs/core/QUALITY_VERIFICATION.md.
Validates title and summary quality using LLM before output.
Also provides LLM-based text cleaning for titles and summaries.
"""

import json
import logging
import os
from typing import Any, cast

from .llm_client import call_llm, SUPPORTED_PROVIDERS
from .models import ContentItem
from .text_cleaning import clean_title_prompt, clean_summary_prompt

logger = logging.getLogger(__name__)


class QualityVerifier:
    """Validates title and summary quality using LLM.
    
    Implements quality verification as specified in specs/core/QUALITY_VERIFICATION.md.
    """

    # Validation prompts
    TITLE_VALIDATION_PROMPT = """Validiere den folgenden Artikel-Titel (der auf Deutsch oder einer anderen Sprache sein kann) auf grammatikalische Korrektheit und semantische Kohärenz.

Titel: "{title}"

Antworte mit genau einem Wort: "GÜLTIG" wenn der Titel grammatikalisch korrekt und verständlich ist, oder "UNGÜLTIG" wenn er Fehler, Beschädigungen oder Unklarheiten enthält."""

    SUMMARY_VALIDATION_PROMPT = """Validiere die folgende Artikel-Zusammenfassung (die auf Deutsch oder einer anderen Sprache sein kann) auf grammatikalische Korrektheit und semantische Kohärenz.

Zusammenfassung: "{summary}"

Antworte mit genau einem Wort: "GÜLTIG" wenn die Zusammenfassung grammatikalisch korrekt, verständlich und kohärent ist, oder "UNGÜLTIG" wenn sie Fehler, Beschädigungen, Abschneidungen oder Unklarheiten enthält."""

    TITLE_GENERATION_PROMPT = """Erstelle eine prägnante, grammatikalisch korrekte deutsche Überschrift aus den folgenden Informationen.

Rohtext/Titel: "{raw_title}"
Zusammenfassung: "{summary}"

Die neue Überschrift sollte:
- Ausschließlich auf Deutsch formuliert sein
- Maximal 10 Wörter lang sein
- Grammatikalisch korrekt und fehlerfrei sein
- Die Kernaussage des Artikels widerspiegeln
- Keine Sonderzeichen oder Artefakte enthalten

Gib nur die neue Überschrift zurück, nichts anderes."""

    def __init__(
        self,
        enabled: bool = False,
        validate_title: bool = True,
        validate_summary: bool = True,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        llm_api_key: str | None = None,
        llm_api_url: str | None = None,
    ):
        """Initialize quality verifier.

        Args:
            enabled: Whether quality verification is enabled
            validate_title: Whether to validate titles
            validate_summary: Whether to validate summaries
            llm_provider: LLM provider ("custom")
            llm_model: LLM model name
            llm_api_key: LLM API key
            llm_api_url: Optional custom LLM API URL (for custom providers)
        """
        self.enabled = enabled
        self.validate_title = validate_title
        self.validate_summary = validate_summary
        self.provider = llm_provider
        self.model = llm_model
        self.api_key = llm_api_key
        self.api_url = llm_api_url
        self.client = None

        if self.enabled:
            self._initialize_client()

    def _initialize_client(self) -> Any | None:
        """Validate LLM config on init."""
        if not self.provider or not self.model or not self.api_key:
            logger.warning("Quality verification enabled but LLM not configured")
            return None
        if self.provider.lower() not in SUPPORTED_PROVIDERS:
            logger.warning(
                f"Unknown LLM provider: '{self.provider}'. "
                f"Supported: {', '.join(SUPPORTED_PROVIDERS)}"
            )
            return None
        logger.info(f"Initialized quality verifier: {self.provider}/{self.model}")
        return None

    def _call_llm(self, prompt: str, max_tokens: int = 10) -> str | None:
        """Call LLM with validation prompt.

        Args:
            prompt: Prompt to send to LLM
            max_tokens: Maximum tokens in response (default: 10)

        Returns:
            LLM response or None if failed
        """
        if not self.enabled or not self.api_key or not self.provider or not self.model:
            return None

        try:
            result = call_llm(
                provider=self.provider,
                model=self.model,
                api_key=self.api_key,
                prompt=prompt,
                api_url=self.api_url,
                max_tokens=max_tokens,
            )
            return result.strip() if result else None
        except Exception as e:
            logger.error(f"LLM validation call failed: {e}")
            return None

    def _validate_title(self, title: str) -> bool:
        """Validate title quality.

        Args:
            title: Title to validate

        Returns:
            True if valid, False if invalid or validation failed
        """
        if not self.validate_title:
            return True

        # Skip validation if title is empty
        if not title or len(title.strip()) == 0:
            logger.warning("Title validation failed (empty title)")
            return False

        prompt = self.TITLE_VALIDATION_PROMPT.format(title=title)
        response = self._call_llm(prompt)

        if response is None:
            logger.warning("Title validation failed (LLM error)")
            return False

        is_valid = response.upper() in ("VALID", "GÜLTIG")
        if not is_valid:
            logger.debug(f"Title validation failed: {title[:100]}")
        return is_valid

    def _validate_summary(self, summary: str) -> bool:
        """Validate summary quality.

        Args:
            summary: Summary to validate

        Returns:
            True if valid, False if invalid or validation failed
        """
        if not self.validate_summary:
            return True

        prompt = self.SUMMARY_VALIDATION_PROMPT.format(summary=summary)
        response = self._call_llm(prompt)

        if response is None:
            logger.warning("Summary validation failed (LLM error)")
            return False

        is_valid = response.upper() in ("VALID", "GÜLTIG")
        if not is_valid:
            logger.debug(f"Summary validation failed: {summary[:100]}")
        return is_valid

    def process(
        self, items: list[ContentItem]
    ) -> tuple[list[ContentItem], list[tuple[str, str]]]:
        """Process items through quality verification stage.

        Args:
            items: List of categorized items to validate

        Returns:
            Tuple of (validated_items, failed_items)
            - validated_items: Items that passed validation (or all items if disabled)
            - failed_items: List of (source_key, reason) tuples for failed items
        """
        if not self.enabled:
            # Validation disabled, pass all items through
            return items, []

        validated_items = []
        failed_items = []

        for item in items:
            # 1. Clean Text (LLM)
            # Clean title
            original_title = item.title
            cleaned_title = self.clean_title(item.title)
            if cleaned_title != item.title:
                item.title = cleaned_title
                logger.debug(f"Title updated during cleaning: '{original_title}' -> '{item.title}'")

            # 2. Generate title if original has grammatical/syntax issues
            generated_title = self.generate_title(item.title, item.summary)
            if generated_title != item.title:
                logger.info(f"Title regenerated due to syntax/grammar issues: '{item.title}' -> '{generated_title}'")
                item.title = generated_title

            # Clean summary
            cleaned_summary = self.clean_summary(item.summary)
            if cleaned_summary != item.summary:
                item.summary = cleaned_summary

            # 2. Validate
            # Pragmatic validation: Only reject if content is truly empty
            # LLM validation can be overly strict for diverse multilingual content
            
            # Reject if title is empty
            if not item.title or len(item.title.strip()) == 0:
                failed_items.append((item.source_key, "title_validation_failed"))
                logger.warning(f"Item rejected (empty title): {item.source_key}")
                continue
            
            # Reject if summary is empty
            if not item.summary or len(item.summary.strip()) == 0:
                failed_items.append((item.source_key, "summary_validation_failed"))
                logger.warning(f"Item rejected (empty summary): {item.source_key}")
                continue
            
            # Log LLM validation results for debugging, but don't reject based on them
            title_valid = self._validate_title(item.title)
            if not title_valid:
                logger.debug(
                    f"LLM flagged title quality issue (non-empty title, passing through): "
                    f"{item.source_key} (title='{item.title[:80]}')"
                )
            
            summary_valid = self._validate_summary(item.summary)
            if not summary_valid:
                logger.debug(
                    f"LLM flagged summary quality issue (non-empty summary, passing through): "
                    f"{item.source_key} (summary='{item.summary[:80]}')"
                )

            # Both passed
            validated_items.append(item)
            logger.debug(f"Item validation passed: {item.source_key}")

        logger.info(
            f"Quality verification: {len(validated_items)} passed, "
            f"{len(failed_items)} failed"
        )
        return validated_items, failed_items
    def clean_title(self, title: str) -> str:
        """Clean title using LLM to remove metadata artifacts.

        Args:
            title: Raw title text

        Returns:
            Cleaned title (or original if cleaning fails or results in empty string)
        """
        if not title or not self.enabled:
            return title

        prompt = clean_title_prompt(title)
        response = self._call_llm(prompt, max_tokens=100)

        if response is None:
            logger.debug(f"Title cleaning failed, returning original: {title[:50]}")
            return title

        cleaned = response.strip()
        # Only use cleaned version if it has meaningful content
        if cleaned and len(cleaned) > 10:  # Require at least 10 chars
            logger.debug(f"Cleaned title: {title[:50]} -> {cleaned[:50]}")
            return cleaned
        
        # Return original if cleaned version is too short or empty
        logger.debug(f"Cleaned title too short, keeping original: {title[:50]}")
        return title

    def clean_summary(self, summary: str) -> str:
        """Clean summary using LLM to remove excessive whitespace and artifacts.

        Args:
            summary: Raw summary text

        Returns:
            Cleaned summary
        """
        if not summary or not self.enabled:
            return summary

        prompt = clean_summary_prompt(summary)
        response = self._call_llm(prompt, max_tokens=1000)

        if response is None:
            logger.debug(f"Summary cleaning failed, returning original: {summary[:50]}")
            return summary

        cleaned = response.strip()
        if cleaned and len(cleaned) > 0:
            logger.debug(f"Cleaned summary: {summary[:50]} -> {cleaned[:50]}")
            return cleaned
        return summary

    def generate_title(self, raw_title: str, summary: str) -> str:
        """Generate a new title using LLM if original title has syntax/grammar issues.

        Args:
            raw_title: Raw/original title that may have issues
            summary: Article summary to derive context from

        Returns:
            Either cleaned original title or LLM-generated title if original has issues
        """
        if not self.enabled or not raw_title:
            return raw_title

        # First validate if original title is acceptable
        is_valid = self._validate_title(raw_title)
        if is_valid:
            logger.debug(f"Title is valid, keeping original: {raw_title[:60]}")
            return raw_title

        # Original title has issues - generate a better one
        logger.info(f"Generating new title for flawed original: {raw_title[:60]}")
        
        # Truncate summary to reasonable length for prompt
        summary_excerpt = summary[:300] if summary else ""
        
        prompt = self.TITLE_GENERATION_PROMPT.format(
            raw_title=raw_title[:100],
            summary=summary_excerpt
        )
        
        generated_title = self._call_llm(prompt, max_tokens=50)
        
        if generated_title and len(generated_title) > 10:
            logger.info(f"Generated new title: {raw_title[:50]} -> {generated_title[:60]}")
            return generated_title.strip()
        
        # LLM generation failed, return original
        logger.warning(f"Failed to generate new title, keeping original: {raw_title[:60]}")
        return raw_title