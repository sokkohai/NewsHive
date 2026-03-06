"""Summarization stage implementation.

Implements LLM-based summarization as specified in specs/core/SUMMARIZATION.md
and language consistency as specified in specs/core/LANGUAGE_CONSISTENCY.md.
"""

import logging
import re
from typing import Any

from .language_detection import get_language_name
from .llm_client import call_llm, SUPPORTED_PROVIDERS
from .models import ContentItem

logger = logging.getLogger(__name__)

# Threshold for content length that's short enough to use as-is
SHORT_CONTENT_MAX_CHARS = 500

# All LLM-generated summary/title outputs must be in German
TARGET_OUTPUT_LANGUAGE_CODE = "de"
TARGET_OUTPUT_LANGUAGE_NAME = "Deutsch"

# Patterns that indicate an LLM meta-response (not a valid summary)
LLM_META_RESPONSE_PATTERNS = [
    r"I don'?t have",
    r"I cannot",
    r"I can'?t",
    r"please (provide|paste|share|send)",
    r"no (article|text|content) (text|provided|available)",
    r"article text is (missing|not provided|unavailable)",
    r"full article",
    r"which do you prefer",
    r"once you provide",
    r"if you want,? I can",
]


class Summarizer:
    """Generates LLM-based summaries of extracted articles.

    Implements summarization as specified in specs/core/SUMMARIZATION.md.
    """

    def __init__(
        self,
        provider: str,
        model: str,
        api_key: str,
        api_url: str | None = None,
    ):
        """Initialize the Summarizer.

        Args:
            provider: LLM provider ("openai", "anthropic", or "custom")
            model: Model identifier within the provider
            api_key: API authentication key
            api_url: Custom API endpoint URL (required for provider="custom")
        """
        self.provider = provider
        self.model = model
        self.api_key = api_key
        self.api_url = api_url
        self._client: Any | None = self._initialize_client()

    def _initialize_client(self) -> Any | None:
        """Validate provider on init. Actual client is created lazily in call_llm."""
        if self.provider.lower() not in SUPPORTED_PROVIDERS:
            raise ValueError(
                f"Unknown LLM provider: '{self.provider}'. "
                f"Supported: {', '.join(SUPPORTED_PROVIDERS)}"
            )
        return None

    def _call_llm(self, prompt: str) -> str:
        """Call the LLM API with the given prompt.

        Args:
            prompt: The prompt to send to the LLM

        Returns:
            The LLM's response text

        Raises:
            RuntimeError: If the LLM API call fails
        """
        try:
            return call_llm(
                provider=self.provider,
                model=self.model,
                api_key=self.api_key,
                prompt=prompt,
                api_url=self.api_url,
            )
        except Exception as e:
            raise RuntimeError(f"LLM API call failed: {e}") from e

    def _is_meta_response(self, text: str) -> bool:
        """Check if the LLM output is a meta-response instead of a valid summary.
        
        Meta-responses are when the LLM says things like "I don't have the article"
        instead of actually summarizing the content.
        
        Args:
            text: The LLM output to check
            
        Returns:
            True if the text appears to be a meta-response
        """
        text_lower = text.lower()
        for pattern in LLM_META_RESPONSE_PATTERNS:
            if re.search(pattern, text_lower, re.IGNORECASE):
                logger.debug(f"Detected meta-response pattern: {pattern}")
                return True
        return False

    def _is_short_content(self, content: str) -> bool:
        """Check if content is short enough to use directly as summary.
        
        Args:
            content: The article content
            
        Returns:
            True if content is short enough to use as-is
        """
        return len(content.strip()) <= SHORT_CONTENT_MAX_CHARS

    def summarize(self, content: str, language_code: str | None = None) -> str | None:
        """Generate a summary for the given content.

        Args:
            content: The article content to summarize
            language_code: ISO 639-1 source language code (e.g., "de", "en")
                          Used only as context for translation to German

        Returns:
            The generated summary, or None if summarization fails
        """
        content = content.strip()

        # Default to English source language if not provided
        if language_code is None:
            language_code = "en"

        source_language_name = get_language_name(language_code)

        prompt = f"""Fasse den folgenden Artikel präzise in 2-3 Sätzen zusammen und übersetze die Ausgabe ins Deutsche. Verzichte auf Floskeln und Einleitungen. Die Zusammenfassung muss für außenstehende Personen ohne Vorwissen verständlich sein und den wesentlichen Inhalt des Artikels klar wiedergeben.

Quellsprache des Artikels: {source_language_name} ({language_code}).
Zielsprache der Zusammenfassung: {TARGET_OUTPUT_LANGUAGE_NAME} ({TARGET_OUTPUT_LANGUAGE_CODE}).
Schreibe die Zusammenfassung ausschließlich auf {TARGET_OUTPUT_LANGUAGE_NAME}.

Artikel:
{content}

Zusammenfassung:"""

        try:
            summary = self._call_llm(prompt)
            if not summary:
                return None
            
            summary = summary.strip()
            
            # Check if LLM returned a meta-response instead of a real summary
            if self._is_meta_response(summary):
                logger.warning(
                    "LLM returned meta-response instead of summary, "
                    "returning None to avoid non-German/raw fallback"
                )
                return None
            
            return summary
        except RuntimeError as e:
            logger.warning(f"Summarization failed: {e}")
            return None

    def generate_title(self, content: str, language_code: str | None = None) -> str | None:
        """Generate a title for the given content in the detected language.

        Args:
            content: The article content to generate a title for
            language_code: ISO 639-1 source language code (e.g., "de", "en")
                          Used only as context for translation to German

        Returns:
            The generated title, or None if title generation fails
        """
        # Default to English source language if not provided
        if language_code is None:
            language_code = "en"

        source_language_name = get_language_name(language_code)

        prompt = f"""Erstelle eine prägnante Überschrift für den folgenden Artikel. Die Überschrift soll den Kern des Artikels auf den Punkt bringen, ohne Floskeln oder allgemeine Formulierungen. Maximal 10 Wörter.

    Quellsprache des Artikels: {source_language_name} ({language_code}).
    Zielsprache der Überschrift: {TARGET_OUTPUT_LANGUAGE_NAME} ({TARGET_OUTPUT_LANGUAGE_CODE}).
    Schreibe die Überschrift ausschließlich auf {TARGET_OUTPUT_LANGUAGE_NAME}.

Artikel:
{content[:3000]}

Überschrift:"""

        try:
            title = self._call_llm(prompt)
            return title.strip() if title else None
        except RuntimeError as e:
            logger.warning(f"Title generation failed: {e}")
            return None

    def process(
        self, items: list[ContentItem]
    ) -> tuple[list[ContentItem], list[tuple[str, str]]]:
        """Process multiple items and generate summaries and titles.

        For each item:
        1. Generate a summary in the detected language
        2. Generate/replace title to match the detected language (if needed)

        Args:
            items: List of ContentItems with extracted content and language_detected

        Returns:
            Tuple of (processed_items, failed_items)
            failed_items format: List of (source_key, error_message)
        """
        processed_items = []
        failed_items = []

        for item in items:
            # Generate summary in German (source language is only contextual)
            summary = self.summarize(item.content, item.language_detected)
            if not summary:
                failed_items.append((item.source_key, "Summarization returned empty"))
                continue
            
            item.summary = summary
            
            # Generate title in German
            # Always generate a new title to ensure language consistency with output
            title = self.generate_title(item.content, item.language_detected)
            if title:
                item.title = title
            else:
                # If title generation fails, keep the original title
                # and log a warning but don't fail the item
                logger.warning(
                    f"Title generation failed for {item.source_key}, "
                    f"keeping original title: {item.title}"
                )
            
            processed_items.append(item)

        return processed_items, failed_items
