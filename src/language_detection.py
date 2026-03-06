"""Language detection utilities.

Implements language detection as specified in specs/core/LANGUAGE_CONSISTENCY.md.
"""

import logging

logger = logging.getLogger(__name__)

# Language name mapping from ISO 639-1 codes
LANGUAGE_NAMES = {
    "de": "German",
    "en": "English",
    "fr": "French",
    "es": "Spanish",
    "it": "Italian",
    "nl": "Dutch",
    "pl": "Polish",
    "pt": "Portuguese",
    "ru": "Russian",
    "ja": "Japanese",
    "zh": "Chinese",
    "cs": "Czech",
    "da": "Danish",
    "fi": "Finnish",
    "hu": "Hungarian",
    "no": "Norwegian",
    "sv": "Swedish",
    "tr": "Turkish",
    "uk": "Ukrainian",
    "ar": "Arabic",
    "he": "Hebrew",
    "ko": "Korean",
    "th": "Thai",
    "vi": "Vietnamese",
}


def detect_language(text: str, min_length: int = 50) -> str:
    """Detect the language of the given text.

    Uses langdetect library to probabilistically detect language from text.
    Falls back to "en" (English) if detection fails or confidence is too low.

    Args:
        text: The text to analyze for language detection
        min_length: Minimum text length to attempt detection (default 50 characters)

    Returns:
        ISO 639-1 language code (e.g., "de", "en", "fr")
        Defaults to "en" if detection fails or text is too short
    """
    if not text or len(text) < min_length:
        logger.debug(f"Text too short for reliable language detection ({len(text) if text else 0} chars), defaulting to 'en'")
        return "en"

    try:
        import langdetect
        from langdetect import detect

        # Note: Not setting seed to allow probabilistic detection
        # Multiple calls on same text may return same language due to text characteristics
        detected = detect(text[:5000])  # Use first 5000 chars for efficiency
        logger.debug(f"Detected language: {detected}")
        return detected

    except Exception as e:
        logger.warning(f"Language detection failed: {e}, defaulting to 'en'")
        return "en"


def get_language_name(language_code: str) -> str:
    """Get the human-readable language name from ISO 639-1 code.

    Args:
        language_code: ISO 639-1 language code (e.g., "de", "en")

    Returns:
        Human-readable language name, or the code itself if not found
    """
    return LANGUAGE_NAMES.get(language_code, language_code)


def format_language_context(language_code: str) -> dict[str, str]:
    """Format language information for use in LLM prompts.

    Args:
        language_code: ISO 639-1 language code

    Returns:
        Dictionary with language_code and language_name keys
    """
    return {
        "language_code": language_code,
        "language_name": get_language_name(language_code),
    }
