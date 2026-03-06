"""Text cleaning utilities using LLM-based approach.

Provides functions for cleaning titles and summaries using LLM inference.
This module is designed to be used by the QualityVerifier class.
"""

import re


def normalize_title(title: str, max_words: int = 15) -> str:
    """Normalize and limit title to maximum words.
    
    Removes excess whitespace, newlines, and limits to max_words.
    Per specs/core/OUTPUT.md, titles should be concise and clean.
    
    Args:
        title: Raw title text
        max_words: Maximum number of words to keep (default: 15)
        
    Returns:
        Cleaned and truncated title
    """
    if not title:
        return ""
    
    # Compress whitespace and newlines
    normalized = " ".join(title.split())
    
    # Split into words and limit
    words = normalized.split()
    if len(words) > max_words:
        normalized = " ".join(words[:max_words]) + "…"
    
    return normalized


def clean_title_prompt(title: str) -> str:
    """Generate LLM prompt for cleaning a title.
    
    Args:
        title: Raw title text
        
    Returns:
        Prompt for LLM
    """
    return f"""Bereinige den folgenden Artikel-Titel (der auf Deutsch oder einer anderen Sprache sein kann) durch Entfernung von Metadaten-Artefakten wie Daten, Lesezeiten und überschüssigen Leerzeichen.
Entferne umgebende Anführungszeichen, wenn sie keinen Wert hinzufügen (z.B. behalte sie für "zitierte Phrase" in einem Satz, aber entferne sie von "Ganzer Titel").
Gib nur den bereinigten Titel zurück, nichts anderes.

Titel: "{title}"

Bereinigter Titel:"""


def clean_summary_prompt(summary: str) -> str:
    """Generate LLM prompt for cleaning a summary.
    
    Args:
        summary: Raw summary text
        
    Returns:
        Prompt for LLM
    """
    return f"""Bereinige und normalisiere den folgenden Zusammenfassungs-Text (der auf Deutsch oder einer anderen Sprache sein kann). Entferne überschüssige Leerzeichen, Zeilenumbrüche und Tabulatoren, während die Absatzstruktur erhalten bleibt.
Gib nur die bereinigte Zusammenfassung zurück, nichts anderes.

Zusammenfassung: "{summary}"

Bereinigte Zusammenfassung:"""
