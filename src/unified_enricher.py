"""Unified enrichment stage combining summarization, cleaning, and validation.

Implements ≤2 LLM calls per article:
- LLM call #1: plain-text summary (always, if LLM is configured)
- LLM call #2: 5-dimension relevance scoring (optional, if relevance_scoring_enabled=true)

Separately (local): keyword-based categorization

Per specs/core/UNIFIED_ENRICHMENT.md and specs/core/RELEVANCE_SCORING.md
"""

import ast
import json
import logging
from typing import Any

from .categorization import Categorizer
from .config import Configuration, RelevanceSchema
from .llm_client import call_llm
from .models import ContentItem
from .relevance_scorer import RelevanceScorer, classify_relevance

logger = logging.getLogger(__name__)


class UnifiedEnricher:
    """Single-call LLM enrichment with local keyword categorization.

    Combines Stage 4 (Summarization), Stage 5 (Categorization), and
    Stage 6 (Quality Verification) into a single unified stage.

    - LLM produces: cleaned_title, summary, cleaned_summary, validation_status
    - Local keyword matching produces: topics (categories)
    """

    SYSTEM_MESSAGE = """Du bist ein präziser, deutschsprachiger Analyse- und Zusammenfassungs-Assistent. 
Deine Aufgabe ist es, Nachrichtenartikel zu analysieren, fachlich zu bewerten und strukturiert aufzubereiten.
Antworte AUSSCHLIESSLICH mit einem validen JSON-Objekt. Kein Markdown, kein erklärender Text."""

    def __init__(
        self,
        config: Configuration,
        llm_provider: str,
        llm_model: str,
        llm_api_key: str,
        llm_api_url: str | None = None,
    ):
        """Initialize unified enricher.

        Args:
            config: Configuration with categories info
            llm_provider: LLM provider (custom)
            llm_model: Model name
            llm_api_key: API key
            llm_api_url: Optional custom API URL
        """
        self.config = config
        self.provider = llm_provider
        self.model = llm_model
        self.api_key = llm_api_key
        self.api_url = llm_api_url  # only required for provider='custom'
        self.categorizer = Categorizer(config)

        # Relevance scoring is now integrated into the single LLM call,
        # but we reuse the helper methods from RelevanceScorer if available
        # or reimplement the logic here. For simplicity in the single-call approach,
        # we will build the schema into the single prompt.
        pass

    def _escape_json_string(self, s: str) -> str:
        """Escape string for safe JSON embedding."""
        if not s:
            return ""
        # Escape backslashes, quotes, newlines, tabs
        s = s.replace("\\", "\\\\")
        s = s.replace('"', '\\"')
        s = s.replace("\n", "\\n")
        s = s.replace("\r", "\\r")
        s = s.replace("\t", "\\t")
        return s

    def _build_single_call_prompt(self, item: ContentItem, relevance_schema: RelevanceSchema | None) -> str:
        """Build the single JSON prompt for Title, Summary, and Relevance Scoring."""
        content = item.content[:4000] if item.content else ""
        source_language = item.language_detected or "unknown"
        
        # Build relevance section
        relevance_block = ""
        practice_area_block = ""
        
        # Default or custom schema
        from .relevance_scorer import _build_dimension_block_from_schema, PRACTICE_AREAS, DIMENSION_KEYS
        
        if relevance_schema:
             dimensions_text = _build_dimension_block_from_schema(relevance_schema)
             practice_areas = relevance_schema.practice_areas or sorted(PRACTICE_AREAS)
        else:
             # Default fallback
             # We should ideally reuse RelevanceScorer logic, but for single-file coherence we inline the defaults if needed
             # or better, assume RelevanceScorer constants are importable.
             # For now, let's use a simplified default if schema is missing, similar to RelevanceScorer default.
             dim_definitions = [
                 "1. Enforcement-Intensität (0-3)\n0=Keine Maßnahme | 1=Ankündigung | 2=Ermittlung | 3=Sanktion",
                 "2. Organ-/Management-Exposition (0-3)\n0=Keine | 1=Indirekt | 2=Management | 3=Vorstand",
                 "3. Systemische Compliance-Relevanz (0-3)\n0=Einzelfall | 1=Operativ | 2=Compliance-Versagen | 3=Strukturell",
                 "4. Regulatorischer Impact (0-3)\n0=Kein Bezug | 1=Routine | 2=Verschärfung/Neue Auslegung | 3=Neues Gesetz",
                 "5. Mandatspotenzial (0-3)\n0=Kein Mandat | 1=Möglich | 2=Wahrscheinlich | 3=Typisch/Hoch"
             ]
             dimensions_text = "\n\n".join(dim_definitions)
             practice_areas = sorted(PRACTICE_AREAS)

        practice_area_options = "|".join(practice_areas)

        return f"""Analysiere den folgenden Artikel und erstelle eine hochwertige deutsche Aufbereitung als JSON.

AUFGABEN:
1. Titel: Erstelle eine prägnante, sachliche deutsche Überschrift (max. 10 Wörter), befreit von Clickbait oder Metadaten.
2. Zusammenfassung: Fasse den wesentlichen Inhalt in 2-3 Sätzen auf Deutsch zusammen. Fokus auf Fakten.
3. Relevanz-Bewertung: Bewerte den Artikel anhand der 5 Dimensionen (0-3 Punkte).
4. Qualitäts-Bewertung: Schätze ein, ob der Artikeltext ausreichend substantiell für eine Bewertung war (0.0 - 1.0).

DIMENSIONEN FÜR BEWERTUNG:
{dimensions_text}

PRAXISBEREICHE:
{practice_area_options}

ARTIKEL (Sprache: {source_language}):
{content}

ANTWORT-SCHEMA (JSON):
{{
  "cleaned_title": "Deutscher Titel",
  "summary": "Deutsche Zusammenfassung...",
  "relevance_dimensions": {{
    "d1_enforcement": 0,
    "d2_organ": 0,
    "d3_compliance": 0,
    "d4_regulatory": 0,
    "d5_mandate": 0
  }},
  "relevance_practice_area": "Einer der Praxisbereiche",
  "quality_score": 1.0
}}
"""
    
    def process_batch(
        self, items: list[ContentItem], batch_size: int = 5
    ) -> tuple[list[ContentItem], list[tuple[str, str]]]:
        """Delegate to single-item process logic (batching handled implicitly by list loop)."""
        return self.process(items)

    def _build_enrichment_prompt_legacy(self, item: ContentItem, matched_keywords: list[str] | None = None) -> str:
        """LEGACY: Build simplified enrichment prompt: article text + matched keywords → plain-text summary."""
        content = item.content[:3000] if item.content else ""
        source_language = item.language_detected or "unknown"

        if matched_keywords:
            keywords_str = ", ".join(matched_keywords)
            keyword_instruction = f"Die Zusammenfassung muss einen oder mehrere der folgenden relevanten Begriffe erwähnen: {keywords_str}.\n"
        else:
            keyword_instruction = ""

        return f"""Fasse den folgenden Artikeltext präzise in 2-3 Sätzen zusammen und übersetze die Ausgabe vollständig ins Deutsche. Verwende keine Floskeln oder Einleitungen. Die Zusammenfassung soll für außenstehende Personen ohne Vorwissen verständlich sein.

Quellsprache: {source_language}.
Zielsprache: Deutsch.
{keyword_instruction}
Artikeltext:
{content}"""

    def _build_translation_prompt(self, item: ContentItem) -> str:
        """Build prompt for full-text translation to German."""
        content = item.content or ""
        source_language = item.language_detected or "unknown"
        return f"""Übersetze den folgenden vollständigen Artikeltext vollständig und originalgetreu ins Deutsche.

Regeln:
- Gib ausschließlich die deutsche Übersetzung zurück
- Keine Einleitung, keine Kommentare, keine Zusammenfassung
- Keine inhaltlichen Ergänzungen oder Auslassungen
- Erhalte Absätze und Listenstruktur soweit möglich

Quellsprache: {source_language}
Zielsprache: Deutsch

Artikeltext:
{content}"""

    def _translate_content_to_german(self, item: ContentItem) -> str | None:
        """Translate full article content to German via LLM."""
        prompt = self._build_translation_prompt(item)
        translated = self._call_llm(prompt)
        if translated is None:
            return None

        translated = translated.strip()
        return translated or None

    def _call_llm(self, prompt: str) -> str | None:
        """Call LLM with enrichment prompt."""
        try:
            return call_llm(
                provider=self.provider,
                model=self.model,
                api_key=self.api_key,
                prompt=prompt,
                system_message=self.SYSTEM_MESSAGE,
                api_url=self.api_url,
            )
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return None

    def _extract_custom_response(self, result: dict[str, Any]) -> str | None:
        """Kept for backward compatibility — delegates to llm_client."""
        try:
            from .llm_client import _extract_custom_response as _ecr
            return _ecr(result)
        except Exception as e:
             # Log the full result structure to help debugging since fallbacks are removed
             logger.error(f"Failed to extract custom response: {e}. Result keys: {list(result.keys())}")
             raise

    def _stringify_response_value(self, val: Any) -> str:
        """Convert various response value types to string."""
        if isinstance(val, str):
            return val.strip()

        if isinstance(val, list):
            # Handle list of strings or dicts with text content
            parts = []
            for item in val:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    # Skip reasoning/status chunks
                    if item.get("type") == "reasoning":
                        continue

                    # Handle message chunks with nested content list (structured format)
                    if "content" in item and isinstance(item["content"], list):
                        parts.append(self._stringify_response_value(item["content"]))
                        continue

                    # Extract text from common dict formats
                    for field in ["text", "content", "summary"]:
                        if field in item and isinstance(item[field], str):
                            parts.append(item[field])
                            break

            if parts:
                return "".join(parts).strip()

            # Fallback: stringify the whole list if no parts extracted
            try:
                return json.dumps(val)
            except Exception:
                return str(val).strip()

        # Fallback: convert to string
        return str(val).strip()

    def _parse_llm_response(self, response: str) -> dict[str, Any] | None:
        """Parse LLM response as JSON, handling markdown code blocks and nested payloads."""
        if not response:
            return None

        text = self._strip_code_block(response)

        parsed = self._extract_json_from_text(text)
        if parsed is not None:
            return parsed

        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            json_str = text[start : end + 1]
            candidate = self._try_parse_dict(json_str)
            if candidate:
                return candidate

        logger.warning(
            f"Failed to parse LLM response as JSON. Content preview: {text[:200]}..."
        )
        return None

    def _strip_code_block(self, text: str) -> str:
        """Remove surrounding markdown code fences if present."""
        stripped = text.strip()
        if stripped.startswith("```"):
            lines = stripped.split("\n")
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            stripped = "\n".join(lines).strip()
        return stripped

    def _extract_json_from_text(self, text: str) -> dict[str, Any] | None:
        """Look for valid JSON objects in the text, including nested content fields."""
        candidate = self._try_parse_dict(text)
        if candidate:
            nested = self._unwrap_nested_result(candidate)
            if nested:
                return nested

        for candidate_text in self._extract_json_candidates(text):
            candidate = self._try_parse_dict(candidate_text)
            if candidate:
                nested = self._unwrap_nested_result(candidate)
                if nested:
                    return nested

        return None

    def _try_parse_dict(self, text: str) -> dict[str, Any] | None:
        """Attempt to parse text as a dict via JSON or AST literal evaluation."""
        # Clean up potential encoding issues before parsing
        try:
            # Handle encoding errors gracefully - replace problematic bytes
            if isinstance(text, str):
                text = text.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
            
            val = json.loads(text)
            if isinstance(val, dict):
                return val
        except json.JSONDecodeError:
            pass

        try:
            val = ast.literal_eval(text)
            if isinstance(val, dict):
                return val
        except (ValueError, SyntaxError):
            pass

        return None

    def _extract_json_candidates(self, text: str) -> list[str]:
        """Scan the text for top-level JSON objects (ignoring braces inside quotes)."""
        candidates: list[str] = []
        depth = 0
        start = -1
        in_single = False
        in_double = False
        escape = False

        for idx, ch in enumerate(text):
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == "'" and not in_double:
                in_single = not in_single
                continue
            if ch == '"' and not in_single:
                in_double = not in_double
                continue
            if in_single or in_double:
                continue
            if ch == "{":
                if depth == 0:
                    start = idx
                depth += 1
            elif ch == "}":
                if depth > 0:
                    depth -= 1
                    if depth == 0 and start != -1:
                        candidates.append(text[start : idx + 1])
                        start = -1

        return candidates

    def _unwrap_nested_result(self, candidate: dict[str, Any]) -> dict[str, Any] | None:
        """If candidate lacks fields, try to unwrap nested 'content' JSON payloads."""
        if self._has_required_fields(candidate):
            return candidate

        content = candidate.get("content")
        if isinstance(content, str):
            inner = self._strip_code_block(content)
            inner_candidate = self._try_parse_dict(inner)
            if inner_candidate:
                return self._unwrap_nested_result(inner_candidate)

        return None

    def _has_required_fields(self, candidate: dict[str, Any]) -> bool:
        """Determine whether the dict already contains the expected enrichment fields."""
        return (
            "cleaned_title" in candidate
            and ("summary" in candidate or "cleaned_summary" in candidate)
        )

    def _validate_enrichment_result(
        self, result: dict[str, Any], source_key: str
    ) -> tuple[bool, str]:
        """Validate that LLM result contains all required fields with valid content.

        Args:
            result: Parsed LLM response dictionary
            source_key: Article source key for logging

        Returns:
            Tuple of (is_valid, error_message)
        """
        required_fields = {
            "cleaned_title": str,
            "summary": str,
            "translated_content": str,
            "validation_status": str,
        }

        missing_fields = []
        for field, field_type in required_fields.items():
            if field not in result:
                missing_fields.append(field)
            elif not isinstance(result[field], field_type):
                missing_fields.append(f"{field} (wrong type: {type(result[field]).__name__})")

        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}"

        # Validate cleaned_title is non-empty
        if not result["cleaned_title"].strip():
            return False, "cleaned_title is empty"

        # Validate summary is non-empty
        if not result["summary"].strip():
            return False, "summary is empty"

        # Validate translated_content is non-empty
        if not result["translated_content"].strip():
            return False, "translated_content is empty"

        # Validate validation_status is valid
        status = result["validation_status"].upper()
        if status not in ("PASS", "WARN", "FAIL"):
            return False, f"Invalid validation_status: {result['validation_status']}"

        # Validate cleaned_summary if present
        if "cleaned_summary" in result and not isinstance(result["cleaned_summary"], str):
            return False, "cleaned_summary has wrong type"

        return True, ""

    def _match_categories_by_keywords(self, item: ContentItem) -> list[str]:
        """Match article to categories using exact keyword matching.

        Uses the existing Categorizer for consistent behavior.
        """
        return self.categorizer.categorize(item)

    def _find_matched_keywords(self, item: ContentItem, category_names: list[str]) -> list[str]:
        """Return the actual keyword strings that matched in the article content."""
        content = (item.content or item.title or "").lower()
        matched: list[str] = []
        for cat_name in category_names:
            category = self.config.get_category(cat_name)
            if not category:
                continue
            for kw in category.keywords:
                if kw.lower() in content and kw not in matched:
                    matched.append(kw)
        return matched

    def _resolve_relevance_schema(self, item: ContentItem) -> tuple[str | None, RelevanceSchema | None]:
        """Resolve the category-specific relevance schema for an item.

        Deterministic rule:
        1) iterate item.categories in order
        2) select first category with configured relevance_schema
        3) return (None, None) for default scorer schema
        """
        for category_name in item.categories:
            category = self.config.get_category(category_name)
            if not category:
                continue
            schema = getattr(category, "relevance_schema", None)
            if isinstance(schema, RelevanceSchema):
                return category_name, schema
        return None, None

    def process(
        self, items: list[ContentItem]
    ) -> tuple[list[ContentItem], list[tuple[str, str]]]:
        """Process items using the new single-call logic."""
        enriched_items: list[ContentItem] = []
        failed_items: list[tuple[str, str]] = []

        from .relevance_scorer import DIMENSION_KEYS, PRACTICE_AREAS

        for item in items:
            try:
                # 1. Local Categorization
                # (Use self.categorizer for this logic if available, or do it inline)
                item.categories = self.categorizer.categorize(item)
                if not item.categories:
                    failed_items.append((item.source_key, "no_categories_matched"))
                    logger.debug(f"Filtered (no category keywords matched): {item.source_key}")
                    continue

                # 2. Build Single Prompt
                # Resolve schema if needed
                selected_category_name: str | None = None
                relevance_schema: RelevanceSchema | None = None
                
                # Deterministic rule: first category with schema
                for cat_name in item.categories:
                    cat_obj = self.config.get_category(cat_name)
                    if cat_obj and cat_obj.relevance_schema:
                        selected_category_name = cat_name
                        relevance_schema = cat_obj.relevance_schema
                        break
                
                prompt = self._build_unified_structure_prompt(item, relevance_schema)

                # 3. Call LLM
                response_text = self._call_llm(prompt)
                if not response_text:
                    failed_items.append((item.source_key, "llm_call_failed"))
                    logger.warning(f"Optimization: Single Enrichment Call failed for {item.source_key}")
                    continue

                # 4. Parse Response
                result = self._parse_llm_response(response_text)
                if not result:
                    failed_items.append((item.source_key, "llm_parsing_failed"))
                    logger.warning(f"Optimization: Single Enrichment Call parsing failed for {item.source_key}")
                    continue

                # 5. Extract & Validate
                # Map JSON fields to ContentItem
                item.title = str(result.get("title", "")).strip() or item.title # Fallback to original if empty
                item.summary = str(result.get("summary", "")).strip()
                item.cleaned_title = item.title # In optimized flow, title IS the cleaned title
                item.cleaned_summary = item.summary # In optimized flow, summary IS the cleaned summary
                
                # Relevance Data
                dims = result.get("relevance_dimensions", {})
                total_score = 0
                if isinstance(dims, dict):
                    # Validating keys
                    valid_dims = {}
                    for k in DIMENSION_KEYS:
                        val = dims.get(k, 0)
                        try:
                            val_int = int(val)
                        except (ValueError, TypeError):
                            val_int = 0
                        valid_dims[k] = val_int
                        total_score += val_int
                    
                    item.relevance_dimensions = valid_dims
                    item.relevance_score = total_score
                    
                    # Levels logic
                    item.relevance_level = classify_relevance(
                        total_score, 
                        custom_thresholds=relevance_schema.thresholds if relevance_schema else None
                    )

                if "practice_area" in result and result["practice_area"] in PRACTICE_AREAS:
                     item.relevance_practice_area = result["practice_area"]
                else:
                     item.relevance_practice_area = result.get("practice_area", "Sonstiges")

                quality_score = result.get("quality_score", 1.0)
                item.validation_status = "PASS" if quality_score > 0.3 else "WARN"

                # 6. Filter Low/Medium Score
                # Only keep "Hoch"
                if item.relevance_level != "Hoch":
                     failed_items.append((item.source_key, f"filtered: {item.relevance_level}"))
                     logger.info(f"Filtered non-high level: {item.source_key} ({item.relevance_level}, score: {item.relevance_score})")
                     continue

                enriched_items.append(item)
                logger.info(f"Enriched: {item.source_key} (Score: {item.relevance_score}, Cat: {item.categories})")

            except Exception as e:
                logger.error(f"Error enriching {item.source_key}: {e}", exc_info=True)
                failed_items.append((item.source_key, f"error: {str(e)}"))

        return enriched_items, failed_items

    def _build_unified_structure_prompt(self, item: ContentItem, relevance_schema: RelevanceSchema | None) -> str:
        """Create the Single-Call prompt JSON structure."""
        content = item.content[:4500] if item.content else ""
        
        # Dimensions Block
        from .relevance_scorer import _build_dimension_block_from_schema, PRACTICE_AREAS, DIMENSION_KEYS, DEFAULT_SYSTEM_MESSAGE
        
        dimensions_text = ""
        practice_areas = sorted(list(PRACTICE_AREAS))

        if relevance_schema:
             dimensions_text = _build_dimension_block_from_schema(relevance_schema)
             if relevance_schema.practice_areas:
                 practice_areas = relevance_schema.practice_areas
        else:
             # Default fallback if no schema
             dimensions_text = """1. Enforcement-Intensität (0-3)
0=Keine Maßnahme | 1=Ankündigung | 2=Ermittlung | 3=Sanktion

2. Organ-/Management-Exposition (0-3)
0=Keine | 1=Indirekt | 2=Management | 3=Vorstand

3. Systemische Compliance-Relevanz (0-3)
0=Einzelfall | 1=Operativ | 2=Compliance-Versagen | 3=Strukturell

4. Regulatorischer Impact (0-3)
0=Kein Bezug | 1=Routine | 2=Verschärfung | 3=Neues Gesetz

5. Mandatspotenzial (0-3)
0=Kein Mandat | 1=Möglich | 2=Wahrscheinlich | 3=Typisch/Hoch"""

        practice_area_options = "|".join(practice_areas)

        return f"""Analysiere den folgenden Artikel und antworte AUSSCHLIESSLICH mit einem validen JSON-Objekt.

AUFGABEN:
1. Titel: Erstelle eine prägnante deutsche Überschrift (max. 10 Wörter).
2. Zusammenfassung: Fasse den Inhalt in 2-3 Sätzen auf Deutsch zusammen.
3. Relevanz: Bewerte 5 Dimensionen (0-3).
4. Qualität: Bewerte die Textgrundlage (0.0-1.0).

DIMENSIONEN:
{dimensions_text}

PRAXISBEREICHE:
{practice_area_options}

ARTIKEL:
{content}

JSON-SCHEMA:
{{
  "title": "...",
  "summary": "...",
  "relevance_dimensions": {{
    "d1_enforcement": 0,
    "d2_organ": 0,
    "d3_compliance": 0,
    "d4_regulatory": 0,
    "d5_mandate": 0
  }},
  "practice_area": "...",
  "quality_score": 1.0
}}"""

