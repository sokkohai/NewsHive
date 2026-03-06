"""5-dimension relevance scoring for CCCI article prioritisation.

Implements the scoring framework defined in specs/core/RELEVANCE_SCORING.md.

Each article is scored on five dimensions (0-3 each) that reflect the
strategic logic of our Corporate Crime, Compliance & Investigations team:
  D1 - Enforcement-Intensität
  D2 - Organ-/Management-Exposition
  D3 - Systemische Compliance-Relevanz
  D4 - Regulatorischer Impact
  D5 - Mandatspotenzial

Total score 0-15 maps to: Niedrig (0-4), Mittel (5-9), Hoch (10-15).
Optional bonus (+2) when D1 >= 2 AND D2 >= 2 (classic Internal Investigation trigger).
"""

import json
import logging
from typing import Any

from .config import RelevanceSchema
from .llm_client import call_llm
from .models import ContentItem

logger = logging.getLogger(__name__)

# Classification thresholds per RELEVANCE_SCORING.md §Score Aggregation
_LEVEL_THRESHOLDS: list[tuple[int, str]] = [
    (10, "Hoch"),
    (5, "Mittel"),
    (0, "Niedrig"),
]

DIMENSION_KEYS = (
    "d1_enforcement",
    "d2_organ",
    "d3_compliance",
    "d4_regulatory",
    "d5_mandate",
)

# Valid practice area values per RELEVANCE_SCORING.md §Practice Area Values
PRACTICE_AREAS = frozenset({
    "Wirtschaftsstrafrecht",
    "Interne Untersuchungen",
    "ESG/Regulatory",
    "Investigativer Journalismus",
    "Sonstiges",
})

SYSTEM_MESSAGE = (
    "Du bewertest die Relevanz eines Nachrichtenartikels für ein Corporate Crime, "
    "Compliance & Investigations (CCCI) Team einer Wirtschaftskanzlei. "
    "Antworte ausschließlich mit validem JSON. Kein Fließtext außerhalb des JSON."
)

DEFAULT_SYSTEM_MESSAGE = SYSTEM_MESSAGE

_PROMPT_TEMPLATE = """Bewerte den folgenden Artikel anhand von 5 Dimensionen. Nutze ausschließlich die definierten Kriterien. Jeder Score muss mit einer Begründung (1 Satz) versehen sein.

ARTIKEL:
{article_text}

DIMENSIONEN:

1. Enforcement-Intensität (0-3)
0=Keine Maßnahme | 1=Ankündigung/Entwurf | 2=Ermittlung/Verdacht | 3=Durchsuchung/Anklage/Sanktion

2. Organ-/Management-Exposition (0-3)
0=Keine Führungsebene | 1=Unklar/indirekt | 2=Management erwähnt | 3=Vorstand/CEO/GC direkt betroffen

3. Systemische Compliance-Relevanz (0-3)
0=Einzelfall | 1=Operative Unregelmäßigkeit | 2=Compliance-Versagen möglich | 3=Strukturelles Governance-Versagen

4. Regulatorischer Impact (0-3)
0=Kein Bezug | 1=Bestehende Norm routinemäßig angewendet | 2=Neue Auslegung / EU-Leitlinien zu CSRD/CSDDD/SFDR/LkSG/EUDR/Sanktionsrecht / behördliche Verschärfung | 3=Neues Gesetz / substanziell neues Regime

5. Mandatspotenzial (0-3)
0=Kein Mandat | 1=Möglich | 2=Wahrscheinlich | 3=Sehr wahrscheinlich/typisch (Ermittlungsmandat, interne Untersuchung, Compliance-Beratung, oder investigativer Medienbericht über mögliche Rechtsverstöße eines Unternehmens)

PRAXISBEREICH:
Ordne den Artikel einem der folgenden Bereiche zu:
- Wirtschaftsstrafrecht (Ermittlungen, Strafurteile, Betrug, Geldwäsche, Bestechung, Kartell, Steuerstrafrecht)
- Interne Untersuchungen (Internal Investigation, interne Kontrollen, Hinweisgebersysteme)
- ESG/Regulatory (CSRD, CSDDD, SFDR, LkSG, EUDR, Sanktionsrecht, Außenwirtschaft, Datenschutz, Barrierefreiheit, sonstige Regulatorik)
- Investigativer Journalismus (Medienrecherche, die mögliche Rechtsverstöße oder Compliance-Mängel aufdeckt)
- Sonstiges

Antworte mit exakt diesem JSON-Schema:
{{
  "d1_enforcement": {{"score": <int 0-3>, "begruendung": "<1 Satz>"}},
  "d2_organ": {{"score": <int 0-3>, "begruendung": "<1 Satz>"}},
  "d3_compliance": {{"score": <int 0-3>, "begruendung": "<1 Satz>"}},
  "d4_regulatory": {{"score": <int 0-3>, "begruendung": "<1 Satz>"}},
  "d5_mandate": {{"score": <int 0-3>, "begruendung": "<1 Satz>"}},
  "practice_area": "<Wirtschaftsstrafrecht|Interne Untersuchungen|ESG/Regulatory|Investigativer Journalismus|Sonstiges>"
}}"""


def _build_dimension_block_from_schema(schema: RelevanceSchema) -> str:
    """Build dimensions prompt block from category-level schema."""
    if not schema.dimensions:
        return ""

    lines: list[str] = []
    for index, key in enumerate(DIMENSION_KEYS, start=1):
        definition = schema.dimensions.get(key)
        if definition is None:
            return ""
        lines.append(f"{index}. {definition.label} (0-3)")
        if definition.scoring_rule:
             lines.append(f"Scoring Rule: {definition.scoring_rule}")
        lines.append(
            " | ".join(
                f"{score_key}={definition.scores.get(score_key, '')}" for score_key in ("0", "1", "2", "3")
            )
        )
        lines.append("")

    return "\n".join(lines).strip()


def _build_prompt(article_text: str, schema: RelevanceSchema | None) -> tuple[str, str, set[str]]:
    """Build prompt + system message + allowed practice areas from optional schema."""
    if schema is None:
        logger.warning(
            "RelevanceScorer: no relevance_schema configured for this category — "
            "falling back to hardcoded CCCI defaults. Add a 'relevance_schema' block "
            "to the category in config.json to make scoring fully configurable."
        )
        return _PROMPT_TEMPLATE.format(article_text=article_text), DEFAULT_SYSTEM_MESSAGE, set(PRACTICE_AREAS)

    dimensions_block = _build_dimension_block_from_schema(schema)
    if not dimensions_block:
        logger.warning("RelevanceScorer: schema dimensions invalid/incomplete at runtime, using default prompt")
        return _PROMPT_TEMPLATE.format(article_text=article_text), DEFAULT_SYSTEM_MESSAGE, set(PRACTICE_AREAS)

    practice_areas = schema.practice_areas or sorted(PRACTICE_AREAS)
    practice_area_lines = "\n".join(f"- {area}" for area in practice_areas)
    practice_area_choices = "|".join(practice_areas)
    system_message = schema.system_message or DEFAULT_SYSTEM_MESSAGE

    prompt = f"""Bewerte den folgenden Artikel anhand von 5 Dimensionen. Nutze ausschließlich die definierten Kriterien. Jeder Score muss mit einer Begründung (1 Satz) versehen sein.

ARTIKEL:
{article_text}

DIMENSIONEN:

{dimensions_block}

PRAXISBEREICH:
Ordne den Artikel einem der folgenden Bereiche zu:
{practice_area_lines}

Antworte mit exakt diesem JSON-Schema:
{{
  "d1_enforcement": {{"score": <int 0-3>, "begruendung": "<1 Satz>"}},
  "d2_organ": {{"score": <int 0-3>, "begruendung": "<1 Satz>"}},
  "d3_compliance": {{"score": <int 0-3>, "begruendung": "<1 Satz>"}},
  "d4_regulatory": {{"score": <int 0-3>, "begruendung": "<1 Satz>"}},
  "d5_mandate": {{"score": <int 0-3>, "begruendung": "<1 Satz>"}},
  "practice_area": "<{practice_area_choices}>"
}}"""

    return prompt, system_message, set(practice_areas)


def classify_relevance(total: int, custom_thresholds: dict[str, int] | None = None) -> str:
    """Return classification string for a given total score."""
    thresholds = _LEVEL_THRESHOLDS
    if custom_thresholds:
        # Support both German and English keys, case-insensitive-ish
        h = custom_thresholds.get("hoch") or custom_thresholds.get("Hoch")
        m = custom_thresholds.get("mittel") or custom_thresholds.get("Mittel")
        if h is not None or m is not None:
            h_val = int(h) if h is not None else 10
            m_val = int(m) if m is not None else 5
            thresholds = [(h_val, "Hoch"), (m_val, "Mittel"), (0, "Niedrig")]

    for threshold, level in thresholds:
        if total >= threshold:
            return level
    return "Niedrig"


class RelevanceScorer:
    """Score a ContentItem along 5 CCCI-specific dimensions via a single LLM call.

    Per specs/core/RELEVANCE_SCORING.md.
    Gracefully returns None on any LLM or parse failure — never blocks the pipeline.
    """

    def __init__(
        self,
        llm_provider: str,
        llm_model: str,
        llm_api_key: str,
        llm_api_url: str | None = None,
        bonus_rule_enabled: bool = False,
    ) -> None:
        """Initialise the scorer.

        Args:
            llm_provider: LLM provider ("openai", "anthropic", or "custom").
            llm_model: Model name for the LLM endpoint.
            llm_api_key: API key / bearer token.
            llm_api_url: Required only for provider="custom".
            bonus_rule_enabled: If True, apply +2 bonus when D1>=2 AND D2>=2.
        """
        self.provider = llm_provider
        self.model = llm_model
        self.api_key = llm_api_key
        self.api_url = llm_api_url
        self.bonus_rule_enabled = bonus_rule_enabled

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def score(self, item: ContentItem, schema: RelevanceSchema | None = None) -> dict[str, Any] | None:
        """Score a ContentItem.

        Returns a dict with keys:
            relevance_score (int), relevance_level (str), relevance_dimensions (dict),
            relevance_practice_area (str | None)
        or None if the LLM call or JSON parsing fails.
        """
        article_text = (item.content or item.title or "")[:3000]
        prompt, system_message, allowed_practice_areas = _build_prompt(article_text, schema)

        try:
            raw = self._call_llm(prompt, system_message=system_message)
        except TypeError:
            # Backward compatibility for mocks/overrides expecting _call_llm(prompt)
            raw = self._call_llm(prompt)
        if raw is None:
            return None

        parsed = self._parse_response(raw)
        if parsed is None:
            return None

        dimensions = self._extract_dimensions(parsed)
        if dimensions is None:
            return None

        total = sum(dimensions.values())

        if self.bonus_rule_enabled and dimensions["d1_enforcement"] >= 2 and dimensions["d2_organ"] >= 2:
            total += 2

        practice_area = self._extract_practice_area(parsed, allowed_practice_areas)

        return {
            "relevance_score": total,
            "relevance_level": classify_relevance(total, custom_thresholds=schema.thresholds if schema else None),
            "relevance_dimensions": dimensions,
            "relevance_practice_area": practice_area,
        }

    # ------------------------------------------------------------------
    # LLM call — mirrors pattern in unified_enricher.py
    # ------------------------------------------------------------------

    def _call_llm(self, prompt: str, system_message: str = DEFAULT_SYSTEM_MESSAGE) -> str | None:
        """Call the LLM and return the raw text response."""
        try:
            return call_llm(
                provider=self.provider,
                model=self.model,
                api_key=self.api_key,
                prompt=prompt,
                system_message=system_message,
                api_url=self.api_url,
            )
        except Exception as e:
            logger.error(f"RelevanceScorer LLM call failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_response(self, raw: str) -> dict[str, Any] | None:
        """Parse JSON response from LLM.  Strips markdown code fences if present."""
        text = raw.strip()

        # Strip markdown code fences
        if text.startswith("```"):
            lines = text.splitlines()
            # Drop opening fence line (```json or ```)
            lines = lines[1:]
            # Drop closing fence if present
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines).strip()

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            # Try to find the first {...} block
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1 and end > start:
                try:
                    parsed = json.loads(text[start : end + 1])
                except json.JSONDecodeError as e:
                    logger.error(f"RelevanceScorer JSON parse failed: {e}. Raw: {raw[:200]}")
                    return None
            else:
                logger.error(f"RelevanceScorer: no JSON object found in response. Raw: {raw[:200]}")
                return None

        if not isinstance(parsed, dict):
            logger.error(f"RelevanceScorer: parsed response is not a dict: {type(parsed)}")
            return None

        return parsed

    def _extract_dimensions(self, parsed: dict[str, Any]) -> dict[str, int] | None:
        """Extract and validate per-dimension integer scores from parsed LLM response."""
        dimensions: dict[str, int] = {}
        for key in DIMENSION_KEYS:
            entry = parsed.get(key)
            if entry is None:
                logger.error(f"RelevanceScorer: missing dimension '{key}' in response")
                return None

            # Accept either {"score": 2, ...} or plain int
            if isinstance(entry, dict):
                raw_score = entry.get("score")
            elif isinstance(entry, int):
                raw_score = entry
            else:
                logger.error(
                    f"RelevanceScorer: dimension '{key}' has unexpected type {type(entry)}"
                )
                return None

            if not isinstance(raw_score, int) or raw_score < 0 or raw_score > 3:
                logger.error(
                    f"RelevanceScorer: dimension '{key}' score out of range: {raw_score!r}"
                )
                return None

            dimensions[key] = raw_score

        return dimensions

    def _extract_practice_area(
        self,
        parsed: dict[str, Any],
        allowed_practice_areas: set[str] | None = None,
    ) -> str | None:
        """Extract and validate the practice_area string from the LLM response.

        Returns a valid PRACTICE_AREAS value, or None if absent / unrecognised.
        Non-blocking: a missing or invalid practice_area never invalidates the score.
        """
        allowed_values = allowed_practice_areas if allowed_practice_areas else set(PRACTICE_AREAS)

        raw = parsed.get("practice_area")
        if raw is None:
            logger.warning("RelevanceScorer: 'practice_area' absent from LLM response")
            return None
        if not isinstance(raw, str):
            logger.warning(f"RelevanceScorer: 'practice_area' is not a string: {raw!r}")
            return None
        value = raw.strip()
        if value not in allowed_values:
            logger.warning(
                f"RelevanceScorer: unrecognised practice_area {value!r}; accepted values: {sorted(allowed_values)}"
            )
            return None
        return value
