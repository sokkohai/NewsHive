"""Tests for the CCCI relevance scoring module.

Tests the RelevanceScorer class per specs/core/RELEVANCE_SCORING.md.
"""

import json
from unittest.mock import patch

from src.config import RelevanceSchema
from src.models import ContentItem
from src.relevance_scorer import DIMENSION_KEYS, PRACTICE_AREAS, RelevanceScorer, classify_relevance

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_scorer(bonus_rule: bool = False) -> RelevanceScorer:
    return RelevanceScorer(
        llm_provider="custom",
        llm_model="test-model",
        llm_api_key="test-key",
        llm_api_url="https://test.example.com/llm",
        bonus_rule_enabled=bonus_rule,
    )


def make_item(content: str = "This is article content about investigations.") -> ContentItem:
    return ContentItem(
        id="test-1",
        source_type="web",
        source_key="web:https://example.com/article",
        title="Test Article",
        summary="",
        content=content,
    )


def make_llm_response(
    d1: int, d2: int, d3: int, d4: int, d5: int,
    practice_area: str = "Wirtschaftsstrafrecht",
) -> dict:
    """Build a valid LLM response dict with five dimension scores and a practice_area."""
    return {
        "d1_enforcement": {"score": d1, "begruendung": "Begründung D1."},
        "d2_organ": {"score": d2, "begruendung": "Begründung D2."},
        "d3_compliance": {"score": d3, "begruendung": "Begründung D3."},
        "d4_regulatory": {"score": d4, "begruendung": "Begründung D4."},
        "d5_mandate": {"score": d5, "begruendung": "Begründung D5."},
        "practice_area": practice_area,
    }


def make_custom_llm_output(text: str) -> dict:
    """Wrap text in the custom LLM response envelope format."""
    return {
        "output": [
            {
                "type": "message",
                "content": [
                    {"type": "output_text", "text": text}
                ],
            }
        ]
    }


def make_schema(
    schema_id: str = "ccci_v1",
    practice_areas: list[str] | None = None,
) -> RelevanceSchema:
    dimensions = {
        "d1_enforcement": {
            "label": "D1 Custom",
            "question": "Q1?",
            "scores": {"0": "n/a", "1": "low", "2": "mid", "3": "high"},
        },
        "d2_organ": {
            "label": "D2 Custom",
            "question": "Q2?",
            "scores": {"0": "n/a", "1": "low", "2": "mid", "3": "high"},
        },
        "d3_compliance": {
            "label": "D3 Custom",
            "question": "Q3?",
            "scores": {"0": "n/a", "1": "low", "2": "mid", "3": "high"},
        },
        "d4_regulatory": {
            "label": "D4 Custom",
            "question": "Q4?",
            "scores": {"0": "n/a", "1": "low", "2": "mid", "3": "high"},
        },
        "d5_mandate": {
            "label": "D5 Custom",
            "question": "Q5?",
            "scores": {"0": "n/a", "1": "low", "2": "mid", "3": "high"},
        },
    }
    return RelevanceSchema.from_dict(
        {
            "schema_id": schema_id,
            "system_message": "SYSTEM CUSTOM",
            "dimensions": dimensions,
            "practice_areas": practice_areas or ["Area A", "Area B"],
        }
    )


# ---------------------------------------------------------------------------
# _classify helper
# ---------------------------------------------------------------------------

class TestClassify:
    def test_niedrig_lower_bound(self):
        assert classify_relevance(0) == "Niedrig"

    def test_niedrig_upper_bound(self):
        assert classify_relevance(4) == "Niedrig"

    def test_mittel_lower_bound(self):
        assert classify_relevance(5) == "Mittel"

    def test_mittel_upper_bound(self):
        assert classify_relevance(9) == "Mittel"

    def test_hoch_lower_bound(self):
        assert classify_relevance(10) == "Hoch"

    def test_hoch_upper_bound(self):
        assert classify_relevance(15) == "Hoch"

    def test_hoch_with_bonus(self):
        """Scores above 15 (possible with bonus) are still Hoch."""
        assert classify_relevance(17) == "Hoch"


# ---------------------------------------------------------------------------
# Score aggregation
# ---------------------------------------------------------------------------

class TestScoreAggregation:
    def test_total_is_sum_of_dimensions(self):
        scorer = make_scorer()
        payload = make_llm_response(1, 2, 1, 1, 2)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 1 + 2 + 1 + 1 + 2  # 7
        assert result["relevance_level"] == "Mittel"

    def test_all_zeros(self):
        scorer = make_scorer()
        payload = make_llm_response(0, 0, 0, 0, 0)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 0
        assert result["relevance_level"] == "Niedrig"

    def test_all_threes(self):
        scorer = make_scorer()
        payload = make_llm_response(3, 3, 3, 3, 3)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 15
        assert result["relevance_level"] == "Hoch"


# ---------------------------------------------------------------------------
# Bonus rule
# ---------------------------------------------------------------------------

class TestBonusRule:
    def test_bonus_not_applied_when_disabled(self):
        scorer = make_scorer(bonus_rule=False)
        payload = make_llm_response(3, 3, 0, 0, 0)  # D1>=2 AND D2>=2 but no bonus

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 6  # 3+3+0+0+0, no bonus

    def test_bonus_applied_when_enabled_and_conditions_met(self):
        scorer = make_scorer(bonus_rule=True)
        payload = make_llm_response(2, 2, 0, 0, 0)  # D1=2, D2=2 -> bonus +2

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 2 + 2 + 2  # 4 + bonus 2 = 6

    def test_bonus_not_applied_when_d1_too_low(self):
        scorer = make_scorer(bonus_rule=True)
        payload = make_llm_response(1, 3, 0, 0, 0)  # D1=1 < 2

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 4  # no bonus

    def test_bonus_not_applied_when_d2_too_low(self):
        scorer = make_scorer(bonus_rule=True)
        payload = make_llm_response(3, 1, 0, 0, 0)  # D2=1 < 2

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 4  # no bonus

    def test_bonus_applied_at_exact_threshold(self):
        """Bonus triggers when both D1 and D2 are exactly 2."""
        scorer = make_scorer(bonus_rule=True)
        payload = make_llm_response(2, 2, 1, 1, 1)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 2 + 2 + 1 + 1 + 1 + 2  # 9


# ---------------------------------------------------------------------------
# Output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def test_dimensions_dict_contains_all_keys(self):
        scorer = make_scorer()
        payload = make_llm_response(1, 1, 1, 1, 1)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        dims = result["relevance_dimensions"]
        for key in DIMENSION_KEYS:
            assert key in dims, f"Missing dimension key: {key}"

    def test_dimensions_values_are_ints(self):
        scorer = make_scorer()
        payload = make_llm_response(1, 2, 0, 3, 1)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        dims = result["relevance_dimensions"]
        for key in DIMENSION_KEYS:
            assert isinstance(dims[key], int), f"{key} should be int"

    def test_begruendung_not_in_output(self):
        """Reasoning text must not appear in the returned dimensions dict."""
        scorer = make_scorer()
        payload = make_llm_response(1, 1, 1, 1, 1)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        dims = result["relevance_dimensions"]
        for _key in DIMENSION_KEYS:
            assert "begruendung" not in dims


# ---------------------------------------------------------------------------
# Article text truncation
# ---------------------------------------------------------------------------

class TestArticleTextTruncation:
    def test_long_content_is_truncated(self):
        """Prompt must only use up to 3000 chars of article content.

        Places a unique sentinel string starting at position 3000 that
        cannot appear in normal German prompt text.
        """
        sentinel = "XSENTINELX99988877766655Z"
        long_content = "a" * 3000 + sentinel + "a" * 5000
        item = make_item(content=long_content)
        scorer = make_scorer()

        captured_prompts: list[str] = []

        def fake_call_llm(prompt: str) -> str:
            captured_prompts.append(prompt)
            return json.dumps(make_llm_response(0, 0, 0, 0, 0))

        with patch.object(scorer, "_call_llm", side_effect=fake_call_llm):
            scorer.score(item)

        assert len(captured_prompts) == 1
        # Sentinel placed at position 3000 must not appear in the generated prompt
        assert sentinel not in captured_prompts[0]
        # Content up to the truncation point must be present
        assert "a" * 50 in captured_prompts[0]


# ---------------------------------------------------------------------------
# Graceful degradation
# ---------------------------------------------------------------------------

class TestGracefulDegradation:
    def test_returns_none_on_llm_failure(self):
        scorer = make_scorer()

        with patch.object(scorer, "_call_llm", return_value=None):
            result = scorer.score(make_item())

        assert result is None

    def test_returns_none_on_invalid_json(self):
        scorer = make_scorer()

        with patch.object(scorer, "_call_llm", return_value="not valid json at all"):
            result = scorer.score(make_item())

        assert result is None

    def test_returns_none_on_missing_dimension(self):
        scorer = make_scorer()
        incomplete = make_llm_response(1, 1, 1, 1, 1)
        del incomplete["d3_compliance"]

        with patch.object(scorer, "_call_llm", return_value=json.dumps(incomplete)):
            result = scorer.score(make_item())

        assert result is None

    def test_returns_none_on_score_out_of_range(self):
        scorer = make_scorer()
        bad = make_llm_response(1, 1, 1, 1, 1)
        bad["d1_enforcement"]["score"] = 5  # out of 0-3 range

        with patch.object(scorer, "_call_llm", return_value=json.dumps(bad)):
            result = scorer.score(make_item())

        assert result is None

    def test_returns_none_on_score_negative(self):
        scorer = make_scorer()
        bad = make_llm_response(1, 1, 1, 1, 1)
        bad["d2_organ"]["score"] = -1

        with patch.object(scorer, "_call_llm", return_value=json.dumps(bad)):
            result = scorer.score(make_item())

        assert result is None

    def test_returns_none_on_network_error(self):
        scorer = make_scorer()

        with patch("requests.post", side_effect=ConnectionError("network error")):
            result = scorer.score(make_item())

        assert result is None


# ---------------------------------------------------------------------------
# JSON parsing robustness
# ---------------------------------------------------------------------------

class TestJsonParsing:
    def test_parses_json_with_markdown_fence(self):
        scorer = make_scorer()
        payload = make_llm_response(2, 1, 2, 1, 2)
        fenced = f"```json\n{json.dumps(payload)}\n```"

        with patch.object(scorer, "_call_llm", return_value=fenced):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 8

    def test_parses_json_with_plain_fence(self):
        scorer = make_scorer()
        payload = make_llm_response(1, 0, 1, 0, 1)
        fenced = f"```\n{json.dumps(payload)}\n```"

        with patch.object(scorer, "_call_llm", return_value=fenced):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 3

    def test_parses_flat_int_scores(self):
        """LLM may return plain int instead of {score: int, begruendung: str}."""
        scorer = make_scorer()
        flat = dict.fromkeys(DIMENSION_KEYS, 2)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(flat)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 10

    def test_parses_json_with_surrounding_text(self):
        """JSON embedded in prose (fallback brace-extraction)."""
        scorer = make_scorer()
        payload = make_llm_response(0, 0, 0, 0, 1)
        wrapped = f"Here is my assessment:\n{json.dumps(payload)}\nDone."

        with patch.object(scorer, "_call_llm", return_value=wrapped):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 1


# ---------------------------------------------------------------------------
# LLM response extraction
# ---------------------------------------------------------------------------

class TestLlmResponseExtraction:
    def test_extract_text_from_valid_envelope(self):
        scorer = make_scorer()
        text = '{"d1_enforcement": {"score": 1, "begruendung": "x"}}'
        envelope = make_custom_llm_output(text)

        result = scorer._extract_text(envelope)
        assert result == text

    def test_extract_text_missing_message(self):
        scorer = make_scorer()
        bad_envelope = {"output": [{"type": "other", "content": []}]}

        result = scorer._extract_text(bad_envelope)
        assert result is None

    def test_extract_text_missing_output_text(self):
        scorer = make_scorer()
        bad_envelope = {
            "output": [{"type": "message", "content": [{"type": "other", "text": "hi"}]}]
        }

        result = scorer._extract_text(bad_envelope)
        assert result is None


# ---------------------------------------------------------------------------
# Integration with ContentItem fields
# ---------------------------------------------------------------------------

class TestContentItemIntegration:
    def test_score_attaches_to_content_item_fields(self):
        """Verifies that calling code (unified_enricher) can assign results to item."""
        item = make_item()
        scorer = make_scorer()
        payload = make_llm_response(3, 3, 2, 1, 3)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(item)

        assert result is not None
        # Simulate what unified_enricher.py does
        item.relevance_score = result["relevance_score"]
        item.relevance_level = result["relevance_level"]
        item.relevance_dimensions = result["relevance_dimensions"]
        item.relevance_practice_area = result.get("relevance_practice_area")

        assert item.relevance_score == 12
        assert item.relevance_level == "Hoch"
        assert item.relevance_dimensions["d1_enforcement"] == 3
        assert item.relevance_practice_area == "Wirtschaftsstrafrecht"

    def test_relevance_fields_in_to_dict(self):
        """to_dict() must include relevance fields when set."""
        item = make_item()
        item.relevance_score = 7
        item.relevance_level = "Mittel"
        item.relevance_dimensions = dict.fromkeys(DIMENSION_KEYS, 1)
        item.relevance_practice_area = "ESG/Regulatory"
        # Set required published/discovered/extracted timestamps
        item.published_at = "2026-01-01T00:00:00Z"
        item.discovered_at = "2026-01-01T00:00:00Z"
        item.extracted_at = "2026-01-01T00:00:00Z"

        d = item.to_dict()
        assert d["relevance_score"] == 7
        assert d["relevance_level"] == "Mittel"
        assert d["relevance_dimensions"] == dict.fromkeys(DIMENSION_KEYS, 1)
        assert d["relevance_practice_area"] == "ESG/Regulatory"

    def test_relevance_fields_absent_when_none(self):
        """to_dict() must omit relevance fields when not set."""
        item = make_item()
        item.published_at = "2026-01-01T00:00:00Z"
        item.discovered_at = "2026-01-01T00:00:00Z"
        item.extracted_at = "2026-01-01T00:00:00Z"

        d = item.to_dict()
        assert "relevance_score" not in d
        assert "relevance_level" not in d
        assert "relevance_dimensions" not in d
        assert "relevance_practice_area" not in d


# ---------------------------------------------------------------------------
# Practice area tagging
# ---------------------------------------------------------------------------

class TestPracticeArea:
    """Tests for the practice_area field per RELEVANCE_SCORING.md §Practice Area Values."""

    def test_valid_practice_area_returned(self):
        for area in PRACTICE_AREAS:
            scorer = make_scorer()
            payload = make_llm_response(1, 0, 1, 0, 1, practice_area=area)

            with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
                result = scorer.score(make_item())

            assert result is not None, f"score() returned None for area={area!r}"
            assert result["relevance_practice_area"] == area

    def test_invalid_practice_area_returns_none_for_field_only(self):
        """Invalid practice_area must NOT invalidate the whole score — only the field is None."""
        scorer = make_scorer()
        payload = make_llm_response(1, 1, 1, 1, 1, practice_area="Ungültiger Bereich")

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 5
        assert result["relevance_practice_area"] is None

    def test_missing_practice_area_returns_none_for_field_only(self):
        """Absent practice_area must NOT invalidate the whole score."""
        scorer = make_scorer()
        payload = make_llm_response(1, 1, 1, 1, 1)
        del payload["practice_area"]

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_score"] == 5
        assert result["relevance_practice_area"] is None

    def test_esg_regulatory_area(self):
        scorer = make_scorer()
        payload = make_llm_response(0, 0, 1, 2, 2, practice_area="ESG/Regulatory")

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_practice_area"] == "ESG/Regulatory"

    def test_investigativer_journalismus_area(self):
        scorer = make_scorer()
        payload = make_llm_response(1, 0, 2, 0, 3, practice_area="Investigativer Journalismus")

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result is not None
        assert result["relevance_practice_area"] == "Investigativer Journalismus"

    def test_practice_area_in_to_dict(self):
        """practice_area propagated through unified_enricher ends up in ContentItem.to_dict()."""
        item = make_item()
        item.relevance_practice_area = "Interne Untersuchungen"
        item.relevance_score = 8
        item.relevance_level = "Mittel"
        item.relevance_dimensions = dict.fromkeys(DIMENSION_KEYS, 1)
        item.published_at = "2026-01-01T00:00:00Z"
        item.discovered_at = "2026-01-01T00:00:00Z"
        item.extracted_at = "2026-01-01T00:00:00Z"

        d = item.to_dict()
        assert d["relevance_practice_area"] == "Interne Untersuchungen"

    def test_practice_area_absent_when_none_in_to_dict(self):
        """to_dict() omits relevance_practice_area when not set."""
        item = make_item()
        item.published_at = "2026-01-01T00:00:00Z"
        item.discovered_at = "2026-01-01T00:00:00Z"
        item.extracted_at = "2026-01-01T00:00:00Z"

        d = item.to_dict()
        assert "relevance_practice_area" not in d


# ---------------------------------------------------------------------------
# Example scores from spec (sanity checks)
# ---------------------------------------------------------------------------

class TestSpecExamples:
    """Verify the example scores from RELEVANCE_SCORING.md §Example Scores."""

    def test_sozialbetrug_example(self):
        """Beispiel A: Sozialbetrug → score=4, Niedrig."""
        scorer = make_scorer(bonus_rule=False)
        payload = make_llm_response(d1=3, d2=0, d3=0, d4=1, d5=0)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result["relevance_score"] == 4
        assert result["relevance_level"] == "Niedrig"

    def test_gc_kuendigung_example_no_bonus(self):
        """Beispiel B without bonus: D1=2,D2=3,D3=3,D4=1,D5=3 → score=12, Hoch."""
        scorer = make_scorer(bonus_rule=False)
        payload = make_llm_response(d1=2, d2=3, d3=3, d4=1, d5=3)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result["relevance_score"] == 12
        assert result["relevance_level"] == "Hoch"

    def test_gc_kuendigung_example_with_bonus(self):
        """Beispiel B with bonus: same scores +2 bonus → score=14, Hoch."""
        scorer = make_scorer(bonus_rule=True)
        payload = make_llm_response(d1=2, d2=3, d3=3, d4=1, d5=3)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result["relevance_score"] == 14
        assert result["relevance_level"] == "Hoch"

    def test_emily_false_positive_example(self):
        """Beispiel C: Emily-Fall → score=2, Niedrig (false positive caught)."""
        scorer = make_scorer(bonus_rule=False)
        payload = make_llm_response(d1=1, d2=0, d3=0, d4=1, d5=0)

        with patch.object(scorer, "_call_llm", return_value=json.dumps(payload)):
            result = scorer.score(make_item())

        assert result["relevance_score"] == 2
        assert result["relevance_level"] == "Niedrig"


class TestCategorySchemaSupport:
    def test_schema_prompt_is_used(self):
        scorer = make_scorer()
        payload = make_llm_response(1, 1, 1, 1, 1, practice_area="Area A")
        schema = make_schema()
        captured_prompts: list[str] = []

        def fake_call_llm(prompt: str, system_message: str = "") -> str:
            captured_prompts.append(system_message + "\n" + prompt)
            return json.dumps(payload)

        with patch.object(scorer, "_call_llm", side_effect=fake_call_llm):
            result = scorer.score(make_item(), schema=schema)

        assert result is not None
        assert captured_prompts, "Expected at least one prompt capture"
        assert "SYSTEM CUSTOM" in captured_prompts[0]
        assert "D1 Custom" in captured_prompts[0]
        assert "Area A" in captured_prompts[0]

    def test_schema_specific_practice_area_validation(self):
        scorer = make_scorer()
        schema = make_schema(practice_areas=["Area A", "Area B"])

        valid_payload = make_llm_response(1, 1, 1, 1, 1, practice_area="Area B")
        with patch.object(scorer, "_call_llm", return_value=json.dumps(valid_payload)):
            valid_result = scorer.score(make_item(), schema=schema)
        assert valid_result is not None
        assert valid_result["relevance_practice_area"] == "Area B"

        invalid_payload = make_llm_response(1, 1, 1, 1, 1, practice_area="ESG/Regulatory")
        with patch.object(scorer, "_call_llm", return_value=json.dumps(invalid_payload)):
            invalid_result = scorer.score(make_item(), schema=schema)
        assert invalid_result is not None
        assert invalid_result["relevance_practice_area"] is None
