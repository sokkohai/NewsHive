"""Tests for unified enrichment stage.

Tests the UnifiedEnricher class per specs/core/UNIFIED_ENRICHMENT.md.
"""

import json
import pytest
from unittest.mock import MagicMock, patch

from src.config import Category, Configuration, WebSource
from src.models import ContentItem
from src.unified_enricher import UnifiedEnricher


@pytest.fixture
def mock_config():
    """Create a mock configuration with categories."""
    return Configuration(
        pipeline_version="1.0",
        web_sources=[
            WebSource(
                url="https://example.com",
                categories=["ESG", "CCCI"],
            )
        ],
        email_folders=[],
        categories=[
            Category(name="ESG", keywords=["sustainability", "ESG", "carbon", "emissions"]),
            Category(name="CCCI", keywords=["investigation", "compliance", "DOJ", "corruption"]),
        ],
    )


@pytest.fixture
def sample_item():
    """Create a sample content item for testing."""
    return ContentItem(
        id="test-1",
        source_type="web",
        source_key="web:https://example.com/article1",
        title="Test Article Title - 5 min read",
        summary="",
        content="This company faces new ESG compliance requirements and sustainability standards. The carbon emissions reporting must comply with new regulations.",
        language_detected="en",
        source_url="https://example.com/article1",
    )


@pytest.fixture
def sample_item_no_keywords():
    """Create a content item that won't match any category keywords."""
    return ContentItem(
        id="test-2",
        source_type="web",
        source_key="web:https://example.com/article2",
        title="Random Article",
        summary="",
        content="This is a random article about cooking recipes and gardening tips.",
        language_detected="en",
        source_url="https://example.com/article2",
    )


class TestUnifiedEnricherInit:
    """Tests for UnifiedEnricher initialization."""

    def test_init_with_openai_provider(self, mock_config):
        """Test initialization with OpenAI provider."""
        with patch("openai.OpenAI") as mock_openai:
            mock_openai.return_value = MagicMock()
            enricher = UnifiedEnricher(
                config=mock_config,
                llm_provider="openai",
                llm_model="gpt-4",
                llm_api_key="test-key",
            )
            assert enricher.provider == "openai"
            assert enricher.model == "gpt-4"

    def test_init_with_custom_provider(self, mock_config):
        """Test initialization with custom provider."""
        enricher = UnifiedEnricher(
            config=mock_config,
            llm_provider="custom",
            llm_model="custom-model",
            llm_api_key="test-key",
            llm_api_url="https://custom.api.com",
        )
        assert enricher.provider == "custom"
        assert enricher.api_url == "https://custom.api.com"
        assert enricher.model == "custom-model"


class TestPromptBuilding:
    """Tests for prompt construction."""

    def test_build_single_call_prompt(self, mock_config, sample_item):
        """Test single-call JSON prompt construction.
        
        The prompt should include:
        - "JSON" instruction
        - Dimension definitions
        - Practice areas
        - Article content
        """
        enricher = UnifiedEnricher(
            config=mock_config,
            llm_provider="custom",
            llm_model="model", 
            llm_api_key="key",
            llm_api_url="http://api"
        )
        prompt = enricher._build_single_call_prompt(sample_item, None)

        assert "JSON" in prompt
        assert "cleaned_title" in prompt
        assert "relevance_dimensions" in prompt
        assert "1. Enforcement-Intensität" in prompt
        assert "Wirtschaftsstrafrecht" in prompt
        assert "Test Article Title" not in prompt # Prompt uses content primarily
        assert "compliance requirements" in prompt # Content check

    def test_escape_json_string(self, mock_config):
        """Test JSON string escaping."""
        with patch("openai.OpenAI"):
            enricher = UnifiedEnricher(
                config=mock_config,
                llm_provider="openai",
                llm_model="gpt-4",
                llm_api_key="test-key",
            )
            
            # Test various special characters
            assert enricher._escape_json_string('Hello\nWorld') == 'Hello\\nWorld'
            assert enricher._escape_json_string('Tab\there') == 'Tab\\there'
            assert enricher._escape_json_string('Quote"test') == 'Quote\\"test'


class TestLLMResponseParsing:
    """Tests for LLM response parsing."""

    def test_parse_valid_json(self, mock_config):
        """Test parsing valid JSON response."""
        with patch("openai.OpenAI"):
            enricher = UnifiedEnricher(
                config=mock_config,
                llm_provider="openai",
                llm_model="gpt-4",
                llm_api_key="test-key",
            )
            
            response = json.dumps({
                "cleaned_title": "Clean Title",
                "summary": "This is a summary.",
                "cleaned_summary": "This is a summary.",
                "validation_status": "PASS",
                "quality_notes": "Good quality."
            })
            
            result = enricher._parse_llm_response(response)
            assert result is not None
            assert result["cleaned_title"] == "Clean Title"
            assert result["validation_status"] == "PASS"

    def test_parse_json_with_markdown_code_block(self, mock_config):
        """Test parsing JSON wrapped in markdown code block."""
        with patch("openai.OpenAI"):
            enricher = UnifiedEnricher(
                config=mock_config,
                llm_provider="openai",
                llm_model="gpt-4",
                llm_api_key="test-key",
            )
            
            response = """```json
{
    "cleaned_title": "Test",
    "summary": "Summary text",
    "cleaned_summary": "Summary text",
    "validation_status": "PASS",
    "quality_notes": ""
}
```"""
            
            result = enricher._parse_llm_response(response)
            assert result is not None
            assert result["cleaned_title"] == "Test"

    def test_parse_single_quoted_json(self, mock_config):
        """Test parsing JSON with single quotes (Python dict syntax)."""
        with patch("openai.OpenAI"):
            enricher = UnifiedEnricher(
                config=mock_config,
                llm_provider="openai",
                llm_model="gpt-4",
                llm_api_key="test-key",
            )
            
            result = enricher._parse_llm_response("{'cleaned_title': 'Single Quote', 'validation_status': 'PASS'}")
            
            assert result is not None
            assert result["cleaned_title"] == "Single Quote"
            assert result["validation_status"] == "PASS"

    def test_parse_json_with_preamble(self, mock_config):
        """Test parsing JSON with text before and after."""
        with patch("openai.OpenAI"):
            enricher = UnifiedEnricher(
                config=mock_config,
                llm_provider="openai",
                llm_model="gpt-4",
                llm_api_key="test-key",
            )
            
            response = "Here is the JSON:\n{'cleaned_title': 'Preamble Test'}\nHope this helps."
            result = enricher._parse_llm_response(response)
            
            assert result is not None
            assert result["cleaned_title"] == "Preamble Test"

    def test_parse_invalid_json(self, mock_config):
        """Test handling of invalid JSON response."""
        enricher = UnifiedEnricher(
            config=mock_config,
            llm_provider="openai",
            llm_model="gpt-4",
            llm_api_key="test-key",
        )
        
        result = enricher._parse_llm_response("This is not JSON")
        assert result is None

    def test_parse_list_returns_none(self, mock_config):
        """Test that parsing a JSON list returns None instead of a list."""
        enricher = UnifiedEnricher(
            config=mock_config,
            llm_provider="openai",
            llm_model="gpt-4",
            llm_api_key="test-key",
        )
        
        # Test with JSON list
        result = enricher._parse_llm_response('["item1", "item2"]')
        assert result is None


class TestCategoryMatching:
    """Tests for local keyword-based category matching."""

    def test_matches_esg_keywords(self, mock_config, sample_item):
        """Test that ESG keywords are matched correctly."""
        with patch("openai.OpenAI"):
            enricher = UnifiedEnricher(
                config=mock_config,
                llm_provider="openai",
                llm_model="gpt-4",
                llm_api_key="test-key",
            )
            
            categories = enricher._match_categories_by_keywords(sample_item)
            assert "ESG" in categories

    def test_no_match_returns_empty(self, mock_config, sample_item_no_keywords):
        """Test that non-matching content returns empty categories."""
        with patch("openai.OpenAI"):
            enricher = UnifiedEnricher(
                config=mock_config,
                llm_provider="openai",
                llm_model="gpt-4",
                llm_api_key="test-key",
            )
            
            categories = enricher._match_categories_by_keywords(sample_item_no_keywords)
            assert categories == []


class TestProcessMethod:
    """Tests for the main process method."""

    def test_process_successful_enrichment(self, mock_config, sample_item):
        """Test successful enrichment of an article with JSON response."""
        enricher = UnifiedEnricher(
            config=mock_config,
            llm_provider="custom",
            llm_model="model",
            llm_api_key="key",
            llm_api_url="http://api"
        )
        
        # Mock LLM to return valid JSON
        with patch.object(enricher, '_call_llm') as mock_call:
            mock_call.return_value = json.dumps({
                "title": "New Title",
                "summary": "This is a summary.",
                "quality_score": 0.8,
                "relevance_dimensions": {
                    "d1_enforcement": 3,
                    "d2_organ": 3, 
                    "d3_compliance": 2, 
                    "d4_regulatory": 0,
                    "d5_mandate": 2
                }, # Total = 10 (Hoch)
                "practice_area": "Wirtschaftsstrafrecht"
            })
            
            # process() internally calls _match_categories_by_keywords first
            # sample_item has keywords for ESG
            enriched, failed = enricher.process([sample_item])

            assert len(enriched) == 1
            assert len(failed) == 0
            
            item = enriched[0]
            assert item.title == "New Title"
            assert item.summary == "This is a summary."
            assert item.relevance_score == 10
            assert item.relevance_level == "Hoch"
            assert item.relevance_practice_area == "Wirtschaftsstrafrecht"
            assert item.validation_status == "PASS"

    def test_process_llm_failure(self, mock_config, sample_item):
        """Test handling of LLM call failure."""
        enricher = UnifiedEnricher(
            config=mock_config,
            llm_provider="custom",
            llm_model="model",
            llm_api_key="key",
            llm_api_url="http://api"
        )
        
        with patch.object(enricher, '_call_llm') as mock_call:
            mock_call.return_value = None  # fail
            
            enriched, failed = enricher.process([sample_item])
            
            assert len(enriched) == 0
            assert len(failed) == 1
            assert failed[0][1] == "llm_call_failed"

    def test_process_validation_status_pass(self, mock_config, sample_item):
        """Test quality score translates to validation status."""
        enricher = UnifiedEnricher(
            config=mock_config,
            llm_provider="custom",
            llm_model="model",
            llm_api_key="key",
            llm_api_url="http://api"
        )
        
        with patch.object(enricher, '_call_llm') as mock_call:
            mock_call.return_value = json.dumps({
                "cleaned_title": "Title",
                "summary": "Summary",
                "quality_score": 0.9, # Should produce PASS
                "relevance_dimensions": {
                    "d1_enforcement": 3, 
                    "d2_organ": 3,
                    "d3_compliance": 2,
                    "d4_regulatory": 1,
                    "d5_mandate": 1
                }, # Total = 10 (Hoch)
                "practice_area": "Sonstiges"
            })
            
            enriched, failed = enricher.process([sample_item])
            
            assert len(enriched) == 1
            assert enriched[0].validation_status == "PASS"

    def test_process_no_categories_matched(self, mock_config, sample_item_no_keywords):
        """Test filtering when no categories match (local keyword filter)."""
        enricher = UnifiedEnricher(
            config=mock_config,
            llm_provider="custom",
            llm_model="model",
            llm_api_key="key",
            llm_api_url="http://api"
        )
        
        # Don't mock LLM because it shouldn't be called
        enriched, failed = enricher.process([sample_item_no_keywords])
        
        assert len(enriched) == 0
        assert len(failed) == 1
        assert failed[0][1] == "no_categories_matched"

    def test_process_filters_low_relevance_score(self, mock_config, sample_item):
        """Test filtering of items with low relevance score."""
        enricher = UnifiedEnricher(
            config=mock_config,
            llm_provider="custom",
            llm_model="model",
            llm_api_key="key",
            llm_api_url="http://api"
        )
        
        with patch.object(enricher, '_call_llm') as mock_call:
            mock_call.return_value = json.dumps({
                "cleaned_title": "Title",
                "summary": "Summary",
                "quality_score": 0.5,
                "relevance_dimensions": {
                    "d1_enforcement": 0,
                    "d2_organ": 0, 
                    "d3_compliance": 0, 
                    "d4_regulatory": 0,
                    "d5_mandate": 0
                } # Total = 0 (< 5)
            })
            
            enriched, failed = enricher.process([sample_item])
            
            assert len(enriched) == 0
            assert len(failed) == 1
            assert failed[0][1] == "relevance_level_too_low: Niedrig"

    def test_process_multiple_items(self, mock_config, sample_item):
        """Test processing logic handles multiple items (loop)."""
        enricher = UnifiedEnricher(
            config=mock_config,
            llm_provider="custom",
            llm_model="model",
            llm_api_key="key",
            llm_api_url="http://api"
        )
        
        item2 = ContentItem(
             id="test-2",
             source_type="web",
             source_key="web:https://example.com/2",
             title="Article 2",
             summary="",
             content="Compliance content here.",
             language_detected="en",
             source_url="http://u",
        )

        with patch.object(enricher, '_call_llm') as mock_call:
            mock_call.return_value = json.dumps({
                 "title": "Title",
                 "summary": "Summary",
                 "relevance_dimensions": {
                    "d1_enforcement": 3, 
                    "d2_organ": 3,
                    "d3_compliance": 2,
                    "d4_regulatory": 1,
                    "d5_mandate": 1
                }, # Total = 10 (Hoch)
                 "quality_score": 1.0
            })
            
            enriched, failed = enricher.process([sample_item, item2])
            
            # Both match keywords (sample_item matches ESG, item2 matches nothing in mock_config.keywords? Wait.)
            # item2 content "Compliance content here." matches "compliance" in "CCCI" category.
            # So both should pass local filter.
            
            assert len(enriched) == 2
            assert len(failed) == 0
            assert mock_call.call_count == 2


class TestLLMCall:
    """Tests for direct LLM calls."""


    def test_call_llm_custom_provider_strict_format(self, mock_config):
        """Test custom provider using stricter output/message/content/output_text format."""
        with patch("requests.post") as mock_post:
            mock_response = MagicMock()
            # Valid response matching the new strict requirement
            mock_response.json.return_value = {
                "output": [
                    {"type": "reasoning", "summary": []},
                    {
                        "type": "message",
                        "content": [
                            {"type": "output_text", "text": "valid json content"}
                        ]
                    }
                ]
            }
            mock_response.raise_for_status.return_value = None
            mock_post.return_value = mock_response

            enricher = UnifiedEnricher(
                config=mock_config,
                llm_provider="custom",
                llm_model="custom-model",
                llm_api_key="test-key",
                llm_api_url="https://custom.api.com",
            )
            
            result = enricher._call_llm("test prompt")
            assert result == "valid json content"
            
            # Verify request format
            call_kwargs = mock_post.call_args.kwargs
            assert "json" in call_kwargs
            assert "input" in call_kwargs["json"]
            assert "model" in call_kwargs["json"]
            assert enricher.SYSTEM_MESSAGE in call_kwargs["json"]["input"]

    def test_call_llm_custom_provider_invalid_format(self, mock_config):
        """Test proper failure when custom provider returns unexpected format."""
        with patch("requests.post") as mock_post:
            mock_response = MagicMock()
            # Invalid response (missing output list)
            mock_response.json.return_value = {"error": "some error"}
            mock_response.raise_for_status.return_value = None
            mock_post.return_value = mock_response

            enricher = UnifiedEnricher(
                config=mock_config,
                llm_provider="custom",
                llm_model="custom-model",
                llm_api_key="test-key",
                llm_api_url="https://custom.api.com",
            )
            
            # Should look like a failure (return None) and log error
            # _call_custom_llm catches exception and returns None
            result = enricher._call_llm("test prompt")
            assert result is None

