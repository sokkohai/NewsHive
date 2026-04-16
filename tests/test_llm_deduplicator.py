"""Unit tests for LLM-based deduplication (llm_deduplicator.py)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.config import Configuration, LLMDeduplicationConfig
from src.llm_deduplicator import LLMDeduplicator
from src.models import ContentItem


@pytest.fixture
def mock_config():
    """Create a mock configuration with LLM deduplication settings."""
    config = MagicMock(spec=Configuration)
    config.llm_deduplication = LLMDeduplicationConfig(
        enabled=True,
        similarity_threshold=85,
        similarity_threshold_by_category={
            "CCCI": 85,
            "ESG & Sustainability Regulations": 80,
        },
        keep_strategy="highest_score",
        batch_size=100,
        verbose_logging=False,
    )
    return config


@pytest.fixture
def test_articles():
    """Create test articles for deduplication testing."""
    return [
        ContentItem(
            id="a1",
            source_type="web",
            source_key="https://example.com/article1",
            title="BGH entscheidet zu Geldwäsche",
            summary="Der BGH hat ein wichtiges Urteil zu Geldwäsche gefällt.",
            content="Volltext mit Details...",
            categories=["CCCI"],
            published_at="2026-03-17T08:00:00Z",
            discovered_at="2026-03-17T09:00:00Z",
            extracted_at="2026-03-17T09:01:00Z",
            relevance_score=12,
            relevance_level="Hoch",
            relevance_practice_area="Wirtschafts- und Steuerstrafrecht",
        ),
        ContentItem(
            id="a2",
            source_type="web",
            source_key="https://news.de/geldwaesche-urteil",
            title="BGH und Geldwäsche: neues Urteil",
            summary="Ein ähnliches Urteil des BGH zur Geldwäsche.",
            content="Verwandter Inhalt...",
            categories=["CCCI"],
            published_at="2026-03-17T09:00:00Z",
            discovered_at="2026-03-17T10:00:00Z",
            extracted_at="2026-03-17T10:01:00Z",
            relevance_score=10,
            relevance_level="Hoch",
            relevance_practice_area="Wirtschafts- und Steuerstrafrecht",
        ),
        ContentItem(
            id="a3",
            source_type="email",
            source_key="mail-csrd",
            title="EU CSRD Update",
            summary="Neuer Standard für Nachhaltigkeitsberichterstattung.",
            content="CSRD-bezogener Inhalt...",
            categories=["ESG & Sustainability Regulations"],
            published_at="2026-03-17T07:00:00Z",
            discovered_at="2026-03-17T10:30:00Z",
            extracted_at="2026-03-17T10:31:00Z",
            relevance_score=8,
            relevance_level="Mittel",
            relevance_practice_area="Compliance & Regulatorik",
        ),
    ]


def test_llm_deduplicator_initialization(mock_config):
    """Test LLMDeduplicator initialization."""
    dedup = LLMDeduplicator(
        config=mock_config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
        llm_api_url="https://api.example.com",
    )
    assert dedup.config == mock_config
    assert dedup.provider == "custom"
    assert dedup.model == "model-v1"
    assert dedup.api_key == "test-key"
    assert dedup.api_url == "https://api.example.com"


def test_deduplicate_empty_list(mock_config):
    """Test deduplication with empty list returns empty list."""
    dedup = LLMDeduplicator(
        config=mock_config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
    )
    result = dedup.deduplicate([])
    assert result == []


def test_deduplicate_single_item(mock_config):
    """Test deduplication with single item returns same item."""
    article = ContentItem(
        id="a1",
        source_type="web",
        source_key="https://example.com/article1",
        title="Test Article",
        summary="Summary",
        content="Content",
    )
    dedup = LLMDeduplicator(
        config=mock_config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
    )
    result = dedup.deduplicate([article])
    assert len(result) == 1
    assert result[0].id == "a1"


def test_deduplicate_disabled_in_config(mock_config):
    """Test that deduplication returns original list when disabled."""
    config = MagicMock(spec=Configuration)
    config.llm_deduplication = LLMDeduplicationConfig(enabled=False)
    
    articles = [
        ContentItem(
            id="a1",
            source_type="web",
            source_key="url1",
            title="Article 1",
            summary="Summary",
            content="Content",
        ),
        ContentItem(
            id="a2",
            source_type="web",
            source_key="url2",
            title="Article 2",
            summary="Summary",
            content="Content",
        ),
    ]
    
    dedup = LLMDeduplicator(
        config=config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
    )
    result = dedup.deduplicate(articles)
    assert len(result) == 2


def test_parse_similarity_response_valid_json(mock_config):
    """Test parsing valid LLM similarity response."""
    dedup = LLMDeduplicator(
        config=mock_config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
    )
    
    response = json.dumps({
        "similar_groups": [
            {"indices": [0, 1], "reason": "Beide über BGH Geldwäsche Urteil"},
            {"indices": [2], "reason": "Single item"}
        ]
    })
    
    groups = dedup._parse_similarity_response(response, num_items=3, verbose=False)
    
    # Should only include group with 2+ items
    assert len(groups) == 1
    assert groups[0] == [0, 1]


def test_parse_similarity_response_empty_groups(mock_config):
    """Test parsing response with no similar groups."""
    dedup = LLMDeduplicator(
        config=mock_config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
    )
    
    response = json.dumps({"similar_groups": []})
    
    groups = dedup._parse_similarity_response(response, num_items=3, verbose=False)
    assert groups == []


def test_parse_similarity_response_invalid_json(mock_config):
    """Test parsing invalid JSON response returns empty list."""
    dedup = LLMDeduplicator(
        config=mock_config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
    )
    
    response = "This is not JSON"
    
    groups = dedup._parse_similarity_response(response, num_items=3, verbose=False)
    assert groups == []


def test_filter_duplicates_keep_highest_score(mock_config):
    """Test _filter_duplicates keeps article with highest score."""
    articles = [
        ContentItem(
            id="a1",
            source_type="web",
            source_key="url1",
            title="Article 1",
            summary="Summary 1",
            content="Content 1",
            relevance_score=10,
            discovered_at="2026-03-17T09:00:00Z",
        ),
        ContentItem(
            id="a2",
            source_type="web",
            source_key="url2",
            title="Article 2",
            summary="Summary 2",
            content="Content 2",
            relevance_score=15,  # Higher score
            discovered_at="2026-03-17T08:00:00Z",
        ),
        ContentItem(
            id="a3",
            source_type="web",
            source_key="url3",
            title="Article 3",
            summary="Summary 3",
            content="Content 3",
            relevance_score=12,
        ),
    ]
    
    dedup = LLMDeduplicator(
        config=mock_config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
    )
    
    # Similar groups: [0, 1] (keep 1 because higher score), keep 2
    similar_groups = [[0, 1]]
    result = dedup._filter_duplicates(articles, similar_groups, verbose=False)
    
    # Should keep a2 (id from index 1) and a3 (index 2)
    assert len(result) == 2
    kept_ids = {item.id for item in result}
    assert "a2" in kept_ids  # Highest score in group
    assert "a3" in kept_ids  # Not in any group


def test_filter_duplicates_keep_earliest(mock_config):
    """Test _filter_duplicates with earliest keep strategy."""
    config = MagicMock(spec=Configuration)
    config.llm_deduplication = LLMDeduplicationConfig(
        enabled=True,
        keep_strategy="earliest",
    )
    
    articles = [
        ContentItem(
            id="a1",
            source_type="web",
            source_key="url1",
            title="Article 1",
            summary="Summary 1",
            content="Content 1",
            discovered_at="2026-03-17T09:00:00Z",
        ),
        ContentItem(
            id="a2",
            source_type="web",
            source_key="url2",
            title="Article 2",
            summary="Summary 2",
            content="Content 2",
            discovered_at="2026-03-17T08:00:00Z",  # Earlier
        ),
    ]
    
    dedup = LLMDeduplicator(
        config=config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
    )
    
    similar_groups = [[0, 1]]
    result = dedup._filter_duplicates(articles, similar_groups, verbose=False)
    
    assert len(result) == 1
    assert result[0].id == "a2"  # Earlier one


def test_build_deduplication_prompt(mock_config, test_articles):
    """Test prompt building for deduplication."""
    dedup = LLMDeduplicator(
        config=mock_config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
    )
    
    prompt = dedup._build_deduplication_prompt(test_articles)
    
    # Verify prompt contains expected elements
    assert "ähnlichen Inhalt" in prompt
    assert "similar_groups" in prompt
    assert "[0, 1]" in prompt or "indices" in prompt
    assert test_articles[0].title in prompt
    assert "85" in prompt  # threshold


@patch("src.llm_deduplicator.requests.post")
def test_call_custom_llm_success(mock_post, mock_config):
    """Test successful LLM call."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": '{"similar_groups": []}'
                    }
                ]
            }
        ]
    }
    mock_post.return_value = mock_response
    
    dedup = LLMDeduplicator(
        config=mock_config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
        llm_api_url="https://api.example.com",
    )
    
    result = dedup._call_custom_llm("test prompt")
    
    assert result == '{"similar_groups": []}'
    mock_post.assert_called_once()


@patch("src.llm_deduplicator.requests.post")
def test_call_custom_llm_failure(mock_post, mock_config):
    """Test LLM call failure handling."""
    mock_post.side_effect = Exception("Connection error")
    
    dedup = LLMDeduplicator(
        config=mock_config,
        llm_provider="custom",
        llm_model="model-v1",
        llm_api_key="test-key",
        llm_api_url="https://api.example.com",
    )
    
    result = dedup._call_custom_llm("test prompt")
    
    assert result is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
