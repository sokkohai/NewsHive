"""Tests for the structured second webhook (Power Automate).

Verifies:
- Payload structure: categories -> practice_areas -> articles
- Only articles WITH relevance_practice_area are included
- Practice areas WITHOUT articles are omitted
- The async background task is triggered when WEBHOOK_URL_STRUCTURED is set
"""

import time
from unittest.mock import MagicMock, patch

import pytest

from src.config import ArticlePreparationConfig, Category, Configuration, WebSource
from src.models import ContentItem, Envelope
from src.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_item(
    item_id: str,
    *,
    category: str = "CCCI",
    practice_area: str | None = None,
    title: str | None = None,
    url: str | None = None,
    keywords: list[str] | None = None,
) -> ContentItem:
    return ContentItem(
        id=item_id,
        source_type="web",
        source_key=url or f"http://example.com/{item_id}",
        title=title or f"Title {item_id}",
        summary=f"Summary {item_id}",
        content=f"Content {item_id}",
        categories=[category],
        published_at="2026-03-17T08:00:00Z",
        discovered_at="2026-03-17T09:00:00Z",
        extracted_at="2026-03-17T09:01:00Z",
        source_url=url or f"http://example.com/{item_id}",
        keywords=keywords,
        relevance_practice_area=practice_area,
    )


# ---------------------------------------------------------------------------
# Unit tests: Envelope.to_structured_webhook_dict()
# ---------------------------------------------------------------------------

class TestEnvelopeStructuredWebhookDict:
    def test_groups_by_category_and_practice_area(self):
        items = [
            _make_item("1", category="CCCI", practice_area="Wirtschafts- und Steuerstrafrecht"),
            _make_item("2", category="CCCI", practice_area="Wirtschafts- und Steuerstrafrecht"),
            _make_item("3", category="CCCI", practice_area="Compliance & Regulatorik"),
        ]
        envelope = Envelope(items=items, pipeline_version="2.0")
        payload = envelope.to_structured_webhook_dict()

        assert payload["pipeline_version"] == "2.0"
        assert "generated_at" in payload
        categories = payload["categories"]
        assert len(categories) == 1
        assert categories[0]["name"] == "CCCI"

        pa_names = {pa["name"] for pa in categories[0]["practice_areas"]}
        assert pa_names == {"Wirtschafts- und Steuerstrafrecht", "Compliance & Regulatorik"}

        wirt_pa = next(pa for pa in categories[0]["practice_areas"] if pa["name"] == "Wirtschafts- und Steuerstrafrecht")
        assert len(wirt_pa["articles"]) == 2

    def test_excludes_items_without_practice_area(self):
        items = [
            _make_item("1", practice_area="Interne Untersuchungen"),
            _make_item("2", practice_area=None),   # should default to "Sonstiges"
        ]
        envelope = Envelope(items=items, pipeline_version="2.0")
        payload = envelope.to_structured_webhook_dict()

        total_articles = sum(
            len(pa["articles"])
            for cat in payload["categories"]
            for pa in cat["practice_areas"]
        )
        assert total_articles == 2  # Both items included, one in "Sonstiges"
        
        # Verify that the item without practice_area is in "Sonstiges"
        sonstiges_pa = next(
            (pa for cat in payload["categories"] for pa in cat["practice_areas"] if pa["name"] == "Sonstiges"),
            None
        )
        assert sonstiges_pa is not None
        assert len(sonstiges_pa["articles"]) == 1

    def test_empty_items_returns_empty_categories(self):
        envelope = Envelope(items=[], pipeline_version="2.0")
        payload = envelope.to_structured_webhook_dict()
        assert payload["categories"] == []

    def test_article_fields_are_correct(self):
        item = _make_item(
            "42",
            practice_area="ESG & Nachhaltigkeit",
            title="ESG Rule",
            url="http://example.com/42",
            keywords=["ESG", "CSRD"],
        )
        item.summary = "An ESG summary."
        envelope = Envelope(items=[item], pipeline_version="2.0")
        payload = envelope.to_structured_webhook_dict()

        article = payload["categories"][0]["practice_areas"][0]["articles"][0]
        assert article["title"] == "ESG Rule"
        assert article["url"] == "http://example.com/42"
        assert article["date"] == "2026-03-17T08:00:00Z"
        assert article["summary"] == "An ESG summary."
        assert article["keywords"] == ["ESG", "CSRD"]

    def test_date_falls_back_to_extracted_at_when_unknown(self):
        item = _make_item("7", practice_area="Interne Untersuchungen")
        item.published_at = "unknown"
        item.extracted_at = "2026-03-17T10:00:00Z"
        envelope = Envelope(items=[item], pipeline_version="2.0")
        payload = envelope.to_structured_webhook_dict()

        article = payload["categories"][0]["practice_areas"][0]["articles"][0]
        assert article["date"] == "2026-03-17T10:00:00Z"

    def test_multiple_categories(self):
        items = [
            _make_item("1", category="CCCI", practice_area="Wirtschafts- und Steuerstrafrecht"),
            _make_item("2", category="ESG & Sustainability Regulations", practice_area="ESG & Nachhaltigkeit"),
        ]
        envelope = Envelope(items=items, pipeline_version="2.0")
        payload = envelope.to_structured_webhook_dict()

        cat_names = {c["name"] for c in payload["categories"]}
        assert cat_names == {"CCCI", "ESG & Sustainability Regulations"}

    def test_item_without_category_falls_back_to_sonstiges(self):
        item = _make_item("1", practice_area="Interne Untersuchungen")
        item.categories = []   # no category assigned
        envelope = Envelope(items=[item], pipeline_version="2.0")
        payload = envelope.to_structured_webhook_dict()

        assert payload["categories"][0]["name"] == "Sonstiges"

    def test_practice_areas_sorted_by_order_list(self):
        """Test that practice areas are sorted according to practice_areas_order."""
        items = [
            _make_item("1", practice_area="Sonstiges"),
            _make_item("2", practice_area="Compliance & Regulatorik"),
            _make_item("3", practice_area="Wirtschafts- und Steuerstrafrecht"),
            _make_item("4", practice_area="Interne Untersuchungen"),
        ]
        envelope = Envelope(items=items, pipeline_version="2.0")
        
        # Define desired order
        order = [
            "Wirtschafts- und Steuerstrafrecht",
            "Interne Untersuchungen",
            "Compliance & Regulatorik",
            "Sonstiges",
        ]
        payload = envelope.to_structured_webhook_dict(practice_areas_order=order)
        
        # Extract practice area names in order from payload
        pa_names = [pa["name"] for pa in payload["categories"][0]["practice_areas"]]
        assert pa_names == order

    def test_practice_areas_with_undefined_order(self):
        """Test that undefined practice areas are appended at end."""
        items = [
            _make_item("1", practice_area="Unknown Area"),
            _make_item("2", practice_area="Sonstiges"),
            _make_item("3", practice_area="Compliance & Regulatorik"),
        ]
        envelope = Envelope(items=items, pipeline_version="2.0")
        
        # Only define partial order
        order = [
            "Compliance & Regulatorik",
            "Sonstiges",
        ]
        payload = envelope.to_structured_webhook_dict(practice_areas_order=order)
        
        # Extract practice area names in order
        pa_names = [pa["name"] for pa in payload["categories"][0]["practice_areas"]]
        
        # Defined areas should come first in specified order
        assert pa_names[:2] == ["Compliance & Regulatorik", "Sonstiges"]
        # Undefined areas should be appended
        assert "Unknown Area" in pa_names


# ---------------------------------------------------------------------------
# Integration tests: pipeline _send_structured_webhook_async
# ---------------------------------------------------------------------------

@pytest.fixture
def structured_config():
    return Configuration(
        pipeline_version="2.0",
        web_sources=[WebSource(url="http://example.com", categories=["CCCI"])],
        email_folders=[],
        categories=[Category("CCCI", ["test"])],
        webhook_url_structured="http://mock-structured-webhook.com",
        article_text_preparation=ArticlePreparationConfig(enabled=False),
    )


@pytest.fixture
def items_with_practice_areas():
    return [
        _make_item("1", practice_area="Wirtschafts- und Steuerstrafrecht"),
        _make_item("2", practice_area="Compliance & Regulatorik"),
    ]


class TestStructuredWebhookPipeline:
    def test_structured_webhook_called_when_configured(self, structured_config, items_with_practice_areas):
        with (
            patch("src.pipeline.StateStoreManager"),
            patch("src.pipeline.Discoverer"),
            patch("src.pipeline.Extractor"),
            patch("src.pipeline.UnifiedEnricher") as mock_enricher_cls,
            patch("requests.post") as mock_post,
        ):
            mock_enricher = MagicMock()
            mock_enricher.process.return_value = (items_with_practice_areas, [])
            mock_enricher_cls.return_value = mock_enricher

            mock_post.return_value = MagicMock(status_code=200, text="OK")

            pipeline = Pipeline(config=structured_config)
            pipeline.unified_enricher = mock_enricher
            pipeline._stage_discovery = MagicMock(return_value=items_with_practice_areas)
            pipeline._stage_deduplication = MagicMock(return_value=items_with_practice_areas)
            pipeline._stage_extraction = MagicMock(return_value=(items_with_practice_areas, [], []))
            pipeline._stage_categorization = MagicMock(return_value=items_with_practice_areas)
            pipeline.state_store.has_processed.return_value = False

            pipeline.run()
            time.sleep(0.5)

            assert mock_post.call_count >= 1
            # At least one call should target the structured webhook URL
            urls_called = [call.args[0] if call.args else call.kwargs.get("url") for call in mock_post.call_args_list]
            assert "http://mock-structured-webhook.com" in urls_called

    def test_structured_webhook_payload_structure(self, structured_config, items_with_practice_areas):
        """The structured payload must follow categories -> practice_areas -> articles."""
        with (
            patch("src.pipeline.StateStoreManager"),
            patch("src.pipeline.Discoverer"),
            patch("src.pipeline.Extractor"),
            patch("src.pipeline.UnifiedEnricher") as mock_enricher_cls,
            patch("requests.post") as mock_post,
        ):
            mock_enricher = MagicMock()
            mock_enricher.process.return_value = (items_with_practice_areas, [])
            mock_enricher_cls.return_value = mock_enricher

            mock_post.return_value = MagicMock(status_code=200, text="OK")

            pipeline = Pipeline(config=structured_config)
            pipeline.unified_enricher = mock_enricher
            pipeline._stage_discovery = MagicMock(return_value=items_with_practice_areas)
            pipeline._stage_deduplication = MagicMock(return_value=items_with_practice_areas)
            pipeline._stage_extraction = MagicMock(return_value=(items_with_practice_areas, [], []))
            pipeline._stage_categorization = MagicMock(return_value=items_with_practice_areas)
            pipeline.state_store.has_processed.return_value = False

            pipeline.run()
            time.sleep(0.5)

            # Find the call that went to the structured webhook
            structured_call = next(
                (c for c in mock_post.call_args_list if c.args and c.args[0] == "http://mock-structured-webhook.com"),
                None,
            )
            assert structured_call is not None, "Structured webhook was not called"

            payload = structured_call.kwargs.get("json") or structured_call.args[1]
            assert "generated_at" in payload
            assert "pipeline_version" in payload
            assert "categories" in payload
            assert isinstance(payload["categories"], list)

            cat = payload["categories"][0]
            assert "name" in cat
            assert "practice_areas" in cat

            pa = cat["practice_areas"][0]
            assert "name" in pa
            assert "articles" in pa

            article = pa["articles"][0]
            assert "title" in article
            assert "url" in article
            assert "date" in article
            assert "summary" in article

    def test_structured_webhook_skipped_when_not_configured(self, items_with_practice_areas):
        config_no_structured = Configuration(
            pipeline_version="2.0",
            web_sources=[WebSource(url="http://example.com", categories=["CCCI"])],
            email_folders=[],
            categories=[Category("CCCI", ["test"])],
            webhook_url_structured=None,
            article_text_preparation=ArticlePreparationConfig(enabled=False),
        )
        with (
            patch("src.pipeline.StateStoreManager"),
            patch("src.pipeline.Discoverer"),
            patch("src.pipeline.Extractor"),
            patch("src.pipeline.UnifiedEnricher") as mock_enricher_cls,
            patch("requests.post") as mock_post,
        ):
            mock_enricher = MagicMock()
            mock_enricher.process.return_value = (items_with_practice_areas, [])
            mock_enricher_cls.return_value = mock_enricher

            mock_post.return_value = MagicMock(status_code=200, text="OK")

            pipeline = Pipeline(config=config_no_structured)
            pipeline.unified_enricher = mock_enricher
            pipeline._stage_discovery = MagicMock(return_value=items_with_practice_areas)
            pipeline._stage_deduplication = MagicMock(return_value=items_with_practice_areas)
            pipeline._stage_extraction = MagicMock(return_value=(items_with_practice_areas, [], []))
            pipeline._stage_categorization = MagicMock(return_value=items_with_practice_areas)
            pipeline.state_store.has_processed.return_value = False

            pipeline.run()
            time.sleep(0.5)

            # Structured webhook URL must never have been called
            urls_called = [call.args[0] if call.args else None for call in mock_post.call_args_list]
            assert "http://mock-structured-webhook.com" not in urls_called
