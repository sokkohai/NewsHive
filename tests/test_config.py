"""Tests for configuration management.

Tests Configuration, Category, WebSource and ConfigLoader as specified in
specs/core/CONFIGURATION.md.
"""

import json
import tempfile
from pathlib import Path

import pytest
import yaml

from src.config import ConfigLoader, Configuration, Category, WebSource


class TestCategory:
    """Test Category data model."""

    def test_create_category(self):
        """Test creating a Category."""
        category = Category(name="compliance", keywords=["GDPR", "regulation"])
        assert category.name == "compliance"
        assert len(category.keywords) == 2

    def test_category_from_dict(self):
        """Test Category from_dict()."""
        data = {"name": "compliance", "keywords": ["GDPR", "compliance", "regulation"]}
        category = Category.from_dict(data)
        assert category.name == "compliance"
        assert len(category.keywords) == 3

    def test_category_from_dict_missing_keywords(self):
        """Test Category from_dict() with missing keywords."""
        data = {"name": "compliance"}
        category = Category.from_dict(data)
        assert category.keywords == []

    def test_category_to_dict(self):
        """Test Category to_dict()."""
        category = Category(name="compliance", keywords=["GDPR"])
        d = category.to_dict()
        assert d["name"] == "compliance"
        assert d["keywords"] == ["GDPR"]

    def test_category_with_relevance_schema_from_dict(self):
        """Test Category parses optional relevance_schema."""
        data = {
            "name": "CCCI",
            "keywords": ["DOJ"],
            "relevance_schema": {
                "schema_id": "ccci_v1",
                "dimensions": {
                    "d1_enforcement": {
                        "label": "Enforcement",
                        "question": "Gibt es Maßnahmen?",
                        "scores": {"0": "nein", "1": "niedrig", "2": "mittel", "3": "hoch"},
                    },
                    "d2_organ": {
                        "label": "Organ",
                        "question": "Ist Management betroffen?",
                        "scores": {"0": "nein", "1": "indirekt", "2": "genannt", "3": "direkt"},
                    },
                    "d3_compliance": {
                        "label": "Compliance",
                        "question": "Systemischer Bezug?",
                        "scores": {"0": "nein", "1": "leicht", "2": "mittel", "3": "hoch"},
                    },
                    "d4_regulatory": {
                        "label": "Regulatory",
                        "question": "Regulatorischer Impact?",
                        "scores": {"0": "nein", "1": "routine", "2": "neu", "3": "regime"},
                    },
                    "d5_mandate": {
                        "label": "Mandat",
                        "question": "Mandatspotenzial?",
                        "scores": {"0": "nein", "1": "möglich", "2": "wahrscheinlich", "3": "hoch"},
                    },
                },
                "practice_areas": ["Wirtschaftsstrafrecht", "Sonstiges"],
            },
        }

        category = Category.from_dict(data)
        assert category.relevance_schema is not None
        assert category.relevance_schema.schema_id == "ccci_v1"
        assert category.relevance_schema.practice_areas == ["Wirtschaftsstrafrecht", "Sonstiges"]

    def test_category_with_relevance_schema_to_dict(self):
        """Test Category serializes optional relevance_schema."""
        data = {
            "name": "CCCI",
            "keywords": ["DOJ"],
            "relevance_schema": {
                "schema_id": "ccci_v1",
                "dimensions": {
                    "d1_enforcement": {
                        "label": "Enforcement",
                        "question": "Q1",
                        "scores": {"0": "a", "1": "b", "2": "c", "3": "d"},
                    },
                    "d2_organ": {
                        "label": "Organ",
                        "question": "Q2",
                        "scores": {"0": "a", "1": "b", "2": "c", "3": "d"},
                    },
                    "d3_compliance": {
                        "label": "Compliance",
                        "question": "Q3",
                        "scores": {"0": "a", "1": "b", "2": "c", "3": "d"},
                    },
                    "d4_regulatory": {
                        "label": "Regulatory",
                        "question": "Q4",
                        "scores": {"0": "a", "1": "b", "2": "c", "3": "d"},
                    },
                    "d5_mandate": {
                        "label": "Mandate",
                        "question": "Q5",
                        "scores": {"0": "a", "1": "b", "2": "c", "3": "d"},
                    },
                },
            },
        }
        category = Category.from_dict(data)

        serialized = category.to_dict()
        assert "relevance_schema" in serialized
        assert serialized["relevance_schema"]["schema_id"] == "ccci_v1"


class TestConfiguration:
    """Test Configuration data model."""

    def test_create_minimal_configuration(self):
        """Test creating a minimal valid Configuration."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[WebSource(url="https://example.com", categories=["compliance"])],
            email_folders=[],
            categories=[Category(name="compliance", keywords=["GDPR"])],
        )

        assert config.pipeline_version == "1.0"
        assert len(config.web_sources) == 1

    def test_configuration_with_email_sources(self):
        """Test Configuration with email sources."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=["Inbox", "Inbox/Compliance"],
            categories=[Category(name="compliance", keywords=["GDPR"])],
        )

        assert len(config.email_folders) == 2

    def test_configuration_validation_success(self):
        """Test that valid configuration passes validation."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[WebSource(url="https://example.com", categories=["compliance"])],
            email_folders=[],
            categories=[Category(name="compliance", keywords=["GDPR"])],
        )

        # Should not raise
        config.validate()

    def test_configuration_validation_missing_version(self):
        """Test validation fails without pipeline_version."""
        config = Configuration(
            pipeline_version="",
            web_sources=[WebSource(url="https://example.com", categories=["test"])],
            email_folders=[],
            categories=[Category(name="test", keywords=["keyword"])],
        )

        with pytest.raises(ValueError):
            config.validate()

    def test_configuration_validation_no_sources(self):
        """Test validation fails without sources."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[],
            email_folders=[],
            categories=[Category(name="test", keywords=["keyword"])],
        )

        with pytest.raises(ValueError):
            config.validate()

    def test_configuration_validation_no_keywords(self):
        """Test validation allows categories without keywords."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[WebSource(url="https://example.com", categories=["test"])],
            email_folders=[],
            categories=[Category(name="test", keywords=[])],
        )

        # Keywords are optional for some configs (email-only or external filters)
        config.validate()

    def test_configuration_from_dict(self):
        """Test Configuration from_dict()."""
        data = {
            "pipeline_version": "1.0",
            "web_sources": [{"url": "https://example.com", "categories": ["compliance"]}],
            "email_folders": [],
            "categories": [{"name": "compliance", "keywords": ["GDPR"]}],
        }

        config = Configuration.from_dict(data)
        assert config.pipeline_version == "1.0"
        assert len(config.categories) == 1

    def test_configuration_to_dict(self):
        """Test Configuration to_dict()."""
        config = Configuration(
            pipeline_version="1.0",
            web_sources=[WebSource(url="https://example.com", categories=["compliance"])],
            email_folders=[],
            categories=[Category(name="compliance", keywords=["GDPR"])],
        )

        d = config.to_dict()
        assert d["pipeline_version"] == "1.0"
        assert len(d["web_sources"]) == 1
        assert d["web_sources"][0]["url"] == "https://example.com"
        assert len(d["categories"]) == 1

    def test_configuration_validation_invalid_relevance_schema_missing_dimension(self):
        """Test validation fails if relevance_schema misses required dimensions."""
        data = {
            "pipeline_version": "1.0",
            "web_sources": [{"url": "https://example.com", "categories": ["CCCI"]}],
            "email_folders": [],
            "categories": [
                {
                    "name": "CCCI",
                    "keywords": ["DOJ"],
                    "relevance_schema": {
                        "schema_id": "ccci_v1",
                        "dimensions": {
                            "d1_enforcement": {
                                "label": "Enforcement",
                                "question": "Q1",
                                "scores": {"0": "a", "1": "b", "2": "c", "3": "d"},
                            }
                        },
                    },
                }
            ],
        }

        with pytest.raises(ValueError):
            Configuration.from_dict(data)

    def test_configuration_validation_duplicate_relevance_schema_ids(self):
        """Test validation fails on duplicate relevance schema IDs across categories."""
        full_dimensions = {
            "d1_enforcement": {"label": "D1", "question": "Q1", "scores": {"0": "a", "1": "b", "2": "c", "3": "d"}},
            "d2_organ": {"label": "D2", "question": "Q2", "scores": {"0": "a", "1": "b", "2": "c", "3": "d"}},
            "d3_compliance": {"label": "D3", "question": "Q3", "scores": {"0": "a", "1": "b", "2": "c", "3": "d"}},
            "d4_regulatory": {"label": "D4", "question": "Q4", "scores": {"0": "a", "1": "b", "2": "c", "3": "d"}},
            "d5_mandate": {"label": "D5", "question": "Q5", "scores": {"0": "a", "1": "b", "2": "c", "3": "d"}},
        }
        data = {
            "pipeline_version": "1.0",
            "web_sources": [{"url": "https://example.com", "categories": ["CCCI", "ESG"]}],
            "email_folders": [],
            "categories": [
                {
                    "name": "CCCI",
                    "keywords": ["DOJ"],
                    "relevance_schema": {"schema_id": "shared_v1", "dimensions": full_dimensions},
                },
                {
                    "name": "ESG",
                    "keywords": ["CSRD"],
                    "relevance_schema": {"schema_id": "shared_v1", "dimensions": full_dimensions},
                },
            ],
        }

        with pytest.raises(ValueError):
            Configuration.from_dict(data)


class TestConfigLoader:
    """Test ConfigLoader."""

    def test_load_config_from_file(self):
        """Test loading configuration from file."""
        config_data = {
            "pipeline_version": "1.0",
            "web_sources": [{"url": "https://example.com", "categories": ["compliance"]}],
            "email_folders": [],
            "categories": [{"name": "compliance", "keywords": ["GDPR"]}],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            with open(config_path, "w") as f:
                json.dump(config_data, f)

            config = ConfigLoader.load(config_path)
            assert config.pipeline_version == "1.0"

    def test_load_config_file_not_found(self):
        """Test loading from non-existent file."""
        with pytest.raises(FileNotFoundError):
            ConfigLoader.load(Path("/nonexistent/config.json"))

    def test_load_config_invalid_json(self):
        """Test loading invalid JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            with open(config_path, "w") as f:
                f.write("invalid json {")

            with pytest.raises(json.JSONDecodeError):
                ConfigLoader.load(config_path)

    def test_load_config_invalid_data(self):
        """Test loading configuration with invalid data."""
        config_data = {
            "pipeline_version": "",  # Invalid: empty
            "web_sources": [{"url": "https://example.com", "categories": ["test"]}],
            "email_folders": [],
            "categories": [{"name": "test", "keywords": ["test"]}],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            with open(config_path, "w") as f:
                json.dump(config_data, f)

            with pytest.raises(ValueError):
                ConfigLoader.load(config_path)

    def test_load_yaml_config(self):
        """Test loading configuration from a .yaml file."""
        config_data = {
            "pipeline_version": "2.0",
            "web_sources": [{"url": "https://example.com", "categories": ["compliance"]}],
            "email_folders": [],
            "categories": [{"name": "compliance", "keywords": ["GDPR"]}],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(config_data, f, allow_unicode=True)

            config = ConfigLoader.load(config_path)
            assert config.pipeline_version == "2.0"
            assert len(config.web_sources) == 1
            assert config.get_category("compliance") is not None

    def test_load_yaml_two_file_pattern(self):
        """Test that sources.yaml is merged into config.yaml automatically."""
        config_data = {
            "pipeline_version": "2.0",
            "categories": [{"name": "compliance", "keywords": ["GDPR"]}],
        }
        sources_data = {
            "web_sources": [{"url": "https://example.com", "categories": ["compliance"]}],
            "email_folders": [],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            sources_path = Path(tmpdir) / "sources.yaml"
            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(config_data, f, allow_unicode=True)
            with open(sources_path, "w", encoding="utf-8") as f:
                yaml.dump(sources_data, f, allow_unicode=True)

            config = ConfigLoader.load(config_path)
            assert len(config.web_sources) == 1
            assert config.web_sources[0].url == "https://example.com"

    def test_load_yaml_two_file_pattern_no_override(self):
        """web_sources in config.yaml are NOT overwritten by sources.yaml."""
        config_data = {
            "pipeline_version": "2.0",
            "categories": [{"name": "compliance", "keywords": ["GDPR"]}],
            "web_sources": [{"url": "https://main.example.com", "categories": ["compliance"]}],
        }
        sources_data = {
            "web_sources": [{"url": "https://should-not-appear.example.com", "categories": ["compliance"]}],
            "email_folders": [],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            sources_path = Path(tmpdir) / "sources.yaml"
            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(config_data, f, allow_unicode=True)
            with open(sources_path, "w", encoding="utf-8") as f:
                yaml.dump(sources_data, f, allow_unicode=True)

            config = ConfigLoader.load(config_path)
            assert len(config.web_sources) == 1
            assert config.web_sources[0].url == "https://main.example.com"

    def test_load_yaml_invalid(self):
        """Test that invalid YAML raises a yaml.YAMLError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            with open(config_path, "w", encoding="utf-8") as f:
                f.write("key: [unclosed bracket\n  bad indent: :")

            with pytest.raises(yaml.YAMLError):
                ConfigLoader.load(config_path)


class TestWebSourceListings:
    """Test WebSource listings_type support."""

    def test_web_source_is_listings_page_default(self):
        """Test WebSource default listings_type is 'linked'."""
        source = WebSource(url="https://example.com/article/123", categories=[])
        assert source.listings_type == "linked"

    def test_web_source_inline_listings(self):
        """Test WebSource with inline articles."""
        source = WebSource(
            url="https://example.com/alerts",
            categories=[],
            listings_type="inline"
        )
        assert source.listings_type == "inline"

    def test_web_source_linked_listings(self):
        """Test WebSource with linked articles."""
        source = WebSource(
            url="https://example.com/news",
            categories=[],
            listings_type="linked"
        )
        assert source.listings_type == "linked"

    def test_web_source_listings_from_dict_inline(self):
        """Test WebSource from_dict with inline listings."""
        data = {
            "url": "https://example.com/listings",
            "listings_type": "inline"
        }
        source = WebSource.from_dict(data)
        assert source.listings_type == "inline"

    def test_web_source_listings_from_dict_linked(self):
        """Test WebSource from_dict with linked listings."""
        data = {
            "url": "https://example.com/listings",
            "listings_type": "linked"
        }
        source = WebSource.from_dict(data)
        assert source.listings_type == "linked"

    def test_web_source_listings_from_dict_no_fields(self):
        """Test WebSource from_dict without listings_type field (default)."""
        data = {
            "url": "https://example.com/article"
        }
        source = WebSource.from_dict(data)
        assert source.listings_type == "linked"

    def test_web_source_listings_to_dict_inline(self):
        """Test WebSource to_dict with inline listings."""
        source = WebSource(
            url="https://example.com/listings",
            categories=["test"],
            listings_type="inline"
        )
        d = source.to_dict()
        assert d["url"] == "https://example.com/listings"
        assert d["listings_type"] == "inline"

    def test_web_source_listings_to_dict_linked(self):
        """Test WebSource to_dict with linked listings (default not serialized)."""
        source = WebSource(
            url="https://example.com/news",
            categories=["test"],
            listings_type="linked"
        )
        d = source.to_dict()
        # Default value should not be included
        assert "listings_type" not in d

    def test_web_source_listings_to_dict_not_listings(self):
        """Test WebSource to_dict for default listings type."""
        source = WebSource(url="https://example.com/article", categories=["test"])
        d = source.to_dict()
        # listings_type="linked" (default) should not be included
        assert "listings_type" not in d

    def test_web_source_listings_to_dict_roundtrip(self):
        """Test WebSource to_dict/from_dict roundtrip for listings."""
        original = WebSource(
            url="https://example.com/listings",
            categories=["CCCI"],
            listings_type="inline"
        )
        d = original.to_dict()
        restored = WebSource.from_dict(d)
        assert restored.url == original.url
        assert restored.listings_type == original.listings_type
