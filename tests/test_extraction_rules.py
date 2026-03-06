"""Tests for extraction rules functionality.

Tests for specs/core/LISTING_ANALYSIS_STRATEGY.md implementation.
"""

from src.config import ExtractionRules, WebSource
from src.discovery import WebDiscoverer


class TestExtractionRules:
    """Tests for ExtractionRules dataclass."""

    def test_extraction_rules_creation(self) -> None:
        """Test creating ExtractionRules."""
        rules = ExtractionRules(
            include_patterns=["/news/", "/blog/"],
            exclude_patterns=["/category/", "/tags/"],
            container_selector=".news-list",
            link_selector="h3 > a",
        )

        assert rules.include_patterns == ["/news/", "/blog/"]
        assert rules.exclude_patterns == ["/category/", "/tags/"]
        assert rules.container_selector == ".news-list"
        assert rules.link_selector == "h3 > a"

    def test_extraction_rules_from_dict(self) -> None:
        """Test creating ExtractionRules from dictionary."""
        data = {
            "include_patterns": ["/articles/"],
            "exclude_patterns": ["/tag/"],
            "container_selector": ".content",
            "link_selector": "a.article-link",
        }
        rules = ExtractionRules.from_dict(data)

        assert rules is not None
        assert rules.include_patterns == ["/articles/"]
        assert rules.exclude_patterns == ["/tag/"]
        assert rules.container_selector == ".content"
        assert rules.link_selector == "a.article-link"

    def test_extraction_rules_from_empty_dict(self) -> None:
        """Test creating ExtractionRules from empty dictionary."""
        rules = ExtractionRules.from_dict({})
        # Empty dict should return an ExtractionRules object (not None)
        assert rules is not None
        assert rules.include_patterns is None
        assert rules.exclude_patterns is None

    def test_extraction_rules_from_none(self) -> None:
        """Test creating ExtractionRules from None."""
        rules = ExtractionRules.from_dict(None)
        assert rules is None

    def test_extraction_rules_to_dict(self) -> None:
        """Test converting ExtractionRules to dictionary."""
        rules = ExtractionRules(
            include_patterns=["/news/"],
            exclude_patterns=["/category/"],
        )
        data = rules.to_dict()

        assert data["include_patterns"] == ["/news/"]
        assert data["exclude_patterns"] == ["/category/"]
        assert "container_selector" not in data
        assert "link_selector" not in data

    def test_extraction_rules_to_dict_empty(self) -> None:
        """Test converting empty ExtractionRules to dictionary."""
        rules = ExtractionRules()
        data = rules.to_dict()
        assert data == {}


class TestWebSourceWithExtractionRules:
    """Tests for WebSource with extraction rules."""

    def test_web_source_with_extraction_rules(self) -> None:
        """Test creating WebSource with extraction rules."""
        rules = ExtractionRules(include_patterns=["/news/"])
        source = WebSource(
            url="https://example.com/news",
            categories=["news"],
            extraction_rules=rules,
        )

        assert source.url == "https://example.com/news"
        assert source.categories == ["news"]
        assert source.extraction_rules == rules

    def test_web_source_from_dict_with_rules(self) -> None:
        """Test creating WebSource from dict with extraction rules."""
        data = {
            "url": "https://example.com",
            "categories": ["tech"],
            "extraction_rules": {
                "include_patterns": ["/articles/"],
                "exclude_patterns": ["/tag/"],
            },
        }
        source = WebSource.from_dict(data)

        assert source.url == "https://example.com"
        assert source.extraction_rules is not None
        assert source.extraction_rules.include_patterns == ["/articles/"]
        assert source.extraction_rules.exclude_patterns == ["/tag/"]

    def test_web_source_to_dict_with_rules(self) -> None:
        """Test converting WebSource to dict with extraction rules."""
        rules = ExtractionRules(include_patterns=["/news/"])
        source = WebSource(
            url="https://example.com",
            categories=["tech"],
            extraction_rules=rules,
        )
        data = source.to_dict()

        assert data["url"] == "https://example.com"
        assert data["categories"] == ["tech"]
        assert data["extraction_rules"]["include_patterns"] == ["/news/"]


class TestApplyExtractionRules:
    """Tests for applying extraction rules to article lists."""

    def test_apply_include_patterns(self) -> None:
        """Test filtering with include patterns."""
        discoverer = WebDiscoverer()
        articles = [
            ("https://example.com/news/article1", "Article 1", "unknown"),
            ("https://example.com/blog/post1", "Post 1", "unknown"),
            ("https://example.com/about", "About", "unknown"),
        ]

        filtered = discoverer._apply_extraction_rules(
            articles,
            include_patterns=["/news/"],
        )

        assert len(filtered) == 1
        assert filtered[0][0] == "https://example.com/news/article1"

    def test_apply_exclude_patterns(self) -> None:
        """Test filtering with exclude patterns."""
        discoverer = WebDiscoverer()
        articles = [
            ("https://example.com/article1", "Article 1", "unknown"),
            ("https://example.com/article2", "Article 2", "unknown"),
            ("https://example.com/category/tech", "Category", "unknown"),
        ]

        filtered = discoverer._apply_extraction_rules(
            articles,
            exclude_patterns=["/category/"],
        )

        assert len(filtered) == 2
        assert all("/category/" not in url for url, _, _ in filtered)

    def test_apply_both_patterns(self) -> None:
        """Test filtering with both include and exclude patterns."""
        discoverer = WebDiscoverer()
        articles = [
            ("https://example.com/news/article1", "Article 1", "unknown"),
            ("https://example.com/news/category/tech", "Category", "unknown"),
            ("https://example.com/blog/post1", "Post 1", "unknown"),
        ]

        filtered = discoverer._apply_extraction_rules(
            articles,
            include_patterns=["/news/"],
            exclude_patterns=["/category/"],
        )

        # After including /news/, only articles with /news/ remain
        # Then exclude /category/, leaving only article1
        assert len(filtered) == 1
        assert filtered[0][0] == "https://example.com/news/article1"

    def test_apply_no_patterns(self) -> None:
        """Test with no patterns (should return all articles)."""
        discoverer = WebDiscoverer()
        articles = [
            ("https://example.com/article1", "Article 1", "unknown"),
            ("https://example.com/article2", "Article 2", "unknown"),
        ]

        filtered = discoverer._apply_extraction_rules(articles)

        assert len(filtered) == 2

    def test_apply_patterns_multiple_matches(self) -> None:
        """Test include pattern with multiple matching articles."""
        discoverer = WebDiscoverer()
        articles = [
            ("https://example.com/news/article1", "Article 1", "unknown"),
            ("https://example.com/news/article2", "Article 2", "unknown"),
            ("https://example.com/news/article3", "Article 3", "unknown"),
            ("https://example.com/blog/post1", "Post 1", "unknown"),
        ]

        filtered = discoverer._apply_extraction_rules(
            articles,
            include_patterns=["/news/"],
        )

        assert len(filtered) == 3
        assert all("/news/" in url for url, _, _ in filtered)

    def test_apply_patterns_exclude_multiple(self) -> None:
        """Test exclude patterns with multiple patterns."""
        discoverer = WebDiscoverer()
        articles = [
            ("https://example.com/article1", "Article 1", "unknown"),
            ("https://example.com/category/tech", "Category", "unknown"),
            ("https://example.com/tag/python", "Tag", "unknown"),
        ]

        filtered = discoverer._apply_extraction_rules(
            articles,
            exclude_patterns=["/category/", "/tag/"],
        )

        assert len(filtered) == 1
        assert filtered[0][0] == "https://example.com/article1"

    def test_apply_patterns_empty_result(self) -> None:
        """Test when filtering results in empty list."""
        discoverer = WebDiscoverer()
        articles = [
            ("https://example.com/category/tech", "Category", "unknown"),
            ("https://example.com/tag/python", "Tag", "unknown"),
        ]

        filtered = discoverer._apply_extraction_rules(
            articles,
            exclude_patterns=["/category/", "/tag/"],
        )

        assert len(filtered) == 0

    def test_apply_patterns_case_sensitive(self) -> None:
        """Test that pattern matching is case-sensitive (as implemented)."""
        discoverer = WebDiscoverer()
        articles = [
            ("https://example.com/NEWS/article1", "Article 1", "unknown"),
            ("https://example.com/news/article2", "Article 2", "unknown"),
        ]

        filtered = discoverer._apply_extraction_rules(
            articles,
            include_patterns=["/news/"],
        )

        # Only the lowercase version matches
        assert len(filtered) == 1
        assert filtered[0][0] == "https://example.com/news/article2"
