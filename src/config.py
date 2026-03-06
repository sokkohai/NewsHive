"""Configuration management for newshive pipeline.

Implements configuration loading, validation, and access as specified in
specs/core/CONFIGURATION.md.

Supports both JSON (.json) and YAML (.yaml / .yml) configuration files.
When a YAML file is loaded, an optional companion sources.yaml in the same
directory is automatically merged in (provides web_sources / email_folders).
"""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast


class ConfigError(ValueError):
    """Raised when configuration validation fails."""

    pass


@dataclass
class Category:
    """Represents a category definition with name and keywords for content matching."""

    name: str
    keywords: list[str]
    relevance_schema: "RelevanceSchema | None" = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Category":
        """Create a Category from dictionary."""
        return cls(
            name=data["name"],
            keywords=data.get("keywords", []),
            relevance_schema=RelevanceSchema.from_dict(data.get("relevance_schema")),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert Category to dictionary."""
        result = {"name": self.name, "keywords": self.keywords}
        if self.relevance_schema is not None:
            result["relevance_schema"] = self.relevance_schema.to_dict()
        return result


@dataclass
class RelevanceDimensionDefinition:
    """Definition of one relevance scoring dimension in category schema."""

    label: str
    question: str
    scores: dict[str, str]
    scoring_rule: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "RelevanceDimensionDefinition | None":
        """Create RelevanceDimensionDefinition from dictionary."""
        if data is None:
            return None
        return cls(
            label=data.get("label", ""),
            question=data.get("question", ""),
            scores=data.get("scores", {}),
            scoring_rule=data.get("scoring_rule"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert RelevanceDimensionDefinition to dictionary."""
        result = {
            "label": self.label,
            "question": self.question,
            "scores": self.scores,
        }
        if self.scoring_rule:
            result["scoring_rule"] = self.scoring_rule
        return result


@dataclass
class RelevanceSchema:
    """Optional category-specific relevance scoring schema."""

    schema_id: str | None = None
    display_name: str | None = None
    system_message: str | None = None
    dimensions: dict[str, RelevanceDimensionDefinition] | None = None
    practice_areas: list[str] | None = None
    thresholds: dict[str, int] | None = None

    REQUIRED_DIMENSION_KEYS = {
        "d1_enforcement",
        "d2_organ",
        "d3_compliance",
        "d4_regulatory",
        "d5_mandate",
    }
    REQUIRED_SCORE_KEYS = {"0", "1", "2", "3"}

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "RelevanceSchema | None":
        """Create RelevanceSchema from dictionary."""
        if data is None:
            return None

        raw_dimensions = data.get("dimensions") or {}
        dimensions: dict[str, RelevanceDimensionDefinition] = {}
        if isinstance(raw_dimensions, dict):
            for key, value in raw_dimensions.items():
                if isinstance(value, dict):
                    definition = RelevanceDimensionDefinition.from_dict(value)
                    if definition is not None:
                        dimensions[key] = definition

        return cls(
            schema_id=data.get("schema_id"),
            display_name=data.get("display_name"),
            system_message=data.get("system_message"),
            dimensions=dimensions or None,
            practice_areas=data.get("practice_areas"),
            thresholds=data.get("thresholds"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert RelevanceSchema to dictionary."""
        result: dict[str, Any] = {}
        if self.schema_id:
            result["schema_id"] = self.schema_id
        if self.display_name:
            result["display_name"] = self.display_name
        if self.system_message:
            result["system_message"] = self.system_message
        if self.dimensions:
            result["dimensions"] = {
                key: value.to_dict() for key, value in self.dimensions.items()
            }
        if self.practice_areas:
            result["practice_areas"] = self.practice_areas
        if self.thresholds:
            result["thresholds"] = self.thresholds
        return result


@dataclass
class ExtractionRules:
    """Extraction rules for robust article discovery from listing pages.

    Provides optional hints to improve article discovery accuracy.
    """

    include_patterns: list[str] | None = None
    exclude_patterns: list[str] | None = None
    container_selector: str | None = None
    link_selector: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "ExtractionRules | None":
        """Create ExtractionRules from dictionary."""
        if data is None:
            return None
        # Create object even with empty dict (to allow setting defaults later)
        return cls(
            include_patterns=data.get("include_patterns"),
            exclude_patterns=data.get("exclude_patterns"),
            container_selector=data.get("container_selector"),
            link_selector=data.get("link_selector"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert ExtractionRules to dictionary."""
        result: dict[str, Any] = {}
        if self.include_patterns:
            result["include_patterns"] = self.include_patterns
        if self.exclude_patterns:
            result["exclude_patterns"] = self.exclude_patterns
        if self.container_selector:
            result["container_selector"] = self.container_selector
        if self.link_selector:
            result["link_selector"] = self.link_selector
        return result


@dataclass
class DateExtractionPattern:
    """Date extraction pattern configuration for listing pages.
    
    Provides fine-grained control over date extraction from listing pages,
    especially useful for JavaScript-rendered sites or non-standard formats.
    """
    
    css_selectors: list[str] | None = None
    regex_patterns: list[str] | None = None
    date_format: str | None = None
    
    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "DateExtractionPattern | None":
        """Create DateExtractionPattern from dictionary."""
        if data is None:
            return None
        return cls(
            css_selectors=data.get("css_selectors"),
            regex_patterns=data.get("regex_patterns"),
            date_format=data.get("date_format"),
        )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert DateExtractionPattern to dictionary."""
        result: dict[str, Any] = {}
        if self.css_selectors:
            result["css_selectors"] = self.css_selectors
        if self.regex_patterns:
            result["regex_patterns"] = self.regex_patterns
        if self.date_format:
            result["date_format"] = self.date_format
        return result


@dataclass
class WebSource:
    """Represents a web source with category assignment and listings support.

    A web source must have assigned categories. Article content is validated
    against category keywords during the Categorization stage.
    
    Fields:
    - categories: list of category names for this source (required)
    - listings_type: "inline" (articles embedded in page HTML) or "linked" (articles on separate URLs, default)
    - discovery_method: Method for discovering articles on the listing page.
      Supported values: "auto" (default), "rss", "static", "browser", "playwright", "sitemap"
      - "auto": tries RSS → static → browser (default fallback chain)
      - "rss": only use RSS/Atom feed discovery
      - "static": only use HTTP + BeautifulSoup (no JS rendering)
      - "browser" / "playwright": use headless browser (Playwright) for JS-rendered pages
      - "sitemap": only use XML sitemap discovery (via trafilatura)
    - sitemap_url: Optional explicit sitemap URL. If omitted with method=sitemap,
      trafilatura auto-discovers from robots.txt and common paths.
    - fetch_method: Method for fetching individual article content.
      Supported values: "auto" (default), "static", "browser", "playwright"
      - "auto": tries static → browser (default fallback chain)
      - "static": only use HTTP requests (no JS rendering)
      - "browser" / "playwright": use headless browser (Playwright/Selenium)
    """

    VALID_DISCOVERY_METHODS = {"auto", "rss", "static", "browser", "playwright", "sitemap"}
    VALID_FETCH_METHODS = {"auto", "static", "browser", "playwright"}
    VALID_RSS_DATE_EXTRACTION = {"feed_fields", "url_pattern", "both"}

    url: str
    categories: list[str]
    extraction_rules: ExtractionRules | None = None
    date_extraction_pattern: DateExtractionPattern | None = None
    listings_type: str = "linked"  # "inline" or "linked"
    rss_feed_url: str | None = None
    sitemap_url: str | None = None
    discovery_method: str = "auto"
    fetch_method: str = "auto"
    rss_date_extraction: str = "both"  # "feed_fields", "url_pattern", or "both"
    browser_actions: list[dict[str, Any]] | None = None
    item_selector: str | None = None  # Selector for finding item containers (optional)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WebSource":
        """Create a WebSource from dictionary."""
        if isinstance(data, str):
            raise ConfigError(
                f"Web source must be an object with 'url' and 'categories', got string: {data}"
            )
        extraction_rules = ExtractionRules.from_dict(
            data.get("extraction_rules")
        )
        date_extraction_pattern = DateExtractionPattern.from_dict(
            data.get("date_extraction_pattern")
        )
        # Support only 'categories' field
        categories = data.get("categories") or []
        rss_date_extraction = data.get("rss_date_extraction", "both")
        if rss_date_extraction not in cls.VALID_RSS_DATE_EXTRACTION:
            raise ConfigError(
                f"Invalid rss_date_extraction '{rss_date_extraction}'. "
                f"Must be one of: {', '.join(cls.VALID_RSS_DATE_EXTRACTION)}"
            )
        
        return cls(
            url=data.get("url", ""),
            categories=categories,
            extraction_rules=extraction_rules,
            date_extraction_pattern=date_extraction_pattern,
            listings_type=data.get("listings_type", "linked"),
            rss_feed_url=data.get("rss_feed_url"),
            sitemap_url=data.get("sitemap_url"),
            discovery_method=data.get("discovery_method", "auto"),
            fetch_method=data.get("fetch_method", "auto"),
            rss_date_extraction=rss_date_extraction,
            browser_actions=data.get("browser_actions"),
            item_selector=data.get("item_selector"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert WebSource to dictionary."""
        result: dict[str, Any] = {"url": self.url}
        if self.categories:
            result["categories"] = self.categories
        if self.extraction_rules is not None:
            rules_dict = self.extraction_rules.to_dict()
            if rules_dict:
                result["extraction_rules"] = rules_dict
        if self.date_extraction_pattern is not None:
            pattern_dict = self.date_extraction_pattern.to_dict()
            if pattern_dict:
                result["date_extraction_pattern"] = pattern_dict
        if self.listings_type != "linked":  # Only include if not default
            result["listings_type"] = self.listings_type
        if self.rss_feed_url:
            result["rss_feed_url"] = self.rss_feed_url
        if self.sitemap_url:
            result["sitemap_url"] = self.sitemap_url
        if self.discovery_method != "auto":
            result["discovery_method"] = self.discovery_method
        if self.fetch_method != "auto":
            result["fetch_method"] = self.fetch_method
        if self.rss_date_extraction != "both":
            result["rss_date_extraction"] = self.rss_date_extraction
        if self.browser_actions:
            result["browser_actions"] = self.browser_actions
        if self.item_selector:
            result["item_selector"] = self.item_selector
        return result


@dataclass
class EmailFolder:
    """Represents an email folder configuration with optional archive settings.

    Per specs/core/EMAIL_ARCHIVAL.md, email folders can optionally specify
    an archive folder for processed emails.
    """

    folder_path: str
    archive_folder: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EmailFolder":
        """Create an EmailFolder from dictionary.

        Args:
            data: Dictionary with folder_path and optional archive_folder

        Returns:
            EmailFolder instance
        """
        if isinstance(data, str):
             raise ConfigError(f"EmailFolder must be a dictionary, got string: {data}")

        # New format: dict with folder_path and optional archive_folder
        return cls(
            folder_path=data.get("folder_path", ""),
            archive_folder=data.get("archive_folder"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert EmailFolder to dictionary."""
        result: dict[str, Any] = {"folder_path": self.folder_path}
        if self.archive_folder is not None:
            result["archive_folder"] = self.archive_folder
        return result


@dataclass
class QualityVerification:
    """Quality verification configuration.
    
    Implements Quality Verification spec from specs/core/QUALITY_VERIFICATION.md.
    
    Uses the same LLM provider, model, and credentials as Summarization stage
    (configured via LLM_PROVIDER, LLM_MODEL, LLM_API_KEY environment variables).
    """
    
    enabled: bool = True
    validate_title: bool = True
    validate_summary: bool = False
    
    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "QualityVerification":
        """Create QualityVerification from dictionary."""
        if data is None:
            return cls()  # defaults: enabled=True, validate_title=True, validate_summary=False
        return cls(
            enabled=data.get("enabled", True),
            validate_title=data.get("validate_title", True),
            validate_summary=data.get("validate_summary", False),
        )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "enabled": self.enabled,
            "validate_title": self.validate_title,
            "validate_summary": self.validate_summary,
        }


@dataclass
class ArticlePreparationConfig:
    """Configuration for Stage 2.5 Article Text Preparation."""

    enabled: bool = True
    min_prepared_chars: int = 600
    min_article_ratio: float = 0.25
    max_repeated_line_ratio: float = 0.30
    warn_margin: float = 0.10

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "ArticlePreparationConfig":
        """Create ArticlePreparationConfig from dictionary."""
        if data is None:
            return cls()
        return cls(
            enabled=data.get("enabled", True),
            min_prepared_chars=data.get("min_prepared_chars", 600),
            min_article_ratio=data.get("min_article_ratio", 0.25),
            max_repeated_line_ratio=data.get("max_repeated_line_ratio", 0.30),
            warn_margin=data.get("warn_margin", 0.10),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "enabled": self.enabled,
            "min_prepared_chars": self.min_prepared_chars,
            "min_article_ratio": self.min_article_ratio,
            "max_repeated_line_ratio": self.max_repeated_line_ratio,
            "warn_margin": self.warn_margin,
        }


@dataclass
class Configuration:
    """Configuration for the newshive pipeline.

    Implements the Configuration schema from specs/core/CONFIGURATION.md,
    specs/core/EMAIL_ARCHIVAL.md, specs/core/SERVICE_TOGGLES.md,
    specs/core/QUALITY_VERIFICATION.md, and Phase 3 Listings support.
    """

    pipeline_version: str
    web_sources: list[WebSource]
    email_folders: list[EmailFolder]
    categories: list[Category]
    webhook_url: str | None = None
    quality_verification: QualityVerification | None = None
    listings_enabled: bool = True  # Phase 3: Enable listings extraction by default
    article_max_age_days: int = 3  # Maximum age of articles in days (default: 3 days)
    keyword_filter_enabled: bool = True  # Enable keyword filtering in Discovery stage
    relevance_scoring_enabled: bool = True  # Enable 5-dimension relevance scoring (RELEVANCE_SCORING.md)
    relevance_scoring_bonus_rule_enabled: bool = False  # Enable Enforcement×Organ bonus (+2 if D1≥2 AND D2≥2)
    article_text_preparation: ArticlePreparationConfig | None = None

    def _validate_relevance_schema(
        self,
        category_name: str,
        schema: RelevanceSchema,
        errors: list[str],
    ) -> None:
        """Validate category-level relevance schema shape and required fields."""
        if schema.dimensions is None:
            errors.append(
                f"categories[{category_name}].relevance_schema.dimensions must be present"
            )
            return

        dimension_keys = set(schema.dimensions.keys())
        missing_dimensions = RelevanceSchema.REQUIRED_DIMENSION_KEYS - dimension_keys
        if missing_dimensions:
            errors.append(
                f"categories[{category_name}].relevance_schema missing dimensions: {sorted(missing_dimensions)}"
            )

        unknown_dimensions = dimension_keys - RelevanceSchema.REQUIRED_DIMENSION_KEYS
        if unknown_dimensions:
            errors.append(
                f"categories[{category_name}].relevance_schema has unknown dimensions: {sorted(unknown_dimensions)}"
            )

        for dim_key, definition in schema.dimensions.items():
            if not definition.label or not definition.label.strip():
                errors.append(
                    f"categories[{category_name}].relevance_schema.dimensions[{dim_key}].label must be non-empty"
                )
            if not definition.question or not definition.question.strip():
                errors.append(
                    f"categories[{category_name}].relevance_schema.dimensions[{dim_key}].question must be non-empty"
                )

            score_keys = set(definition.scores.keys())
            missing_scores = RelevanceSchema.REQUIRED_SCORE_KEYS - score_keys
            if missing_scores:
                errors.append(
                    f"categories[{category_name}].relevance_schema.dimensions[{dim_key}] missing scores: {sorted(missing_scores)}"
                )

            unknown_scores = score_keys - RelevanceSchema.REQUIRED_SCORE_KEYS
            if unknown_scores:
                errors.append(
                    f"categories[{category_name}].relevance_schema.dimensions[{dim_key}] has unknown score keys: {sorted(unknown_scores)}"
                )

            for score_key, score_text in definition.scores.items():
                if not isinstance(score_text, str) or not score_text.strip():
                    errors.append(
                        f"categories[{category_name}].relevance_schema.dimensions[{dim_key}].scores[{score_key}] must be non-empty string"
                    )

    def get_category(self, name: str) -> Category | None:
        """Get a category by name."""
        for cat in self.categories:
            if cat.name == name:
                return cat
        return None

    def get_category_keywords(self, category_name: str) -> list[str]:
        """Get keywords for a category."""
        cat = self.get_category(category_name)
        return cat.keywords if cat else []

    def is_keyword_filtering_enabled(self) -> bool:
        """Check if keyword filtering is enabled in Discovery stage.
        
        Per DISCOVERY.md spec:
        - Enabled when: keyword_filter_enabled is true AND keywords array is non-empty
        - Disabled when: keyword_filter_enabled is false OR keywords array is empty
        """
        all_keywords: set[str] = set()
        for cat in self.categories:
            all_keywords.update(cat.keywords)
        
        return self.keyword_filter_enabled and len(all_keywords) > 0

    def __post_init__(self) -> None:
        """Post-init validation."""
        # No implicit conversion - types must be correct
        pass

    def validate(self) -> None:
        """Validate configuration against specification requirements."""
        errors: list[str] = []
        relevance_schema_ids: set[str] = set()

        # Validate pipeline_version
        if not self.pipeline_version or not isinstance(
            self.pipeline_version, str
        ):
            errors.append("pipeline_version must be a non-empty string")

        # Validate that at least one source is configured
        if not self.web_sources and not self.email_folders:
            errors.append(
                "At least one of web_sources or email_folders must be "
                "non-empty"
            )

        # Validate categories
        if not isinstance(self.categories, list):
            errors.append("categories must be a list")
        elif not self.categories:
            errors.append("categories must not be empty - at least one category is required")
        else:
            category_names: set[str] = set()
            for i, cat in enumerate(self.categories):
                if not isinstance(cat, Category):
                    errors.append(f"Expected Category object, got {type(cat)}")
                    continue
                if not cat.name:
                    errors.append(f"categories[{i}].name must be non-empty")
                if cat.name in category_names:
                    errors.append(f"Duplicate category name: {cat.name}")
                category_names.add(cat.name)
                # Keywords can be empty (email-only configs or legacy keyword filtering)

                if cat.relevance_schema is not None:
                    schema_id = cat.relevance_schema.schema_id
                    if schema_id:
                        if schema_id in relevance_schema_ids:
                            errors.append(f"Duplicate relevance schema_id: {schema_id}")
                        relevance_schema_ids.add(schema_id)
                    self._validate_relevance_schema(cat.name, cat.relevance_schema, errors)

        # Validate web sources
        if not isinstance(self.web_sources, list):
            errors.append("web_sources must be a list")
        else:
            valid_category_names = {c.name for c in self.categories}
            for i, source in enumerate(self.web_sources):
                if not isinstance(source, WebSource):
                    errors.append(
                        f"web_sources[{i}] must be a WebSource object"
                    )
                    continue
                if not source.url:
                    errors.append(f"web_sources[{i}].url must be non-empty")
                # Validate that categories are provided
                if not source.categories and self.categories:
                    errors.append(
                        f"web_sources[{i}] ({source.url}): categories must be non-empty"
                    )
                # Validate that referenced categories exist
                for category_name in source.categories:
                    if category_name not in valid_category_names:
                        errors.append(
                            f"web_sources[{i}]: category '{category_name}' "
                            "not found in categories list"
                        )
                # Validate discovery_method
                if source.discovery_method not in WebSource.VALID_DISCOVERY_METHODS:
                    errors.append(
                        f"web_sources[{i}] ({source.url}): invalid discovery_method "
                        f"'{source.discovery_method}', must be one of {sorted(WebSource.VALID_DISCOVERY_METHODS)}"
                    )
                # Validate fetch_method
                if source.fetch_method not in WebSource.VALID_FETCH_METHODS:
                    errors.append(
                        f"web_sources[{i}] ({source.url}): invalid fetch_method "
                        f"'{source.fetch_method}', must be one of {sorted(WebSource.VALID_FETCH_METHODS)}"
                    )

        # Validate email folders
        if not isinstance(self.email_folders, list):
            errors.append("email_folders must be a list")
        else:
            for i, folder in enumerate(self.email_folders):
                if not isinstance(folder, EmailFolder):
                    errors.append(
                        f"email_folders[{i}] must be an EmailFolder object"
                    )
                    continue
                if not folder.folder_path:
                    errors.append(f"email_folders[{i}].folder_path must be non-empty")
        
        # Phase 3: Validate listings configuration
        if not isinstance(self.listings_enabled, bool):
            errors.append(
                f"listings_enabled must be a boolean, "
                f"got: {type(self.listings_enabled).__name__}"
            )
        
        # Validate article_max_age_days
        if not isinstance(self.article_max_age_days, int):
            errors.append(
                f"article_max_age_days must be a positive integer, "
                f"got: {type(self.article_max_age_days).__name__}"
            )
        elif self.article_max_age_days < 1:
            errors.append(
                f"article_max_age_days must be >= 1, "
                f"got: {self.article_max_age_days}"
            )
        
        # Validate keyword_filter_enabled
        if not isinstance(self.keyword_filter_enabled, bool):
            errors.append(
                f"keyword_filter_enabled must be a boolean, "
                f"got: {type(self.keyword_filter_enabled).__name__}"
            )
        
        if errors:
            raise ConfigError(
                "Configuration validation failed:\n" + "\n".join(
                    f"  - {e}" for e in errors
                )
            )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Configuration":
        """Create a Configuration from dictionary (e.g., from JSON)."""
        # Support only 'categories' field
        categories_data = data.get("categories") or []
        if not categories_data and data.get("keywords"):
            categories_data = [
                {"name": "general", "keywords": data.get("keywords")}
            ]
        categories = [
            Category.from_dict(c) if isinstance(c, dict) else Category(name=str(c), keywords=[])
            for c in categories_data
        ]

        # Handle web_sources
        web_sources_data = data.get("web_sources", [])
        web_sources = [
            WebSource.from_dict(ws) for ws in web_sources_data
        ]
        if categories:
            default_categories = [category.name for category in categories]
            for source in web_sources:
                if not source.categories:
                    source.categories = default_categories.copy()

        # Handle email_folders: support both old format (list of strings)
        # and new format (list of dicts with folder_path and optional archive_folder)
        email_folders_data = data.get("email_folders", [])
        email_folders = [
            EmailFolder.from_dict(ef) for ef in email_folders_data
        ]

        quality_verification = QualityVerification.from_dict(
            data.get("quality_verification")
        )

        config = cls(
            pipeline_version=data.get("pipeline_version", ""),
            web_sources=web_sources,
            email_folders=email_folders,
            categories=categories,
            webhook_url=os.getenv("WEBHOOK_URL") or data.get("webhook_url"),
            quality_verification=quality_verification,
            # Phase 3: Listings configuration
            listings_enabled=data.get("listings_enabled", True),
            article_max_age_days=data.get("article_max_age_days", 3),
            keyword_filter_enabled=data.get("keyword_filter_enabled", True),
            relevance_scoring_enabled=data.get("relevance_scoring_enabled", True),
            relevance_scoring_bonus_rule_enabled=data.get("relevance_scoring_bonus_rule_enabled", False),
            article_text_preparation=ArticlePreparationConfig.from_dict(
                data.get("article_text_preparation")
            ),
        )

        config.validate()
        return config

    def to_dict(self) -> dict[str, Any]:
        """Convert Configuration to dictionary."""
        result: dict[str, Any] = {
            "pipeline_version": self.pipeline_version,
            "web_sources": [ws.to_dict() for ws in self.web_sources],
            "email_folders": [ef.to_dict() for ef in self.email_folders],
            "categories": [c.to_dict() for c in self.categories],
        }
        if self.webhook_url:
            result["webhook_url"] = self.webhook_url
        if self.quality_verification and self.quality_verification.enabled:
            result["quality_verification"] = self.quality_verification.to_dict()
        if self.article_text_preparation is not None:
             result["article_text_preparation"] = self.article_text_preparation.to_dict()
        return result


class ConfigLoader:
    """Loads and manages pipeline configuration from file and environment.

    Implements configuration loading as specified in
    specs/core/CONFIGURATION.md.

    File format support:
        - JSON  (.json)  - original format, still fully supported
        - YAML  (.yaml / .yml)  - human-readable alternative with comment support

    Two-file pattern (opt-in):
        Place user-facing settings in config.yaml and technical scraping
        settings in sources.yaml next to it.  ConfigLoader merges the two
        automatically when sources.yaml exists and config.yaml does not
        already contain web_sources / email_folders.
    """

    DEFAULT_CONFIG_PATH = Path("./config.yaml")
    _YAML_CANDIDATES = (Path("./config.yaml"), Path("./config.yml"))

    @staticmethod
    def _load_file(path: Path) -> dict[str, Any]:
        """Read a JSON or YAML file and return the parsed dictionary."""
        with open(path, encoding="utf-8") as f:
            if path.suffix in (".yaml", ".yml"):
                import yaml  # pyyaml – always available (listed in dependencies)
                result = yaml.safe_load(f)
                if result is None:
                    return {}
                if not isinstance(result, dict):
                    raise ValueError(f"{path}: expected a YAML mapping at top level")
                return result
            return json.load(f)  # type: ignore[no-any-return]

    @staticmethod
    def load(config_path: Path | None = None) -> Configuration:
        """Load configuration from file.

        Auto-detection order (when config_path is None):
            1. config.yaml  in current directory
            2. config.yml   in current directory
            3. config.json  in current directory (legacy default)

        Two-file merge:
            If the resolved config file is a YAML file and a sources.yaml
            exists in the same directory, web_sources and email_folders are
            loaded from sources.yaml (unless they are already present in the
            main config file).

        Args:
            config_path: Explicit path to configuration file (.json or .yaml).
                         Pass None to use auto-detection.

        Returns:
            Validated Configuration object.

        Raises:
            FileNotFoundError: If configuration file doesn't exist.
            ValueError: If configuration is semantically invalid.
            json.JSONDecodeError: If a .json file contains invalid JSON.
            yaml.YAMLError: If a .yaml file contains invalid YAML.
        """
        if config_path is None:
            # Auto-detect: prefer YAML, fall back to JSON
            for candidate in ConfigLoader._YAML_CANDIDATES:
                if candidate.exists():
                    config_path = candidate
                    break
            if config_path is None:
                config_path = ConfigLoader.DEFAULT_CONFIG_PATH

        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found at {config_path}. "
                "Please create config.yaml (or config.json) in the "
                "pipeline working directory."
            )

        data = ConfigLoader._load_file(config_path)

        # Two-file pattern: merge sources.yaml alongside a YAML main config
        if config_path.suffix in (".yaml", ".yml"):
            sources_path = config_path.parent / "sources.yaml"
            if sources_path.exists() and sources_path != config_path:
                sources_data = ConfigLoader._load_file(sources_path)
                if "web_sources" not in data and "web_sources" in sources_data:
                    data["web_sources"] = sources_data["web_sources"]
                if "email_folders" not in data and "email_folders" in sources_data:
                    data["email_folders"] = sources_data["email_folders"]

        return Configuration.from_dict(data)

    @staticmethod
    def get_llm_config() -> tuple[str, str, str]:
        """Get LLM configuration from environment variables.

        Returns:
            Tuple of (provider, model, api_key)

        Raises:
            ValueError: If any required LLM environment variable is missing
        """
        provider = os.getenv("LLM_PROVIDER")
        model = os.getenv("LLM_MODEL")
        api_key = os.getenv("LLM_API_KEY")

        errors = []
        if not provider:
            errors.append("LLM_PROVIDER environment variable not set")
        if not model:
            errors.append("LLM_MODEL environment variable not set")
        if not api_key:
            errors.append("LLM_API_KEY environment variable not set")

        if errors:
            raise ValueError(
                "LLM configuration incomplete:\n" + "\n".join(
                    f"  - {e}" for e in errors
                )
            )

        return cast(str, provider), cast(str, model), cast(str, api_key)

    @staticmethod
    def get_azure_config() -> tuple[str, str, str, str | None]:
        """Get Azure credentials from environment variables.

        Returns:
            Tuple of (client_id, client_secret, tenant_id, refresh_token)

        Raises:
            ValueError: If any required Azure environment variable is missing
        """
        client_id = os.getenv("AZURE_CLIENT_ID") or os.getenv("OUTLOOK_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET") or os.getenv("OUTLOOK_CLIENT_SECRET")
        tenant_id = os.getenv("AZURE_TENANT_ID") or os.getenv("OUTLOOK_TENANT_ID")
        refresh_token = os.getenv("AZURE_REFRESH_TOKEN")

        errors = []
        if not client_id:
            errors.append(
                "AZURE_CLIENT_ID or OUTLOOK_CLIENT_ID "
                "environment variable not set"
            )
        if not client_secret:
            errors.append(
                "AZURE_CLIENT_SECRET or OUTLOOK_CLIENT_SECRET "
                "environment variable not set"
            )
        if not tenant_id:
            errors.append(
                "AZURE_TENANT_ID or OUTLOOK_TENANT_ID "
                "environment variable not set"
            )

        if errors:
            error_msg = (
                "Azure/Outlook configuration incomplete "
                "(required for email sources):\n"
                + "\n".join(f"  - {e}" for e in errors)
            )
            raise ValueError(error_msg)

        return (
            cast(str, client_id),
            cast(str, client_secret),
            cast(str, tenant_id),
            refresh_token,
        )
