"""Utilities for loading and formatting few-shot examples from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class FewShotExample:
    """Compact representation of one prompt example."""

    id: str
    section: str
    title: str
    article_text: str
    expected_score: int
    expected_level: str
    reason: str
    feedback_type: str = "reference"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FewShotExample | None":
        required = {
            "id",
            "section",
            "title",
            "article_text",
            "expected_score",
            "expected_level",
            "reason",
        }
        if not required.issubset(data.keys()):
            return None

        try:
            score = int(data["expected_score"])
        except (TypeError, ValueError):
            return None

        return cls(
            id=str(data["id"]),
            section=str(data["section"]),
            title=str(data["title"]),
            article_text=str(data["article_text"]),
            expected_score=score,
            expected_level=str(data["expected_level"]),
            reason=str(data["reason"]),
            feedback_type=str(data.get("feedback_type", "reference")),
        )


class FewShotExampleStore:
    """Load examples.yaml and render prompt-ready blocks."""

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> list[FewShotExample]:
        """Load examples from article_llm_examples.yaml. Returns [] if missing/invalid."""
        if not self.path.exists():
            return []

        import yaml  # type: ignore[import-untyped]

        with open(self.path, encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}

        raw_examples = payload.get("examples", [])
        if not isinstance(raw_examples, list):
            return []

        parsed: list[FewShotExample] = []
        for entry in raw_examples:
            if not isinstance(entry, dict):
                continue
            parsed_example = FewShotExample.from_dict(entry)
            if parsed_example is not None:
                parsed.append(parsed_example)

        return parsed

    @staticmethod
    def render_for_prompt(examples: list[FewShotExample], max_examples: int = 15) -> str:
        """Render a compact text block to prepend to an LLM prompt."""
        if not examples:
            return ""

        blocks: list[str] = []
        for index, example in enumerate(examples[:max_examples], start=1):
            blocks.append(
                "\n".join(
                    [
                        f"Beispiel {index}:",
                        f"Sektion: {example.section}",
                        f"Titel: {example.title}",
                        f"Text: {example.article_text}",
                        f"Score: {example.expected_score}",
                        f"Level: {example.expected_level}",
                        f"Begruendung: {example.reason}",
                    ]
                )
            )

        return "\n\n".join(blocks)