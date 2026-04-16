"""LLM-based deduplication stage for post-enrichment similarity detection."""

import json
import logging
from typing import Any

import requests

from .config import Configuration
from .models import ContentItem

logger = logging.getLogger(__name__)


class LLMDeduplicator:
    """LLM-based deduplication of articles after relevance scoring."""

    def __init__(
        self,
        config: Configuration,
        llm_provider: str,
        llm_model: str,
        llm_api_key: str,
        llm_api_url: str | None = None,
    ):
        self.config = config
        self.provider = llm_provider
        self.model = llm_model
        self.api_key = llm_api_key
        self.api_url = llm_api_url

    def deduplicate(self, items: list[ContentItem]) -> list[ContentItem]:
        """Deduplicate items using LLM-based similarity detection."""
        if not items or len(items) < 2:
            return items

        dedup_cfg = self.config.llm_deduplication
        if not dedup_cfg or not getattr(dedup_cfg, "enabled", True):
            logger.debug("LLM deduplication disabled in config")
            return items

        logger.info("Starting LLM-based deduplication for %d articles", len(items))

        batch_size = dedup_cfg.batch_size or 0
        verbose = dedup_cfg.verbose_logging

        try:
            if batch_size and batch_size > 0:
                similar_groups = self._deduplicate_batched(items, batch_size, verbose)
            else:
                similar_groups = self._get_similar_groups(items, verbose)

            deduplicated = self._filter_duplicates(items, similar_groups, verbose)
            logger.info(
                "LLM deduplication complete: %d → %d items (%d duplicates removed)",
                len(items),
                len(deduplicated),
                len(items) - len(deduplicated),
            )
            return deduplicated
        except Exception as exc:
            logger.error("LLM deduplication failed: %s. Returning original items.", exc)
            return items

    def _deduplicate_batched(
        self, items: list[ContentItem], batch_size: int, verbose: bool = False
    ) -> list[list[int]]:
        all_groups = []
        total_batches = (len(items) + batch_size - 1) // batch_size

        for batch_idx in range(total_batches):
            start = batch_idx * batch_size
            end = min(start + batch_size, len(items))
            batch = items[start:end]
            logger.debug("Processing batch %d/%d (%d items)", batch_idx + 1, total_batches, len(batch))

            batch_groups = self._get_similar_groups(batch, verbose)
            for group in batch_groups:
                all_groups.append([start + idx for idx in group])

        return all_groups

    def _get_similar_groups(
        self, items: list[ContentItem], verbose: bool = False
    ) -> list[list[int]]:
        if len(items) < 2:
            return []

        prompt = self._build_deduplication_prompt(items)
        response_text = self._call_llm(prompt)
        if not response_text:
            logger.warning("LLM deduplication call failed, returning no groups")
            return []

        similar_groups = self._parse_similarity_response(response_text, len(items), verbose)

        filtered_groups = []
        for group in similar_groups:
            source_keys = [items[idx].source_key for idx in group]
            if len(set(source_keys)) == len(source_keys):
                filtered_groups.append(group)
            elif verbose:
                logger.debug("Skipping similarity group with duplicate sources: %s (%s)", group, source_keys)

        if verbose and filtered_groups:
            logger.info("LLM identified %d cross-source similarity groups:", len(filtered_groups))
            for group in filtered_groups:
                group_info = [
                    f"{items[idx].title[:30]}... ({items[idx].source_key})"
                    for idx in group
                ]
                logger.info("  Group: %s", group_info)

        return filtered_groups

    def _build_deduplication_prompt(self, items: list[ContentItem]) -> str:
        threshold = self.config.llm_deduplication.similarity_threshold if self.config.llm_deduplication else 85

        articles_data = []
        for idx, item in enumerate(items):
            articles_data.append({
                "index": idx,
                "id": item.id,
                "source": item.source_key,
                "title": item.title,
                "summary": item.summary,
                "category": item.categories[0] if item.categories else "Unknown",
                "relevance_score": item.relevance_score or 0,
                "practice_area": item.relevance_practice_area or "Sonstiges",
            })

        articles_json = json.dumps(articles_data, ensure_ascii=False, indent=2)

        return f"""Du wirst eine Reihe von Nachrichtenartikeln analysiert bekommen. Deine Aufgabe ist es, Artikel zu identifizieren, die einen ähnlichen Inhalt haben.

WICHTIG - CROSS-SOURCE DEDUPLICATION NUR:
- Nur Artikel von VERSCHIEDENEN Quellen (\"source\" Feld) können als Duplikate gruppiert werden
- Artikel mit der GLEICHEN Quelle DÜRFEN NICHT in eine Gruppe zusammengefasst werden

Mit einem Ähnlichkeitsschwellenwert von {threshold}%: Artikel mit höherer Ähnlichkeit sollten als Duplikate gruppiert werden (NUR wenn von verschiedenen Quellen).

ARTIKEL-LISTE (im JSON-Format):
{articles_json}

ANTWORTFORMAT:
Antworte AUSSCHLIESSLICH mit folgendem JSON-Format. KEIN zusätzlicher Text:
{{
  \"similar_groups\": [
    {{\"indices\": [0, 2], \"reason\": \"Beide berichten über gleiche Untersuchung\"}}
  ]
}}

Falls KEINE ähnlichen Gruppen von verschiedenen Quellen gefunden: Antworte mit {{\"similar_groups\": []}}
"""

    def _parse_similarity_response(
        self, response_text: str, num_items: int, verbose: bool = False
    ) -> list[list[int]]:
        try:
            response_text = response_text.strip()
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]
                response_text = response_text.strip()

            data = json.loads(response_text)
            similar_groups_raw = data.get("similar_groups", [])

            similar_groups = []
            for group_data in similar_groups_raw:
                if isinstance(group_data, dict):
                    indices = group_data.get("indices", [])
                elif isinstance(group_data, list):
                    indices = group_data
                else:
                    continue

                if not isinstance(indices, list) or len(indices) < 2:
                    continue

                if all(isinstance(idx, int) and 0 <= idx < num_items for idx in indices):
                    similar_groups.append(sorted(set(indices)))

            return similar_groups
        except json.JSONDecodeError as exc:
            logger.error("Failed to parse LLM similarity response as JSON: %s", exc)
            logger.debug("Response was: %s", response_text[:500])
            return []
        except Exception as exc:
            logger.error("Error parsing similarity response: %s", exc)
            return []

    def _filter_duplicates(
        self, items: list[ContentItem], similar_groups: list[list[int]], verbose: bool = False
    ) -> list[ContentItem]:
        if not similar_groups:
            return items

        keep_strategy = self.config.llm_deduplication.keep_strategy if self.config.llm_deduplication else "highest_score"
        to_remove = set()
        marked_duplicates = {}

        for group in similar_groups:
            if len(group) < 2:
                continue

            if keep_strategy == "highest_score":
                kept_idx = max(group, key=lambda idx: items[idx].relevance_score or 0)
            elif keep_strategy == "earliest":
                kept_idx = min(group, key=lambda idx: items[idx].discovered_at or "")
            elif keep_strategy == "latest":
                kept_idx = max(group, key=lambda idx: items[idx].discovered_at or "")
            else:
                kept_idx = group[0]

            for idx in group:
                if idx != kept_idx:
                    to_remove.add(idx)
                    marked_duplicates[items[idx].id] = items[kept_idx].id
                    if verbose:
                        logger.debug(
                            "Marking %s (%s) as duplicate of %s (%s)",
                            items[idx].id,
                            items[idx].title,
                            items[kept_idx].id,
                            items[kept_idx].title,
                        )

        result = [item for idx, item in enumerate(items) if idx not in to_remove]

        for item_id, kept_id in marked_duplicates.items():
            for item in items:
                if item.id == item_id:
                    item.duplicate_of_id = kept_id
                    break

        if verbose and to_remove:
            logger.debug("Removed %d duplicate items from %d", len(to_remove), len(items))

        return result

    def _call_llm(self, prompt: str) -> str | None:
        try:
            return self._call_custom_llm(prompt)
        except Exception as exc:
            logger.error("LLM call failed: %s", exc)
            return None

    def _call_custom_llm(self, prompt: str) -> str | None:
        if not self.api_url:
            logger.error("Custom LLM provider requires api_url")
            return None

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        system_message = """Du bist ein präziser Analyse-Assistent für Nachrichtendeduplication.
Deine Aufgabe ist es, ähnliche Artikel zu identifizieren.
Antworte AUSSCHLIESSLICH mit validem JSON. Kein Markdown, kein erklärender Text."""
        full_prompt = f"{system_message}\n\n{prompt}"
        data = {
            "input": full_prompt,
            "model": self.model,
        }

        try:
            response = requests.post(self.api_url, headers=headers, json=data, timeout=120)
            response.raise_for_status()
            result = response.json()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Custom LLM deduplication request sent")
            return self._extract_custom_response(result)
        except Exception as exc:
            logger.error("Custom LLM call failed: %s", exc)
            return None

    def _extract_custom_response(self, result: dict[str, Any]) -> str | None:
        try:
            outputs = result.get("output", [])
            if not isinstance(outputs, list):
                raise ValueError("Response 'output' is not a list")

            message_item = next((item for item in outputs if item.get("type") == "message"), None)
            if not message_item:
                raise ValueError("No item with type='message' found in output")

            content_list = message_item.get("content", [])
            if not isinstance(content_list, list):
                raise ValueError("Message 'content' is not a list")

            text_item = next((item for item in content_list if item.get("type") == "output_text"), None)
            if not text_item:
                raise ValueError("No item with type='output_text' found in content")

            text_value = text_item.get("text")
            if isinstance(text_value, str):
                return text_value
            return None
        except Exception as exc:
            logger.error("Failed to extract custom response: %s", exc)
            raise