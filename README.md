# NewsHive

Specification-driven news discovery and enrichment pipeline. Scrapes web sources and email folders, extracts article content, and generates AI-powered German summaries with relevance scoring.

## Features

- **Discovery**: Scrapes configured web sources and Outlook email folders for new articles
- **Deduplication**: Tracks processed items via a local state store — no duplicates
- **Extraction**: Full article content retrieval with Playwright/Selenium support
- **Summarization**: LLM-generated German summaries (OpenAI, Anthropic, or custom endpoint)
- **Relevance Scoring**: 5-dimension AI scoring for CCCI/ESG practice area relevance
- **Output**: Structured JSON results + webhook delivery (Power Automate or similar)
- **LLM-agnostic**: Set `LLM_PROVIDER=openai`, `anthropic`, or `custom` — swap without code changes

## Workflow

1. Configure sources in `config.yaml` and `sources.yaml`
2. Set your LLM provider and API key in `.env`
3. Run the pipeline — results are written to `data/results.json`
4. Optionally deliver results via webhook

## Quick Start

See [QUICKSTART.md](QUICKSTART.md) for installation, configuration, and usage.

## LLM Configuration

NewsHive supports three providers, configured via environment variables:

| Provider | `LLM_PROVIDER` | Required packages |
|---|---|---|
| OpenAI | `openai` | `pip install newshive[openai]` |
| Anthropic | `anthropic` | `pip install newshive[anthropic]` |
| Custom endpoint | `custom` | — (uses `requests`) |

```bash
# .env example — OpenAI
LLM_PROVIDER=openai
LLM_MODEL=gpt-4o
LLM_API_KEY=sk-...

# .env example — Anthropic
LLM_PROVIDER=anthropic
LLM_MODEL=claude-3-5-sonnet-20241022
LLM_API_KEY=sk-ant-...

# .env example — Custom endpoint
LLM_PROVIDER=custom
LLM_MODEL=your-model-name
LLM_API_KEY=your-bearer-token
LLM_API_URL=https://your-endpoint/v1/responses
```

If no LLM is configured, discovery and extraction still run — only summarization and scoring are skipped.

## Repository Structure

```
src/          # Pipeline source code
tests/        # Test suites
tools/        # Utility scripts
data/         # Runtime output (gitignored)
```
