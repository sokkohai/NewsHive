# NewsHive — Quick Start

## Installation

```bash
# Standard install
pip install -e .

# With OpenAI support
pip install -e ".[openai]"

# With Anthropic support
pip install -e ".[anthropic]"

# Development (includes both LLM libraries + test tools)
pip install -e ".[dev]"
```

## Configuration

### 1. Environment variables

```bash
cp .env.example .env
```

Edit `.env`:

```bash
# LLM Provider — choose one:

# OpenAI
LLM_PROVIDER=openai
LLM_MODEL=gpt-4o
LLM_API_KEY=sk-...

# Anthropic
LLM_PROVIDER=anthropic
LLM_MODEL=claude-3-5-sonnet-20241022
LLM_API_KEY=sk-ant-...

# Custom endpoint
LLM_PROVIDER=custom
LLM_MODEL=your-model-name
LLM_API_KEY=your-bearer-token
LLM_API_URL=https://your-endpoint/v1/responses

# Email integration (optional)
OUTLOOK_CLIENT_ID=...
OUTLOOK_CLIENT_SECRET=...
OUTLOOK_TENANT_ID=...

# Webhook delivery (optional)
WEBHOOK_URL=https://...
```

> **Never commit `.env`** — it is in `.gitignore`.

### 2. Sources

```bash
cp sources.yaml.example sources.yaml
cp config.yaml.example config.yaml
```

Edit `sources.yaml` to add your web sources and email folders.  
Edit `config.yaml` to configure categories, keywords, and relevance scoring.

## Running the Pipeline

```bash
# Full pipeline (discovery → extraction → enrichment → output)
python -m src pipeline

# Or using the entry point (after pip install -e .)
newshive

# Single source only
newshive --site bakertilly.de

# Dry run (discover only, no LLM calls)
newshive --no-enrich
```

Results are written to `data/results.json`.

## Running Tests

```bash
pytest
pytest --cov=src --cov-report=term-missing
```

## Code Quality

```bash
ruff check src/ tests/
mypy src/
```
