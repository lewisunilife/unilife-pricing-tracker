# Architecture

Project: [unilife-pricing-tracker](https://github.com/lewisunilife/unilife-pricing-tracker)

## Pipeline Overview

The scraper is now a config-driven pipeline with clear separation of concerns:

- `scraper/config/`: city and source registry YAML files
- `scraper/core/models.py`: typed source/run models
- `scraper/core/ids.py`: deterministic HALL ID / ROOM ID generation
- `scraper/core/normalisers.py`: canonical field normalisation helpers
- `scraper/core/validators.py`: strict row validation and publishability rules
- `scraper/core/api_detector.py`: API candidate discovery and JSON extraction
- `scraper/core/playwright_helpers.py`: browser navigation/click utilities
- `scraper/core/coverage.py`: property-level and URL-level coverage tracking
- `scraper/core/workbook.py`: append-only Excel schema migration + writing
- `scraper/core/pipeline.py`: orchestration of API -> DOM -> Playwright stages
- `scraper/main.py`: primary entrypoint
- `scraper/unilife_pricing_snapshot.py`: backward-compatible wrapper entrypoint

## Extraction Strategy

Each URL attempt follows this order:

1. API detection (`api_detector`)
2. rendered DOM adapter extraction
3. Playwright interaction fallback
4. fail safely with exact reason

## Adapter Strategy

Operator-specific adapters live in `scraper/parsers/`.

Each adapter owns operator-specific selectors/interactions. No single global parser is used as the published extraction path.

## Persistence Model

Only one persistent analytics dataset is kept in the repository:

- `data/Unilife_Pricing_Snapshot.xlsx` (sheet `All Pricing`)

No per-run snapshot directories are persisted in version control.

## History Guarantee

- Workbook remains append-only.
- Historical rows are never deleted or overwritten by normal runs.
- Reset/wipe is only allowed via explicit user instruction.
