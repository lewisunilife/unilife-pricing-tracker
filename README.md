# Unilife Pricing Tracker

Multi-city student accommodation pricing intelligence tracker with append-only historical snapshots.

## What This Is

- Architected as an **any-city** system.
- **Southampton** is the first detailed competitor city implemented.
- Historical workbook lives at `data/Unilife_Pricing_Snapshot.xlsx` (sheet: `All Pricing`).
- Cloud runs via GitHub Actions and auto-commits updated workbook rows back to the repo.

## Exact Schema (Column Order)

1. Snapshot ID
2. Snapshot Date
3. Run Timestamp
4. City
5. Operator
6. HALL ID
7. Property
8. ROOM ID
9. Room Name
10. Floor Level
11. Contract Length
12. Academic Year
13. Price
14. Availability
15. Source URL
16. Scrape Source

## ID Definitions

### HALL ID

Stable deterministic hall identifier for `Operator + Property`.

- Canonical text normalization (case/whitespace/punctuation)
- Deterministic slug + stable hash suffix
- Does **not** depend on run date or row order

### ROOM ID

Stable deterministic room identifier for `Operator + Property + Room Name`.

- Canonical text normalization
- Deterministic slug + stable hash suffix
- Blank if room name is blank
- Does **not** depend on run date or row order

## Floor Level and Academic Year

- `Floor Level` is populated only when publicly visible in source text.
- `Academic Year` is populated only when publicly visible in source text.
- Neither field is guessed or fabricated.

## Historical Data Rules

- Append-only history.
- New runs append new rows only.
- No cross-history dedupe.
- Historical rows must never be destructively overwritten.
- Schema migrations/backfills are allowed for newly introduced metadata fields.

## Southampton Master Property List

Southampton source URLs are maintained in:
- `scraper/source_config.py`

This includes property-level URLs for:
- Abodus Student Living
- Canvas Student
- Capitol Students
- Collegiate
- CRM Students
- Every Student
- Hello Student
- Homes for Students
- Host Students
- Mezzino
- Now Students
- Prestige Student Living
- Student Roost
- Unilife
- Unite Students
- Vita Student
- Yugo

## Running Locally

```bash
python scraper/unilife_pricing_snapshot.py
```

Optional migration-only mode:

```bash
python scraper/unilife_pricing_snapshot.py --clean-existing
```

## GitHub Actions

Workflow:
- `.github/workflows/unilife_pricing_snapshot.yml`

Behavior:
- `workflow_dispatch` supported
- daily schedule enabled
- UTC cron + Europe/London 9AM gate in scraper logic
- installs dependencies and Playwright Chromium
- appends new snapshot rows
- commits workbook back when files changed

## Adding New Sources Safely

1. Add source entries in `scraper/source_config.py`.
2. Reuse existing scraper type or add a new handler.
3. Run scraper.

This appends future snapshot rows without rewriting old historical data.

## Internal Docs

- `docs/schema_and_ids.md`
- `docs/source_config.md`
