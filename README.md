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
14. Incentives
15. Availability
16. Source URL
17. Scrape Source

## ID Definitions

### HALL ID

Stable deterministic hall identifier for `Operator + Property`.

### ROOM ID

Stable deterministic room identifier for `Operator + Property + Room Name`.

## Field Rules

- `Room Name`: title-selector based and cleaned; excludes price/CTA/offer text.
- `Floor Level`: populated only from explicit visible floor text.
- `Academic Year`: populated only from explicit visible year text.
- `Incentives`: visible offer text only (cashback, bus pass, bedding pack, vouchers, discounts, etc.).

## Historical Data Rules

- Append-only history.
- New runs append new rows only.
- No destructive overwrites of valid historical snapshots.
- Schema migrations/backfills are allowed for new metadata columns.

## Southampton Master Property List

Southampton source URLs are maintained in:
- `scraper/source_config.py`

## Running Locally

```bash
python scraper/unilife_pricing_snapshot.py
```

Migration-only mode:

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

## Internal Docs

- `docs/schema_and_ids.md`
- `docs/source_config.md`
