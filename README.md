# Unilife Pricing Tracker

Config-driven PBSA pricing intelligence pipeline with append-only historical analytics output.

## What It Does

- Runs operator-specific adapters for Southampton PBSA sources.
- Uses staged extraction order per URL: API detection -> rendered DOM -> Playwright interaction fallback.
- Writes a validated analytics table to:
  - `data/Unilife_Pricing_Snapshot.xlsx` (sheet `All Pricing`)
- Writes auditable snapshot artifacts per run to:
  - `data/snapshots/<snapshot_id>/`

## Canonical Analytics Schema (Exact Order)

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

## History Rules (Critical)

- Append-only history.
- New runs append new rows only.
- Never overwrite valid historical rows.
- Never delete historical rows.
- Resets are only allowed via explicit instruction.

## Architecture

- `scraper/config/`
  - `cities.yaml`
  - `southampton.yaml`
- `scraper/core/`
  - models, IDs, normalisers, validators, API detector, coverage, workbook append logic, pipeline orchestration
- `scraper/parsers/`
  - operator adapters (`unilife`, `abodus`, `canvas`, `capitol`, `collegiate`, `crm`, `every_student`, `hello_student`, `homes_for_students`, `host`, `mezzino`, `now_students`, `prestige`, `student_roost`, `unite`, `vita`, `yugo`)
- `scraper/main.py`
  - primary entrypoint
- `scraper/unilife_pricing_snapshot.py`
  - backward-compatible wrapper entrypoint

## Local Run

```bash
python scraper/main.py --city Southampton --ignore-9am-gate
```

Compatibility wrapper:

```bash
python scraper/unilife_pricing_snapshot.py --city Southampton --ignore-9am-gate
```

Schema migration only:

```bash
python scraper/unilife_pricing_snapshot.py --clean-existing
```

## GitHub Actions

Workflow: `.github/workflows/unilife_pricing_snapshot.yml`

- `workflow_dispatch` supported
- daily schedule supported
- Europe/London 9AM gate enforced in runtime logic
- Playwright Chromium installation retained
- workbook commit-back retained

## Docs

- `docs/architecture.md`
- `docs/schema_and_ids.md`
- `docs/source_config.md`
- `docs/validation_rules.md`
