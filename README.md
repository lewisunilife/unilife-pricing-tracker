# Unilife Pricing Tracker

Production-ready pricing intelligence tracker for student accommodation with append-only historical snapshots.

## Overview

- The tracker is **multi-city by design**.
- **Southampton** is the first city implemented for competitor expansion.
- Data is stored as a historical dataset in Excel and updated automatically by GitHub Actions.

Workbook path:
- `data/Unilife_Pricing_Snapshot.xlsx`

Sheet:
- `All Pricing`

## Schema

The tracker writes rows using this exact column order:

1. `Snapshot ID`
2. `Snapshot Date`
3. `Run Timestamp`
4. `City`
5. `Operator`
6. `Property`
7. `Room Name`
8. `Contract Length`
9. `Price`
10. `Availability`
11. `Source URL`
12. `Scrape Source`

Notes:
- `Snapshot ID` is the run-level ISO timestamp (same for all rows in one run).
- `Run Timestamp` is in `Europe/London`.
- `Scrape Source` is `GitHub Actions` for workflow runs and `Local` for local runs.

## Historical Behavior

- Dataset is append-only for normal operation.
- New runs append beneath existing history, even when prices have not changed.
- Deduplication is only within the current run.
- No deduplication against historical runs.

## City Coverage

The scraper uses a city-driven configuration so additional cities can be added cleanly.

Current implemented city:
- Southampton

Current Southampton operators:
- Unilife
- Yugo
- Student Roost
- Unite Students

## Manual Usage

Run locally from repo root:

```bash
python scraper/unilife_pricing_snapshot.py
```

Optional one-time workbook cleanup mode:

```bash
python scraper/unilife_pricing_snapshot.py --clean-existing
```

## GitHub Actions

Workflow file:
- `.github/workflows/unilife_pricing_snapshot.yml`

Behavior:
- Supports `workflow_dispatch` manual runs.
- Runs daily on schedule.
- Uses UTC cron plus in-script London 09:00 gating.
- Installs Python dependencies and Playwright Chromium.
- Runs scraper and appends new rows.
- Commits updated workbook back to the repository when files changed.
