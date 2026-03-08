# Unilife Pricing Tracker

This repository runs an automated scraper for Unilife room pricing and stores historical snapshots in Excel.

## What It Does

- Scrapes live room/pricing data for Unilife properties.
- Writes rows to `data/Unilife_Pricing_Snapshot.xlsx`.
- Appends a new historical snapshot on each valid run, even if prices did not change.
- Adds both `Snapshot Date` and `Run Timestamp` (Europe/London).

## Workbook Location

- `data/Unilife_Pricing_Snapshot.xlsx`
- Sheet: `All Pricing`
- Columns:
  - `Snapshot Date`
  - `Run Timestamp`
  - `Property`
  - `Room Name`
  - `Contract Length`
  - `Price`
  - `Availability`
  - `Source URL`

## Manual Run (GitHub Actions)

Use **Actions > Unilife Pricing Snapshot > Run workflow** (`workflow_dispatch`) to trigger a run on demand.

## Daily Schedule and UK 9AM Logic

The workflow is scheduled at both `08:00 UTC` and `09:00 UTC` daily to handle UK daylight saving changes.

The scraper checks current `Europe/London` local time and only proceeds when local time is exactly **09:00**. Non-matching schedule runs exit cleanly without changing files.

## Automatic Commit Back to Repo

After a successful 09:00 London run, the workflow commits and pushes the updated workbook back to this repository (only when files changed).
