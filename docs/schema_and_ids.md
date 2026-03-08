# Schema and IDs

Project: [unilife-pricing-tracker](https://github.com/lewisunilife/unilife-pricing-tracker)

## Exact Column Order

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

This order is enforced in:
- scraper output rows
- workbook migrations
- append logic
- workbook write order

## HALL ID

Purpose:
- Stable hall-level identifier for analytics.
- Unique by `Operator + Property`.

Generation:
- Canonicalize operator/property (lowercase, trimmed, punctuation-normalized slug)
- Build deterministic token from canonical values
- Add short stable hash suffix for collision resistance

Format:
- `hall-<operator-slug>-<property-slug>-<hash8>`

Properties:
- Deterministic across runs
- Not dependent on row order, snapshot date, or workbook position

## ROOM ID

Purpose:
- Stable room-type identifier for analytics.
- Unique by `Operator + Property + Room Name`.

Generation:
- Canonicalize operator/property/room
- Build deterministic token and stable hash

Format:
- `room-<operator-slug>-<property-slug>-<room-slug>-<hash8>`

Rules:
- If `Property` is blank => `HALL ID` and `ROOM ID` blank
- If `Room Name` is blank => `ROOM ID` blank

## Floor Level

Only populated when publicly visible in source text.
Examples that may be captured:
- Ground Floor
- 1st Floor
- Floors 3-5
- Level 2

No inference is performed when not visible.

## Academic Year

Only populated when publicly visible in source text.
Examples:
- 2025/26
- 2026/27

No inference is performed when not visible.

## Historical Data Rules

- Append-only history: old snapshots are retained.
- New runs append new rows.
- No historical dedupe across runs.
- Allowed workbook-wide modification: safe schema migration/backfill for new metadata columns.
- Historical rows must never be destructively overwritten.

## References

- [README](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/README.md)
- [Scraper](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/scraper/unilife_pricing_snapshot.py)
- [Workflow](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/.github/workflows/unilife_pricing_snapshot.yml)
- [Workbook](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/data/Unilife_Pricing_Snapshot.xlsx)
