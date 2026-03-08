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
14. Incentives
15. Availability
16. Source URL
17. Scrape Source

## HALL ID

Deterministic hall-level ID generated from canonical `Operator + Property` with a stable hash suffix.

## ROOM ID

Deterministic room-level ID generated from canonical `Operator + Property + Room Name` with a stable hash suffix.

Rules:
- Blank property => blank HALL ID and ROOM ID
- Blank room name => blank ROOM ID

## Floor Level

Populated only from explicit floor text visible on source pages/modals/tiles.
No inferred values.

## Academic Year

Populated only from explicit year text (e.g. `2026/27`).
No inferred values.

## Incentives

Captures visible promotional text, e.g. cashback, bus pass, bedding/kitchen pack, voucher, discount, offer.

Rules:
- Incentives must stay in `Incentives`
- Incentives must not pollute `Room Name` or `Price`

## Historical Rules

- Append-only snapshots
- New runs append rows
- No destructive rewrites of valid historical data
- Safe schema migrations/backfills are allowed for new columns

## References

- [README](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/README.md)
- [Scraper](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/scraper/unilife_pricing_snapshot.py)
- [Workflow](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/.github/workflows/unilife_pricing_snapshot.yml)
- [Workbook](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/data/Unilife_Pricing_Snapshot.xlsx)
