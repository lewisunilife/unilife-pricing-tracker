# Schema and IDs

Project: [unilife-pricing-tracker](https://github.com/lewisunilife/unilife-pricing-tracker)

## Exact Column Order

1. Snapshot ID
2. Snapshot Date
3. City
4. Operator
5. HALL ID
6. Property
7. ROOM ID
8. Room Name
9. Floor Level
10. Contract Length
11. Academic Year
12. Price
13. Contract Value
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
Standardized to canonical labels (e.g. `Ground`, `First`, `Second`, or `First to Third`).
No inferred values.

## Academic Year

Populated only from explicit year text and normalized to `YYYY/YY` (e.g. `2026/27`).
No inferred values.

## Price

Stored as numeric weekly value only.

Rules:
- Weekly text is parsed to decimal (2dp)
- Monthly text is converted using `monthly * 12 / 52` (2dp)
- Ambiguous period leaves `Price` blank

## Contract Value

Stored as numeric total contract rent only when the source explicitly shows a total.

Rules:
- No inferred calculations from weekly rent x weeks
- If total rent is not visibly shown, `Contract Value` stays blank

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
