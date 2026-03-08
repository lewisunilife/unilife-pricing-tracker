# Source Configuration

Project: [unilife-pricing-tracker](https://github.com/lewisunilife/unilife-pricing-tracker)

## Goal

Keep source management config-driven so new URLs and future cities can be added without refactoring core append/history logic.

## Current Structure

File:
- `scraper/source_config.py`

Top-level object:
- `CITY_SOURCES`

Entry fields:
- `operator`
- `property`
- `url`
- `scraper`

## Scraper Routing

Current scraper routing types:
- `unilife`: strict Unilife booking-modal parsing (contract tile rows)
- `generic`: operator-targeted non-Unilife parsing with strict room-title and field separation

Routing implementation:
- `scraper/unilife_pricing_snapshot.py`
- `scraper/parsers/*_parser.py` (operator-specific parser modules)

## Southampton Master Property List

Southampton is the first detailed city list and uses property-level source URLs where possible.

Current operators configured:
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

## Adding New URLs Safely

1. Add a source entry to `CITY_SOURCES` under the correct city.
2. Choose `scraper` type (`unilife` or `generic`) or add a new parser path.
3. Run scraper.

Result:
- new snapshots append new rows
- existing historical rows remain untouched
- run summary includes a coverage audit entry for every configured source

## Future City Expansion

To add Birmingham, Bristol, Winchester, Guildford, etc.:

1. Add new city key in `CITY_SOURCES`
2. Add property-level entries
3. Reuse or extend parser routing
4. Run and validate appended rows

## References

- [Repo](https://github.com/lewisunilife/unilife-pricing-tracker)
- [README](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/README.md)
- [Scraper](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/scraper/unilife_pricing_snapshot.py)
- [Workflow](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/.github/workflows/unilife_pricing_snapshot.yml)
