# Source Configuration

Project: [unilife-pricing-tracker](https://github.com/lewisunilife/unilife-pricing-tracker)

## Goal

Keep source management config-driven so new URLs and future cities can be added without refactoring append/history logic.

## Current Structure

File:
- `scraper/source_config.py`

Top-level object:
- `CITY_SOURCES`

Shape:
- `CITY_SOURCES[<City>]` => list of source entries
- each source entry contains:
  - `operator`
  - `property`
  - `url`
  - `scraper`

## Southampton Master Property List

Southampton is the first detailed city list and is stored as property-level URLs where available.

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

## Scraper Routing

Current scraper types:
- `unilife`: Unilife-specific room/contract extraction logic
- `generic`: reusable extractor for non-Unilife operator pages

Routing happens in:
- `scraper/unilife_pricing_snapshot.py`

## Adding New URLs Safely

1. Add a new source entry under the target city in `CITY_SOURCES`.
2. Choose existing `scraper` type (`generic` or `unilife`), or add a new scraper handler.
3. Run scraper.
4. New rows append with new snapshot ID.

This does not rewrite prior history.

## Future City Expansion

To add a city (e.g. Birmingham, Bristol, Winchester, Guildford):

1. Add a new city key in `CITY_SOURCES`.
2. Add property-level source entries for that city.
3. Reuse existing handlers or add new per-operator handlers as needed.
4. Run and validate appended rows.

No changes are required to core append history logic for normal additions.

## References

- [Repo](https://github.com/lewisunilife/unilife-pricing-tracker)
- [README](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/README.md)
- [Scraper](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/scraper/unilife_pricing_snapshot.py)
- [Workflow](https://github.com/lewisunilife/unilife-pricing-tracker/blob/main/.github/workflows/unilife_pricing_snapshot.yml)
