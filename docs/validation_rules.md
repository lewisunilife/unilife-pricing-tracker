# Validation Rules

## Row-Level Rules

- `Price`: numeric weekly only; ambiguous period -> blank
- `Academic Year`: canonical `YYYY/YY` only; malformed values rejected to blank
- `Property`: cleaned to proper-case building name
- `Room Name`: cleaned of CTA/price/offer leakage; if equal to property, blanked
- `Floor Level`: canonicalized (`Ground`, `First`, `Second`, etc.) or blank
- `Incentives`: kept separate from `Room Name` and `Price`
- `Availability`: normalized to stable labels where possible

Rows that are not confidently publishable are filtered before workbook append.

## Source-Level Rules

Coverage is logged per property URL attempt with:

- stage (`api`, `dom`, `playwright`, `load`, `adapter`)
- method (`API`, `DOM`, `Playwright`, `none`)
- status (`success`, `failed`, `blocked`)
- reason (exact text)
- rows extracted

Property summary merges all supplied URLs and reports:

- scraped successfully with rows
- partially blocked / no rows
- fully blocked
- no rows from checked pages

## Accuracy Rule

Accuracy is prioritized over volume:

- uncertain values are blanked
- weakly inferred values are not published
- obviously contaminated rows are not silently accepted
