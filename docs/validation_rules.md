# Validation Rules

## Row-Level Rules

- `Price`: numeric weekly only; ambiguous period -> blank
- `Contract Value`: numeric total rent only when explicitly shown
- `Academic Year`: canonical `YYYY/YY` only; malformed values rejected to blank
- `Property`: cleaned to proper-case building name
- `Room Name`: cleaned of CTA/price/offer leakage; if equal to property, blanked
- `Floor Level`: canonicalized (`Ground`, `First`, `Second`, etc.) or blank
- `Incentives`: kept separate from `Room Name` and `Price`
- `Availability`: normalized to `Available`, `Sold Out`, `Waitlist`, `Limited Availability`, `Unavailable`, `Unknown`

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

Missing-price rows are additionally classified in summary output:

- `sold_out`
- `unavailable_no_contract_options`
- `hidden_deeper_in_flow`
- `parser_selector_failure`
- `blocked`
- `ambiguous_period`
- `not_shown_publicly`

## Accuracy Rule

Accuracy is prioritized over volume:

- uncertain values are blanked
- weakly inferred values are not published
- obviously contaminated rows are not silently accepted
