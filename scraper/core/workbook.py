from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from .ids import hall_id, room_id
from .normalisers import (
    clean_property_name,
    clean_room_name,
    normalise_academic_year,
    normalise_availability,
    normalise_floor_level,
    normalize_space,
    parse_contract_value_numeric,
    parse_price_to_weekly_numeric,
)

SHEET_NAME = "All Pricing"

OUTPUT_COLUMNS = [
    "Snapshot ID",
    "Snapshot Date",
    "City",
    "Operator",
    "HALL ID",
    "Property",
    "ROOM ID",
    "Room Name",
    "Floor Level",
    "Contract Length",
    "Academic Year",
    "Price",
    "Contract Value",
    "Incentives",
    "Availability",
    "Source URL",
    "Scrape Source",
]


def migrate_schema(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    for col in OUTPUT_COLUMNS:
        if col not in work.columns:
            work[col] = pd.NA

    text_cols = [c for c in OUTPUT_COLUMNS if c not in {"Price", "Contract Value"}]
    for col in text_cols:
        work[col] = work[col].apply(normalize_space)

    work["Property"] = work["Property"].apply(clean_property_name)
    work["Room Name"] = work["Room Name"].apply(clean_room_name)
    work["Floor Level"] = work["Floor Level"].apply(normalise_floor_level)
    work["Academic Year"] = work["Academic Year"].apply(normalise_academic_year)
    work["Availability"] = work["Availability"].apply(normalise_availability)
    work["Price"] = work["Price"].apply(parse_price_to_weekly_numeric)
    work["Contract Value"] = work["Contract Value"].apply(parse_contract_value_numeric)

    work["HALL ID"] = work.apply(
        lambda r: hall_id(r["Operator"], r["Property"]) if normalize_space(r["Property"]) else "",
        axis=1,
    )
    work["ROOM ID"] = work.apply(
        lambda r: room_id(r["Operator"], r["Property"], r["Room Name"])
        if normalize_space(r["Property"]) and normalize_space(r["Room Name"])
        else "",
        axis=1,
    )
    return work[OUTPUT_COLUMNS]


def read_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    return migrate_schema(pd.read_excel(path, sheet_name=SHEET_NAME, engine="openpyxl"))


def save_history(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, index=False, sheet_name=SHEET_NAME, engine="openpyxl")


def migrate_workbook(path: Path) -> Dict[str, int]:
    if not path.exists():
        return {"before": 0, "after": 0}
    before = pd.read_excel(path, sheet_name=SHEET_NAME, engine="openpyxl")
    migrated = migrate_schema(before)
    save_history(path, migrated)
    return {"before": len(before), "after": len(migrated)}


def dedupe_within_run(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for row in rows:
        key = []
        for col in OUTPUT_COLUMNS:
            value = row.get(col, "")
            if col in {"Price", "Contract Value"}:
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    key.append("")
                else:
                    key.append(f"{float(value):.2f}")
            else:
                key.append(normalize_space(value))
        key_t = tuple(key)
        if key_t in seen:
            continue
        seen.add(key_t)
        out.append(row)
    return out


def append_rows(path: Path, rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    # Append-only by design: existing historical rows are never deleted/overwritten.
    history = read_history(path)
    run_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    merged = pd.concat([history, run_df], ignore_index=True)
    save_history(path, merged)
    return len(history), len(run_df)
