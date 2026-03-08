import argparse
import asyncio
import datetime as dt
import hashlib
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from playwright.async_api import async_playwright

from source_config import CITY_SOURCES
from parsers import common
from parsers import (
    abodus_parser,
    canvas_parser,
    capitol_parser,
    collegiate_parser,
    crm_parser,
    every_student_parser,
    hello_student_parser,
    homes_for_students_parser,
    host_students_parser,
    mezzino_parser,
    now_students_parser,
    prestige_student_living_parser,
    student_roost_parser,
    unilife_parser,
    unite_parser,
    vita_parser,
    yugo_parser,
)

LONDON_TZ = ZoneInfo("Europe/London")
SHEET_NAME = "All Pricing"

OUTPUT_COLUMNS = [
    "Snapshot ID",
    "Snapshot Date",
    "Run Timestamp",
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
    "Incentives",
    "Availability",
    "Source URL",
    "Scrape Source",
]

PARSER_MAP: Dict[str, Callable] = {
    "abodus student living": abodus_parser.parse,
    "canvas student": canvas_parser.parse,
    "capitol students": capitol_parser.parse,
    "collegiate": collegiate_parser.parse,
    "crm students": crm_parser.parse,
    "every student": every_student_parser.parse,
    "hello student": hello_student_parser.parse,
    "homes for students": homes_for_students_parser.parse,
    "host students": host_students_parser.parse,
    "mezzino": mezzino_parser.parse,
    "now students": now_students_parser.parse,
    "prestige student living": prestige_student_living_parser.parse,
    "student roost": student_roost_parser.parse,
    "unilife": unilife_parser.parse,
    "unite students": unite_parser.parse,
    "vita student": vita_parser.parse,
    "yugo": yugo_parser.parse,
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def workbook_path() -> Path:
    return repo_root() / "data" / "Unilife_Pricing_Snapshot.xlsx"


def scrape_source_label() -> str:
    return "GitHub Actions" if os.getenv("GITHUB_ACTIONS", "").lower() == "true" else "Local"


def normalize_key(text: str) -> str:
    return common.normalize_key(text)


def stable_hall_id(operator: str, property_name: str) -> str:
    op = normalize_key(operator)
    prop = normalize_key(property_name)
    if not op or not prop:
        return ""
    digest = hashlib.sha1(f"{op}|{prop}".encode("utf-8")).hexdigest()[:8]
    return f"hall-{op}-{prop}-{digest}"


def stable_room_id(operator: str, property_name: str, room_name: str) -> str:
    op = normalize_key(operator)
    prop = normalize_key(property_name)
    room = normalize_key(room_name)
    if not op or not prop or not room:
        return ""
    digest = hashlib.sha1(f"{op}|{prop}|{room}".encode("utf-8")).hexdigest()[:8]
    return f"room-{op}-{prop}-{room}-{digest}"


def should_run_for_london_9am() -> bool:
    event_name = os.getenv("GITHUB_EVENT_NAME", "").strip().lower()
    enforce = os.getenv("ENFORCE_LONDON_9AM", "").strip().lower() in {"1", "true", "yes"}
    if event_name != "schedule" and not enforce:
        return True
    now = dt.datetime.now(LONDON_TZ)
    if now.hour == 9 and now.minute == 0:
        return True
    print(f"[INFO] Skipping run: Europe/London time is {now.strftime('%Y-%m-%d %H:%M:%S %Z')} (required: 09:00).")
    return False


def parse_run_timestamp(value: Any) -> Optional[dt.datetime]:
    text = common.normalize_space(value)
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce", utc=False)
    if pd.isna(parsed):
        return None
    if parsed.tzinfo is None:
        return parsed.to_pydatetime().replace(tzinfo=LONDON_TZ)
    return parsed.to_pydatetime().astimezone(LONDON_TZ)


def to_snapshot_id(ts: Optional[dt.datetime]) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S") if ts else ""


def infer_city(url: str) -> str:
    low = common.normalize_space(url).lower()
    for city in ["southampton", "winchester", "guildford", "birmingham", "bristol"]:
        if city in low:
            return city.title()
    return "Unknown"


def migrate_schema(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    for col in OUTPUT_COLUMNS:
        if col not in work.columns:
            work[col] = pd.NA

    text_cols = [c for c in OUTPUT_COLUMNS if c != "Price"]
    for col in text_cols:
        work[col] = work[col].apply(common.normalize_space)

    work["Snapshot Date"] = work["Snapshot Date"].str.replace(" 00:00:00", "", regex=False)
    missing_city = work["City"].str.lower().isin(["", "unknown"])
    work.loc[missing_city, "City"] = work.loc[missing_city, "Source URL"].apply(infer_city)

    missing_sid = work["Snapshot ID"].apply(common.normalize_space) == ""
    work.loc[missing_sid, "Snapshot ID"] = work.loc[missing_sid, "Run Timestamp"].apply(lambda v: to_snapshot_id(parse_run_timestamp(v)))

    work["Operator"] = work["Operator"].apply(lambda x: common.normalize_space(x).title() if common.normalize_space(x).isupper() else common.normalize_space(x))
    work["Property"] = work["Property"].apply(lambda x: common.proper_case_property(common.normalize_space(x), ""))
    work["Room Name"] = work["Room Name"].apply(common.clean_room_name)
    work["Floor Level"] = work["Floor Level"].apply(common.normalise_floor_level)
    work["Academic Year"] = work["Academic Year"].apply(common.normalise_academic_year)
    work["Incentives"] = work["Incentives"].apply(common.extract_and_normalise_incentives)
    work["Price"] = work["Price"].apply(common.parse_price_to_weekly_numeric)

    work["HALL ID"] = work.apply(
        lambda r: stable_hall_id(r["Operator"], r["Property"]) if common.normalize_space(r["Property"]) else "",
        axis=1,
    )
    work["ROOM ID"] = work.apply(
        lambda r: stable_room_id(r["Operator"], r["Property"], r["Room Name"])
        if common.normalize_space(r["Property"]) and common.normalize_space(r["Room Name"])
        else "",
        axis=1,
    )

    missing_source = work["Scrape Source"].apply(common.normalize_space) == ""
    work.loc[missing_source, "Scrape Source"] = "Local"

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
    old = pd.read_excel(path, sheet_name=SHEET_NAME, engine="openpyxl")
    migrated = migrate_schema(old)
    save_history(path, migrated)
    return {"before": len(old), "after": len(migrated)}


def append_history(path: Path, rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    history = read_history(path)
    run_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    merged = pd.concat([history, run_df], ignore_index=True)
    save_history(path, merged)
    return len(history), len(run_df)


def make_row(
    snapshot_id: str,
    snapshot_date: str,
    run_timestamp: str,
    city: str,
    operator: str,
    property_name: str,
    payload: Dict[str, Any],
    scrape_source: str,
) -> Dict[str, Any]:
    operator_name = common.normalize_space(operator)
    prop = common.proper_case_property(property_name, property_name)
    room = common.clean_room_name(payload.get("Room Name", ""))
    hall_id = stable_hall_id(operator_name, prop) if prop else ""
    room_id = stable_room_id(operator_name, prop, room) if prop and room else ""
    return {
        "Snapshot ID": snapshot_id,
        "Snapshot Date": snapshot_date,
        "Run Timestamp": run_timestamp,
        "City": common.normalize_space(city),
        "Operator": operator_name,
        "HALL ID": hall_id,
        "Property": prop,
        "ROOM ID": room_id,
        "Room Name": room,
        "Floor Level": common.normalise_floor_level(payload.get("Floor Level", "")),
        "Contract Length": common.extract_contract_length(payload.get("Contract Length", "")) or common.normalize_space(payload.get("Contract Length", "")),
        "Academic Year": common.normalise_academic_year(payload.get("Academic Year", "")),
        "Price": common.parse_price_to_weekly_numeric(payload.get("Price", "")),
        "Incentives": common.extract_and_normalise_incentives(payload.get("Incentives", "")),
        "Availability": common.normalize_space(payload.get("Availability", "")) or common.infer_availability(payload.get("Availability", "")),
        "Source URL": common.normalize_space(payload.get("Source URL", "")) or common.normalize_space(payload.get("url", "")),
        "Scrape Source": scrape_source,
    }


def dedupe_current_run(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for row in rows:
        key = []
        for col in OUTPUT_COLUMNS:
            val = row.get(col, "")
            if col == "Price":
                key.append("" if val is None or pd.isna(val) else f"{float(val):.2f}")
            else:
                key.append(common.normalize_space(val))
        t = tuple(key)
        if t in seen:
            continue
        seen.add(t)
        out.append(row)
    return out


async def scrape_source(page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    ok = await common.safe_goto(page, src["url"])
    if not ok:
        return [], "page unavailable"

    parser = PARSER_MAP.get(src["operator"].lower())
    if not parser:
        return [], "parser missing"

    rows, reason = await parser(page, src)
    return rows, reason


async def run_scrape() -> int:
    if not should_run_for_london_9am():
        return 0

    mig = migrate_workbook(workbook_path())
    print(f"[INFO] Workbook migration complete: {mig['before']} -> {mig['after']} rows")

    now = dt.datetime.now(LONDON_TZ)
    snapshot_id = now.strftime("%Y-%m-%dT%H:%M:%S")
    snapshot_date = now.date().isoformat()
    run_timestamp = now.strftime("%Y-%m-%d %H:%M:%S %Z")
    source_label = scrape_source_label()

    sources = [{**s, "city": city} for city, entries in CITY_SOURCES.items() for s in entries]
    all_rows: List[Dict[str, Any]] = []
    coverage: List[Dict[str, str]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            for src in sources:
                print(f"[SOURCE] {src['city']} | {src['operator']} | {src['property']} | {src['url']}")
                rows, reason = await scrape_source(page, src)
                if rows:
                    materialized = [
                        make_row(
                            snapshot_id,
                            snapshot_date,
                            run_timestamp,
                            src["city"],
                            src["operator"],
                            src["property"],
                            r,
                            source_label,
                        )
                        for r in rows
                    ]
                    materialized = [r for r in materialized if r["Room Name"] or r["Price"] is not None]
                    all_rows.extend(materialized)
                    coverage.append({
                        "city": src["city"],
                        "operator": src["operator"],
                        "property": src["property"],
                        "status": "scraped successfully with rows",
                        "reason": str(len(materialized)),
                    })
                    print(f"[OK] collected {len(materialized)} rows")
                else:
                    status = "selector mismatch"
                    if reason in {"page unavailable", "parser missing"}:
                        status = reason
                    coverage.append({
                        "city": src["city"],
                        "operator": src["operator"],
                        "property": src["property"],
                        "status": status,
                        "reason": reason or "no room data",
                    })
                    print(f"[WARN] {src['operator']} | {src['property']} ({reason or 'no room data'})")
        finally:
            await page.close()
            await browser.close()

    all_rows = dedupe_current_run(all_rows)
    if not all_rows:
        print("[WARN] No rows scraped; workbook not updated.")
        print("Coverage audit:")
        for c in coverage:
            print(f" - {c['city']} | {c['operator']} | {c['property']} => {c['status']} ({c['reason']})")
        return 1

    prev, appended = append_history(workbook_path(), all_rows)

    cities = sorted({r["City"] for r in all_rows if common.normalize_space(r["City"])})
    operators = sorted({r["Operator"] for r in all_rows if common.normalize_space(r["Operator"])})
    properties = sorted({r["Property"] for r in all_rows if common.normalize_space(r["Property"])})

    with_price = sum(1 for r in all_rows if r.get("Price") is not None and not pd.isna(r.get("Price")))
    with_floor = sum(1 for r in all_rows if common.normalize_space(r["Floor Level"]))
    with_ay = sum(1 for r in all_rows if common.normalize_space(r["Academic Year"]))
    with_incentives = sum(1 for r in all_rows if common.normalize_space(r["Incentives"]))

    print("\nRun Summary")
    print(f"Snapshot ID: {snapshot_id}")
    print(f"Snapshot Date: {snapshot_date}")
    print(f"Run Timestamp: {run_timestamp}")
    print(f"Cities scraped: {', '.join(cities)}")
    print(f"Operators scraped: {', '.join(operators)}")
    print(f"Properties scraped: {len(properties)}")
    print(f"Rows appended: {appended}")
    print(f"Rows with prices: {with_price}")
    print(f"Rows without prices: {appended - with_price}")
    print(f"Rows with Floor Level: {with_floor}")
    print(f"Rows with Academic Year: {with_ay}")
    print(f"Rows with Incentives: {with_incentives}")
    print("Coverage audit:")
    for c in coverage:
        print(f" - {c['city']} | {c['operator']} | {c['property']} => {c['status']} ({c['reason']})")
    print(f"Workbook path: {workbook_path()}")
    print(f"Historical total rows: {prev + appended}")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pricing intelligence scraper")
    p.add_argument("--clean-existing", action="store_true", help="Migrate workbook schema without scraping.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.clean_existing:
        m = migrate_workbook(workbook_path())
        print("Workbook migration complete")
        print(f"Before rows: {m['before']}")
        print(f"After rows: {m['after']}")
        raise SystemExit(0)
    raise SystemExit(asyncio.run(run_scrape()))


if __name__ == "__main__":
    main()
