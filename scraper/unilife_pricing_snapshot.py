import argparse
import asyncio
import datetime as dt
import hashlib
import os
import re
import unicodedata
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from playwright.async_api import Browser, Page, async_playwright

sys.path.append(str(Path(__file__).resolve().parent))
from source_config import CITY_SOURCES

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
    "Availability",
    "Source URL",
    "Scrape Source",
]

PRICE_RE = re.compile(r"(?:£|Ł)?\s*\d{2,4}(?:[.,]\d{1,2})?\s*(?:pw|p/w|/\s*week|per\s*week)", re.IGNORECASE)
CONTRACT_RE = re.compile(r"\d{1,2}\s*(?:weeks?|months?)", re.IGNORECASE)
FLOOR_RE = re.compile(
    r"(Ground Floor|Lower Ground|Upper Ground|Level\s*\d+|Floor\s*\d+|Floors?\s*\d+\s*-\s*\d+|\d+(?:st|nd|rd|th)\s*Floor)",
    re.IGNORECASE,
)
ACADEMIC_YEAR_RE = re.compile(r"\b(20\d{2}\s*[/\-]\s*(?:20)?\d{2})\b")


def normalize_space(text: str) -> str:
    if text is None:
        return ""
    if isinstance(text, float) and pd.isna(text):
        return ""
    value = str(text)
    if value.strip().lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", value).strip()


def normalize_key(text: str) -> str:
    value = unicodedata.normalize("NFKD", (text or "").strip().lower())
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value


def stable_hall_id(operator: str, property_name: str) -> str:
    op = normalize_key(operator)
    hall = normalize_key(property_name)
    if not op or not hall:
        return ""
    raw = f"{op}|{hall}".encode("utf-8")
    short = hashlib.sha1(raw).hexdigest()[:8]
    return f"hall-{op}-{hall}-{short}"


def stable_room_id(operator: str, property_name: str, room_name: str) -> str:
    op = normalize_key(operator)
    hall = normalize_key(property_name)
    room = normalize_key(room_name)
    if not op or not hall or not room:
        return ""
    raw = f"{op}|{hall}|{room}".encode("utf-8")
    short = hashlib.sha1(raw).hexdigest()[:8]
    return f"room-{op}-{hall}-{room}-{short}"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def workbook_path() -> Path:
    return repo_root() / "data" / "Unilife_Pricing_Snapshot.xlsx"


def scrape_source_label() -> str:
    return "GitHub Actions" if os.getenv("GITHUB_ACTIONS", "").lower() == "true" else "Local"


def should_run_for_london_9am() -> bool:
    event_name = os.getenv("GITHUB_EVENT_NAME", "").strip().lower()
    enforce = os.getenv("ENFORCE_LONDON_9AM", "").strip().lower() in {"1", "true", "yes"}
    if event_name != "schedule" and not enforce:
        return True
    now_london = dt.datetime.now(LONDON_TZ)
    if now_london.hour == 9 and now_london.minute == 0:
        return True
    print(f"[INFO] Skipping run: Europe/London time is {now_london.strftime('%Y-%m-%d %H:%M:%S %Z')} (required: 09:00).")
    return False


def normalize_price(text: str) -> str:
    raw = normalize_space(text)
    if not raw:
        return ""
    m = PRICE_RE.search(raw)
    if not m:
        return ""
    token = normalize_space(m.group(0)).replace("Ł", "£")
    if "£" not in token:
        token = f"£{token}"
    return token.replace(" /", "/")


def parse_floor_level(text: str) -> str:
    m = FLOOR_RE.search(str(text or ""))
    return normalize_space(m.group(1)) if m else ""


def parse_academic_year(text: str) -> str:
    m = ACADEMIC_YEAR_RE.search(str(text or ""))
    if not m:
        return ""
    value = normalize_space(m.group(1)).replace("-", "/")
    parts = value.split("/")
    if len(parts) == 2 and len(parts[1]) == 2:
        value = f"{parts[0]}/{parts[1]}"
    return value


def parse_run_timestamp(value: Any) -> Optional[dt.datetime]:
    if pd.isna(value):
        return None
    parsed = pd.to_datetime(str(value).strip(), errors="coerce", utc=False)
    if pd.isna(parsed):
        return None
    if parsed.tzinfo is None:
        return parsed.to_pydatetime().replace(tzinfo=LONDON_TZ)
    return parsed.to_pydatetime().astimezone(LONDON_TZ)


def to_snapshot_id(ts: Optional[dt.datetime]) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S") if ts else ""


def infer_city(url: str) -> str:
    low = (url or "").lower()
    for city in ["southampton", "winchester", "guildford", "birmingham", "bristol"]:
        if city in low:
            return city.title()
    return "Southampton" if "unilife" in low else "Unknown"


def migrate_schema(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    for col in OUTPUT_COLUMNS:
        if col not in work.columns:
            work[col] = ""

    for col in OUTPUT_COLUMNS:
        work[col] = work[col].fillna("").astype(str)
    work = work.replace(to_replace=r"^\s*nan\s*$", value="", regex=True)

    work["Snapshot Date"] = work["Snapshot Date"].str.replace(" 00:00:00", "", regex=False)
    work["Run Timestamp"] = work["Run Timestamp"].str.strip()
    work["Operator"] = work["Operator"].str.strip()
    work["Property"] = work["Property"].str.strip()
    work["Room Name"] = work["Room Name"].str.strip()
    work["City"] = work["City"].str.strip()

    missing_city = work["City"].str.lower().isin(["", "unknown"])
    work.loc[missing_city, "City"] = work.loc[missing_city, "Source URL"].astype(str).apply(infer_city)

    missing_id = work["Snapshot ID"].fillna("").astype(str).str.strip() == ""
    work.loc[missing_id, "Snapshot ID"] = work.loc[missing_id, "Run Timestamp"].apply(lambda v: to_snapshot_id(parse_run_timestamp(v)))

    work["HALL ID"] = work.apply(
        lambda r: stable_hall_id(r["Operator"], r["Property"]) if normalize_space(str(r["Property"])) else "",
        axis=1,
    )
    work["ROOM ID"] = work.apply(
        lambda r: stable_room_id(r["Operator"], r["Property"], r["Room Name"])
        if normalize_space(str(r["Property"])) and normalize_space(str(r["Room Name"]))
        else "",
        axis=1,
    )

    missing_floor = work["Floor Level"].fillna("").astype(str).str.strip() == ""
    work.loc[missing_floor, "Floor Level"] = (
        work.loc[missing_floor, "Room Name"].astype(str) + " " + work.loc[missing_floor, "Contract Length"].astype(str)
    ).apply(parse_floor_level)

    missing_ay = work["Academic Year"].fillna("").astype(str).str.strip() == ""
    work.loc[missing_ay, "Academic Year"] = (
        work.loc[missing_ay, "Room Name"].astype(str)
        + " "
        + work.loc[missing_ay, "Contract Length"].astype(str)
        + " "
        + work.loc[missing_ay, "Source URL"].astype(str)
    ).apply(parse_academic_year)

    missing_source = work["Scrape Source"].fillna("").astype(str).str.strip() == ""
    work.loc[missing_source, "Scrape Source"] = "Local"

    return work[OUTPUT_COLUMNS]


def read_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    current = pd.read_excel(path, sheet_name=SHEET_NAME, engine="openpyxl")
    migrated = migrate_schema(current)
    return migrated


def save_history(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, index=False, sheet_name=SHEET_NAME, engine="openpyxl")


def migrate_workbook(path: Path) -> Dict[str, int]:
    if not path.exists():
        return {"before": 0, "after": 0}
    original = pd.read_excel(path, sheet_name=SHEET_NAME, engine="openpyxl")
    before = len(original)
    migrated = migrate_schema(original)
    save_history(path, migrated)
    return {"before": before, "after": len(migrated)}


def append_history(path: Path, new_rows: List[Dict[str, str]]) -> Tuple[int, int]:
    history = read_history(path)
    new_df = pd.DataFrame(new_rows, columns=OUTPUT_COLUMNS)
    combined = pd.concat([history, new_df], ignore_index=True)
    save_history(path, combined)
    return len(history), len(new_df)


async def safe_goto(page: Page, url: str, timeout: int = 90000) -> bool:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        await page.wait_for_timeout(1800)
        return True
    except Exception as exc:
        print(f"[WARN] Failed opening {url}: {exc}")
        return False


async def click_to_reveal(page: Page) -> None:
    for text in ["Accept", "Accept All", "Allow all", "View Rooms", "Rooms", "Book Now", "Show more", "Load more"]:
        try:
            loc = page.get_by_role("button", name=re.compile(rf"^{re.escape(text)}$", re.IGNORECASE))
            for i in range(min(await loc.count(), 8)):
                btn = loc.nth(i)
                if await btn.is_visible():
                    await btn.click(timeout=1000)
                    await page.wait_for_timeout(120)
        except Exception:
            pass


def make_row(
    snapshot_id: str,
    snapshot_date: str,
    run_timestamp: str,
    city: str,
    operator: str,
    property_name: str,
    room_name: str,
    floor_level: str,
    contract_length: str,
    academic_year: str,
    price: str,
    availability: str,
    source_url: str,
    scrape_source: str,
) -> Dict[str, str]:
    hall_id = stable_hall_id(operator, property_name) if normalize_space(property_name) else ""
    room_id = stable_room_id(operator, property_name, room_name) if hall_id and normalize_space(room_name) else ""
    return {
        "Snapshot ID": snapshot_id,
        "Snapshot Date": snapshot_date,
        "Run Timestamp": run_timestamp,
        "City": city,
        "Operator": operator,
        "HALL ID": hall_id,
        "Property": normalize_space(property_name),
        "ROOM ID": room_id,
        "Room Name": normalize_space(room_name),
        "Floor Level": normalize_space(floor_level),
        "Contract Length": normalize_space(contract_length),
        "Academic Year": normalize_space(academic_year),
        "Price": normalize_space(price),
        "Availability": normalize_space(availability),
        "Source URL": source_url,
        "Scrape Source": scrape_source,
    }


async def scrape_unilife_source(
    browser: Browser,
    src: Dict[str, str],
    snapshot_id: str,
    snapshot_date: str,
    run_timestamp: str,
    source_label: str,
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    page = await browser.new_page()
    rows: List[Dict[str, str]] = []
    try:
        if not await safe_goto(page, src["url"]):
            return [], f"{src['operator']} | {src['property']} (load failed)"
        await click_to_reveal(page)
        await page.wait_for_timeout(800)

        property_name = normalize_space(await page.locator("h1").first.inner_text()) if await page.locator("h1").count() else src["property"]
        body_text = await page.inner_text("body")
        academic_year = parse_academic_year(body_text)

        blocks = await page.evaluate(
            r"""
            () => {
              const blocks = [...document.querySelectorAll('.modal-content__inner')];
              return blocks.map(block => ({
                title: (block.querySelector('.title, h1, h2, h3, .top')?.innerText || '').replace(/\s+/g, ' ').trim(),
                base_price: (block.querySelector('.price')?.innerText || '').replace(/\s+/g, ' ').trim(),
                availability: (block.querySelector('.available')?.innerText || '').replace(/\s+/g, ' ').trim(),
                full_text: (block.innerText || '').replace(/\s+/g, ' ').trim(),
                books: [...block.querySelectorAll('.book')].map(b => (b.innerText || '').replace(/\s+/g, ' ').trim()).filter(Boolean),
              }));
            }
            """
        )

        for block in blocks:
            room_name = normalize_space(block.get("title", ""))
            room_name = re.sub(r"\bbook\s*now\b", "", room_name, flags=re.IGNORECASE)
            room_name = re.sub(r"[£Ł]?\s*\d{2,4}(?:[.,]\d{1,2})?\s*(?:pw|p/w|/\s*week|per\s*week)", "", room_name, flags=re.IGNORECASE)
            room_name = normalize_space(room_name)
            if not room_name:
                continue

            full_text = normalize_space(block.get("full_text", ""))
            availability = block.get("availability", "")
            if not availability:
                availability = "Sold Out" if re.search(r"sold out|fully booked", full_text, re.I) else ""
                if not availability and re.search(r"available", full_text, re.I):
                    availability = "Available"

            base_price = normalize_price(block.get("base_price", ""))
            floor_level = parse_floor_level(full_text)
            block_ay = parse_academic_year(full_text) or academic_year
            books = block.get("books", []) or []
            if books:
                for text in books:
                    contract = CONTRACT_RE.search(text)
                    rows.append(
                        make_row(
                            snapshot_id,
                            snapshot_date,
                            run_timestamp,
                            src["city"],
                            src["operator"],
                            property_name,
                            room_name,
                            parse_floor_level(text) or floor_level,
                            contract.group(0).upper() if contract else "",
                            parse_academic_year(text) or block_ay,
                            normalize_price(text) or base_price,
                            availability,
                            src["url"],
                            source_label,
                        )
                    )
            else:
                rows.append(
                    make_row(
                        snapshot_id,
                        snapshot_date,
                        run_timestamp,
                        src["city"],
                        src["operator"],
                        property_name,
                        room_name,
                        floor_level,
                        "",
                        block_ay,
                        base_price,
                        availability,
                        src["url"],
                        source_label,
                    )
                )
        return rows, None
    except Exception as exc:
        return [], f"{src['operator']} | {src['property']} ({exc})"
    finally:
        await page.close()


def _clean_room_name(text: str) -> str:
    value = normalize_space(text)
    value = re.sub(r"[£Ł]?\s*\d{2,4}(?:[.,]\d{1,2})?\s*(?:pw|p/w|/\s*week|per\s*week)", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\b(from|book now|contact us|available|sold out)\b", "", value, flags=re.IGNORECASE)
    return normalize_space(value)


async def scrape_generic_source(
    browser: Browser,
    src: Dict[str, str],
    snapshot_id: str,
    snapshot_date: str,
    run_timestamp: str,
    source_label: str,
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    page = await browser.new_page()
    rows: List[Dict[str, str]] = []
    try:
        if not await safe_goto(page, src["url"]):
            return [], f"{src['operator']} | {src['property']} (load failed)"

        await click_to_reveal(page)
        await page.wait_for_timeout(800)
        full_text = await page.inner_text("body")
        academic_year = parse_academic_year(full_text)
        property_name = src["property"]
        if await page.locator("h1").count():
            h1 = normalize_space(await page.locator("h1").first.inner_text())
            if h1:
                property_name = h1

        blocks = await page.evaluate(
            r"""
            () => {
              const selectors = ['article', 'li', 'tr', 'div[class*="room"]', 'div[class*="card"]', 'div[class*="price"]', 'section'];
              const nodes = [];
              selectors.forEach(s => document.querySelectorAll(s).forEach(n => nodes.push(n)));
              const out = [];
              for (const node of nodes) {
                const txt = (node.innerText || '').replace(/\s+/g, ' ').trim();
                if (!txt || txt.length < 12 || txt.length > 420) continue;
                if (/(room|studio|suite|ensuite|en-suite|flat|bed|from|week|sold out|available|floor|level)/i.test(txt)) {
                  out.push(txt);
                }
              }
              return Array.from(new Set(out)).slice(0, 300);
            }
            """
        )

        for block in blocks:
            text = normalize_space(block)
            if not text:
                continue
            price = normalize_price(text)
            roomish = bool(re.search(r"(room|studio|suite|ensuite|en-suite|flat|apartment|bedroom)", text, re.IGNORECASE))
            signal = bool(
                price
                or re.search(r"(sold out|available|waitlist|book now|per week|/week|pw)", text, re.IGNORECASE)
                or CONTRACT_RE.search(text)
                or FLOOR_RE.search(text)
            )
            if not roomish or not signal:
                continue

            room_name = ""
            pre_text = re.split(r"\b(Prices|Price|Features|Availability|Book now|Join waitlist)\b", text, maxsplit=1)[0]
            for part in re.split(r"\s{2,}| \| ", pre_text):
                cand = _clean_room_name(part)
                if re.search(r"(room|studio|suite|ensuite|en-suite|flat|bed)", cand, re.I):
                    room_name = cand
                    break
            if not room_name and price:
                room_name = src["property"]
            if not room_name:
                continue
            if len(room_name.split()) > 12 or len(room_name) > 90:
                continue

            availability = ""
            if re.search(r"sold out|fully booked", text, re.I):
                availability = "Sold Out"
            elif re.search(r"available|book now|limited availability", text, re.I):
                availability = "Available"

            contract = CONTRACT_RE.search(text)
            floor_level = parse_floor_level(text)
            ay = parse_academic_year(text) or academic_year

            rows.append(
                make_row(
                    snapshot_id,
                    snapshot_date,
                    run_timestamp,
                    src["city"],
                    src["operator"],
                    src["property"],
                    room_name,
                    floor_level,
                    contract.group(0).upper() if contract else "",
                    ay,
                    price,
                    availability,
                    src["url"],
                    source_label,
                )
            )

        # operator-specific safe fallback for Student Roost style pages.
        if src["operator"] == "Student Roost":
            compact = " ".join(full_text.split())
            for room_name, amount in re.findall(
                r"(En-suite Rooms|Studio Rooms)\s+from\s+(?:£|Ł)?\s*(\d{2,4}(?:[.,]\d{1,2})?)\s+per\s+week",
                compact,
                flags=re.IGNORECASE,
            ):
                rows.append(
                    make_row(
                        snapshot_id,
                        snapshot_date,
                        run_timestamp,
                        src["city"],
                        src["operator"],
                        src["property"],
                        room_name.title(),
                        "",
                        "",
                        academic_year,
                        f"£{amount} per week",
                        "",
                        src["url"],
                        source_label,
                    )
                )

        return rows, None
    except Exception as exc:
        return [], f"{src['operator']} | {src['property']} ({exc})"
    finally:
        await page.close()


def dedupe_current_run(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for row in rows:
        key = tuple(row.get(col, "") for col in OUTPUT_COLUMNS)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


async def run_scrape() -> int:
    if not should_run_for_london_9am():
        return 0

    migration = migrate_workbook(workbook_path())
    print(f"[INFO] Workbook migration complete: {migration['before']} -> {migration['after']} rows")

    now = dt.datetime.now(LONDON_TZ)
    snapshot_id = now.strftime("%Y-%m-%dT%H:%M:%S")
    snapshot_date = now.date().isoformat()
    run_timestamp = now.strftime("%Y-%m-%d %H:%M:%S %Z")
    source_label = scrape_source_label()

    all_sources: List[Dict[str, str]] = []
    for city, entries in CITY_SOURCES.items():
        for src in entries:
            all_sources.append({**src, "city": city})

    all_rows: List[Dict[str, str]] = []
    failed: List[str] = []
    operators = set()
    properties = set()
    cities = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            for src in all_sources:
                print(f"[SOURCE] {src['city']} | {src['operator']} | {src['property']} | {src['url']}")
                if src["scraper"] == "unilife":
                    rows, err = await scrape_unilife_source(browser, src, snapshot_id, snapshot_date, run_timestamp, source_label)
                else:
                    rows, err = await scrape_generic_source(browser, src, snapshot_id, snapshot_date, run_timestamp, source_label)
                if err:
                    failed.append(err)
                    print(f"[WARN] {err}")
                if rows:
                    all_rows.extend(rows)
                    print(f"[OK] collected {len(rows)} rows")
                elif not err:
                    msg = f"{src['operator']} | {src['property']} (no extractable room/price rows)"
                    failed.append(msg)
                    print(f"[WARN] {msg}")
        finally:
            await browser.close()

    all_rows = dedupe_current_run(all_rows)
    if not all_rows:
        print("[WARN] No rows scraped; workbook not updated.")
        return 1

    for row in all_rows:
        operators.add(row["Operator"])
        cities.add(row["City"])
        if row["Property"]:
            properties.add(row["Property"])

    prev, appended = append_history(workbook_path(), all_rows)
    with_price = sum(1 for r in all_rows if normalize_space(r.get("Price", "")))
    with_floor = sum(1 for r in all_rows if normalize_space(r.get("Floor Level", "")))
    with_ay = sum(1 for r in all_rows if normalize_space(r.get("Academic Year", "")))

    print("\nRun Summary")
    print(f"Snapshot ID: {snapshot_id}")
    print(f"Snapshot Date: {snapshot_date}")
    print(f"Run Timestamp: {run_timestamp}")
    print(f"Cities scraped: {', '.join(sorted(cities))}")
    print(f"Operators scraped: {', '.join(sorted(operators))}")
    print(f"Properties scraped: {len(properties)}")
    print(f"Rows appended: {appended}")
    print(f"Rows with prices: {with_price}")
    print(f"Rows without prices: {appended - with_price}")
    print(f"Rows with Floor Level: {with_floor}")
    print(f"Rows with Academic Year: {with_ay}")
    print(f"Blocked/failed sites: {len(failed)}")
    for item in failed[:30]:
        print(f" - {item}")
    print(f"Workbook path: {workbook_path()}")
    print(f"Historical total rows: {prev + appended}")
    return 0


def clean_existing_workbook(path: Path) -> int:
    mig = migrate_workbook(path)
    print("Workbook migration complete")
    print(f"Before rows: {mig['before']}")
    print(f"After rows: {mig['after']}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multi-city student accommodation pricing intelligence tracker")
    parser.add_argument("--clean-existing", action="store_true", help="Run only schema migration/backfill on historical workbook.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.clean_existing:
        raise SystemExit(clean_existing_workbook(workbook_path()))
    raise SystemExit(asyncio.run(run_scrape()))


if __name__ == "__main__":
    main()
