import argparse
import asyncio
import datetime as dt
import hashlib
import os
import re
import sys
import unicodedata
from pathlib import Path
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
    "Incentives",
    "Availability",
    "Source URL",
    "Scrape Source",
]

PRICE_RE = re.compile(r"(?:£|Ł)?\s*\d{2,4}(?:[.,]\d{1,2})?\s*(?:pw|p/w|/\s*week|per\s*week|weekly)", re.IGNORECASE)
CONTRACT_RE = re.compile(r"\b\d{1,2}\s*(?:weeks?|months?)\b", re.IGNORECASE)
FLOOR_RE = re.compile(
    r"\b(Ground Floor|Lower Ground|Upper Ground|Basement|Level\s*\d+|Floors?\s*\d+\s*-\s*\d+|Floor\s*\d+|\d+(?:st|nd|rd|th)\s*Floor|First Floor|Second Floor|Third Floor|Fourth Floor|Fifth Floor)\b",
    re.IGNORECASE,
)
ACADEMIC_YEAR_RE = re.compile(r"\b(20\d{2}\s*[/\-]\s*(?:20)?\d{2})\b")
INCENTIVE_RE = re.compile(
    r"(£\s*\d+(?:[.,]\d{1,2})?\s*cashback|cashback|free\s+annual\s+bus\s+pass|free\s+bus\s+pass|bedding\s+pack(?:\s+included)?|kitchen\s+pack(?:\s+included|(?:\s+worth\s+£?\d+)?)?|voucher|discount)",
    re.IGNORECASE,
)

ROOM_NAME_STRIP_RE = re.compile(
    r"(£\s*\d{2,4}(?:[.,]\d{1,2})?\s*(?:pw|p/w|/ ?week|per week|weekly)|\bview room\b|\bbook now\b|\bjoin waitlist\b|\bavailable\b|\bsold out\b|\bfrom\b|\bcontact us\b|\b\d{1,2}\s*weeks?\b)",
    re.IGNORECASE,
)


def normalize_space(text: Any) -> str:
    if text is None:
        return ""
    if isinstance(text, float) and pd.isna(text):
        return ""
    value = str(text).strip()
    if value.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", value).strip()


def normalize_key(text: str) -> str:
    value = unicodedata.normalize("NFKD", normalize_space(text).lower())
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value


def normalize_currency(text: str) -> str:
    return normalize_space(text).replace("Ł", "£").replace("Â£", "£")


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


def clean_room_name(raw: str) -> str:
    text = normalize_currency(raw)
    text = ROOM_NAME_STRIP_RE.sub(" ", text)
    text = INCENTIVE_RE.sub(" ", text)
    text = re.sub(r"\b(availability|features|prices?|details?|offer|review|google)\b", " ", text, flags=re.IGNORECASE)
    text = normalize_space(text)
    if len(text) > 90:
        return ""
    return text


def extract_price(text: str) -> str:
    t = normalize_currency(text)
    m = PRICE_RE.search(t)
    if not m:
        return ""
    val = normalize_space(m.group(0))
    if "£" not in val:
        val = f"£{val}"
    return val


def extract_contract(text: str) -> str:
    m = CONTRACT_RE.search(normalize_space(text))
    return m.group(0).upper() if m else ""


def extract_floor(text: str) -> str:
    m = FLOOR_RE.search(normalize_space(text))
    return normalize_space(m.group(1)) if m else ""


def extract_academic_year(text: str) -> str:
    m = ACADEMIC_YEAR_RE.search(normalize_space(text))
    if not m:
        return ""
    return normalize_space(m.group(1)).replace("-", "/")


def extract_incentives(*texts: str) -> str:
    joined = " ".join(normalize_currency(t) for t in texts if normalize_space(t))
    found = []
    for m in INCENTIVE_RE.finditer(joined):
        token = normalize_space(m.group(0))
        if token and token.lower() not in [x.lower() for x in found]:
            found.append(token)
    return " | ".join(found)


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
    now = dt.datetime.now(LONDON_TZ)
    if now.hour == 9 and now.minute == 0:
        return True
    print(f"[INFO] Skipping run: Europe/London time is {now.strftime('%Y-%m-%d %H:%M:%S %Z')} (required: 09:00).")
    return False


def parse_run_timestamp(value: Any) -> Optional[dt.datetime]:
    text = normalize_space(value)
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
    low = normalize_space(url).lower()
    for city in ["southampton", "winchester", "guildford", "birmingham", "bristol"]:
        if city in low:
            return city.title()
    if "unilife-high-street" in low:
        return "Winchester"
    if "riverside-house" in low:
        return "Guildford"
    return "Unknown"


def migrate_schema(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    for col in OUTPUT_COLUMNS:
        if col not in work.columns:
            work[col] = ""
    for col in OUTPUT_COLUMNS:
        work[col] = work[col].fillna("").astype(str)
    work = work.replace(to_replace=r"^\s*nan\s*$", value="", regex=True)

    work["Snapshot Date"] = work["Snapshot Date"].str.replace(" 00:00:00", "", regex=False)
    work["Run Timestamp"] = work["Run Timestamp"].apply(normalize_space)
    work["City"] = work["City"].apply(normalize_space)
    work["Operator"] = work["Operator"].apply(normalize_space)
    work["Property"] = work["Property"].apply(normalize_space)
    work["Room Name"] = work["Room Name"].apply(clean_room_name)
    work["Price"] = work["Price"].apply(normalize_currency)
    work["Incentives"] = work["Incentives"].apply(normalize_space)

    missing_city = work["City"].str.lower().isin(["", "unknown"])
    work.loc[missing_city, "City"] = work.loc[missing_city, "Source URL"].apply(infer_city)

    missing_sid = work["Snapshot ID"].apply(normalize_space) == ""
    work.loc[missing_sid, "Snapshot ID"] = work.loc[missing_sid, "Run Timestamp"].apply(lambda v: to_snapshot_id(parse_run_timestamp(v)))

    # Stable IDs for historical rows.
    work["HALL ID"] = work.apply(
        lambda r: stable_hall_id(r["Operator"], r["Property"]) if normalize_space(r["Property"]) else "",
        axis=1,
    )
    work["ROOM ID"] = work.apply(
        lambda r: stable_room_id(r["Operator"], r["Property"], r["Room Name"])
        if normalize_space(r["Property"]) and normalize_space(r["Room Name"])
        else "",
        axis=1,
    )

    # Safe backfills only.
    floor_missing = work["Floor Level"].apply(normalize_space) == ""
    work.loc[floor_missing, "Floor Level"] = (
        work.loc[floor_missing, "Room Name"] + " " + work.loc[floor_missing, "Contract Length"]
    ).apply(extract_floor)

    ay_missing = work["Academic Year"].apply(normalize_space) == ""
    work.loc[ay_missing, "Academic Year"] = (
        work.loc[ay_missing, "Room Name"] + " " + work.loc[ay_missing, "Contract Length"] + " " + work.loc[ay_missing, "Source URL"]
    ).apply(extract_academic_year)

    inc_missing = work["Incentives"].apply(normalize_space) == ""
    # Only backfill incentives from existing room/contract text when explicit keywords exist.
    work.loc[inc_missing, "Incentives"] = (
        work.loc[inc_missing, "Room Name"] + " " + work.loc[inc_missing, "Contract Length"]
    ).apply(lambda x: extract_incentives(x))

    source_missing = work["Scrape Source"].apply(normalize_space) == ""
    work.loc[source_missing, "Scrape Source"] = "Local"

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


def append_history(path: Path, rows: List[Dict[str, str]]) -> Tuple[int, int]:
    history = read_history(path)
    run_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    merged = pd.concat([history, run_df], ignore_index=True)
    save_history(path, merged)
    return len(history), len(run_df)


async def safe_goto(page: Page, url: str, timeout: int = 90000) -> bool:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        await page.wait_for_timeout(1600)
        return True
    except Exception as exc:
        print(f"[WARN] Failed opening {url}: {exc}")
        return False


async def click_to_reveal(page: Page) -> None:
    for label in ["Accept", "Accept All", "Allow all", "View Rooms", "Rooms", "Book Now", "Show more", "Load more"]:
        try:
            loc = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
            for i in range(min(await loc.count(), 6)):
                btn = loc.nth(i)
                if await btn.is_visible():
                    await btn.click(timeout=800)
                    await page.wait_for_timeout(100)
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
    incentives: str,
    availability: str,
    source_url: str,
    scrape_source: str,
) -> Dict[str, str]:
    prop = normalize_space(property_name)
    room = clean_room_name(room_name)
    hall_id = stable_hall_id(operator, prop) if prop else ""
    room_id = stable_room_id(operator, prop, room) if prop and room else ""
    return {
        "Snapshot ID": snapshot_id,
        "Snapshot Date": snapshot_date,
        "Run Timestamp": run_timestamp,
        "City": city,
        "Operator": operator,
        "HALL ID": hall_id,
        "Property": prop,
        "ROOM ID": room_id,
        "Room Name": room,
        "Floor Level": normalize_space(floor_level),
        "Contract Length": normalize_space(contract_length),
        "Academic Year": normalize_space(academic_year),
        "Price": normalize_currency(price),
        "Incentives": normalize_space(incentives),
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
    scrape_source: str,
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    page = await browser.new_page()
    rows: List[Dict[str, str]] = []
    try:
        if not await safe_goto(page, src["url"]):
            return [], f"{src['operator']} | {src['property']} (load failed)"
        await click_to_reveal(page)
        await page.wait_for_timeout(1200)

        property_name = src["property"]
        if await page.locator("h1").count():
            property_name = normalize_space(await page.locator("h1").first.inner_text()).replace("\n", " ")
        page_text = await page.inner_text("body")
        page_ay = extract_academic_year(page_text)

        blocks = await page.evaluate(
            r"""
            () => {
              const out = [];
              const modals = [...document.querySelectorAll('.modal-content__inner')];
              for (const modal of modals) {
                const title = (modal.querySelector('.title, h1, h2, h3, .top')?.innerText || '').replace(/\s+/g, ' ').trim();
                const availability = (modal.querySelector('.available')?.innerText || '').replace(/\s+/g, ' ').trim();
                const modalText = (modal.innerText || '').replace(/\s+/g, ' ').trim();
                const columns = [...modal.querySelectorAll('.contracts-column')].map(col => ({
                  header: (col.querySelector('.top, h3, h4, p, strong')?.innerText || '').replace(/\s+/g, ' ').trim(),
                  tiles: [...col.querySelectorAll('.book')].map(tile => (tile.innerText || '').replace(/\s+/g, ' ').trim())
                }));
                out.push({title, availability, modalText, columns});
              }
              return out;
            }
            """
        )

        for block in blocks:
            room_name = clean_room_name(block.get("title", ""))
            if not room_name:
                continue
            modal_text = normalize_space(block.get("modalText", ""))
            modal_avail = normalize_space(block.get("availability", ""))
            if not modal_avail:
                if re.search(r"sold out|fully booked", modal_text, re.I):
                    modal_avail = "Sold Out"
                elif re.search(r"available|last few", modal_text, re.I):
                    modal_avail = "Available"

            modal_ay = extract_academic_year(modal_text) or page_ay
            modal_incentives = extract_incentives(modal_text)
            base_price = extract_price(modal_text)

            columns = block.get("columns", [])
            wrote_tile = False
            for col in columns:
                col_header = normalize_space(col.get("header", ""))
                col_contract = extract_contract(col_header)
                col_incentives = extract_incentives(col_header)
                for tile in col.get("tiles", []):
                    tile_text = normalize_space(tile)
                    price = extract_price(tile_text) or extract_price(col_header) or base_price
                    contract = extract_contract(col_header) or extract_contract(tile_text)
                    floor = extract_floor(tile_text) or extract_floor(modal_text)
                    ay = extract_academic_year(tile_text) or modal_ay
                    incentives = extract_incentives(tile_text, col_incentives, modal_incentives)
                    availability = "Sold Out" if re.search(r"sold out|fully booked", tile_text, re.I) else modal_avail

                    rows.append(
                        make_row(
                            snapshot_id,
                            snapshot_date,
                            run_timestamp,
                            src["city"],
                            src["operator"],
                            property_name,
                            room_name,
                            floor,
                            contract,
                            ay,
                            price,
                            incentives,
                            availability,
                            src["url"],
                            scrape_source,
                        )
                    )
                    wrote_tile = True

            if not wrote_tile:
                rows.append(
                    make_row(
                        snapshot_id,
                        snapshot_date,
                        run_timestamp,
                        src["city"],
                        src["operator"],
                        property_name,
                        room_name,
                        extract_floor(modal_text),
                        extract_contract(modal_text),
                        modal_ay,
                        base_price,
                        modal_incentives,
                        modal_avail,
                        src["url"],
                        scrape_source,
                    )
                )

        return rows, None
    except Exception as exc:
        return [], f"{src['operator']} | {src['property']} ({exc})"
    finally:
        await page.close()


def extract_lines(page_text: str) -> List[str]:
    return [normalize_space(x) for x in page_text.splitlines() if normalize_space(x)]


async def scrape_non_unilife_source(
    browser: Browser,
    src: Dict[str, str],
    snapshot_id: str,
    snapshot_date: str,
    run_timestamp: str,
    scrape_source: str,
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    page = await browser.new_page()
    rows: List[Dict[str, str]] = []
    try:
        if not await safe_goto(page, src["url"]):
            return [], f"{src['operator']} | {src['property']} (load failed)"

        await click_to_reveal(page)
        await page.wait_for_timeout(900)
        body = await page.inner_text("body")
        lines = extract_lines(body)
        page_ay = extract_academic_year(body)
        property_name = src["property"]
        if await page.locator("h1").count():
            maybe = clean_room_name(await page.locator("h1").first.inner_text())
            if maybe:
                property_name = maybe

        # Operator-specific tighter parsing rules.
        op = src["operator"]
        if op == "Student Roost":
            compact = " ".join(lines)
            for room, amt in re.findall(
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
                        op,
                        src["property"],
                        room.title(),
                        "",
                        "",
                        page_ay,
                        f"£{amt} per week",
                        extract_incentives(compact),
                        "",
                        src["url"],
                        scrape_source,
                    )
                )
            return rows, None

        if op == "Yugo":
            for i, line in enumerate(lines):
                if line.upper() == line and len(line) <= 45 and re.search(r"(STUDIO|SUITE|ROOM)", line, re.I):
                    room = clean_room_name(line.title())
                    if not room:
                        continue
                    look = " ".join(lines[i : min(i + 6, len(lines))])
                    rows.append(
                        make_row(
                            snapshot_id,
                            snapshot_date,
                            run_timestamp,
                            src["city"],
                            op,
                            src["property"],
                            room,
                            extract_floor(look),
                            extract_contract(look),
                            extract_academic_year(look) or page_ay,
                            extract_price(look),
                            extract_incentives(look),
                            "Sold Out" if re.search(r"sold out", look, re.I) else "",
                            src["url"],
                            scrape_source,
                        )
                    )
            return rows, None

        # Strict card-style parsing for other operators.
        candidates = await page.evaluate(
            r"""
            () => {
              const sels = ['article', '[class*="room-card"]', '[class*="room"]', '[class*="tile"]', '[class*="card"]'];
              const nodes = [];
              sels.forEach(s => document.querySelectorAll(s).forEach(n => nodes.push(n)));
              const out = [];
              for (const n of nodes) {
                const title = (n.querySelector('h2,h3,h4,.title,[class*="title"]')?.innerText || '').replace(/\s+/g, ' ').trim();
                const price = (n.querySelector('[class*="price"], .price')?.innerText || '').replace(/\s+/g, ' ').trim();
                const avail = (n.querySelector('[class*="avail"], .available, .sold-out')?.innerText || '').replace(/\s+/g, ' ').trim();
                const txt = (n.innerText || '').replace(/\s+/g, ' ').trim();
                if (!txt || txt.length < 20 || txt.length > 420) continue;
                out.push({title, price, avail, txt});
              }
              return out.slice(0, 400);
            }
            """
        )

        banned_title = re.compile(r"\b(review|google|do you|what is|faq|airport|printing|facilities|terms|privacy|can i|is there|how do i)\b", re.IGNORECASE)
        roomish_re = re.compile(r"(room|studio|suite|ensuite|en-suite|flat|apartment|bedroom|ensuites?)", re.IGNORECASE)
        for c in candidates:
            title = clean_room_name(c.get("title", ""))
            text = normalize_space(c.get("txt", ""))
            if not title:
                for part in re.split(r"\s{2,}| \| ", text):
                    cand = clean_room_name(part)
                    if roomish_re.search(cand):
                        title = cand
                        break
            if not title:
                continue
            if banned_title.search(title):
                continue
            if "?" in title:
                continue
            if not roomish_re.search(title):
                continue
            if len(title.split()) > 10:
                continue

            price = extract_price(c.get("price", "")) or extract_price(text)
            availability = normalize_space(c.get("avail", ""))
            if not availability:
                if re.search(r"sold out|fully booked", text, re.I):
                    availability = "Sold Out"
                elif re.search(r"available|last few", text, re.I):
                    availability = "Available"

            rows.append(
                make_row(
                    snapshot_id,
                    snapshot_date,
                    run_timestamp,
                    src["city"],
                    op,
                    src["property"],
                    title,
                    extract_floor(text),
                    extract_contract(text),
                    extract_academic_year(text) or page_ay,
                    price,
                    extract_incentives(text),
                    availability,
                    src["url"],
                    scrape_source,
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

    mig = migrate_workbook(workbook_path())
    print(f"[INFO] Workbook migration complete: {mig['before']} -> {mig['after']} rows")

    now = dt.datetime.now(LONDON_TZ)
    snapshot_id = now.strftime("%Y-%m-%dT%H:%M:%S")
    snapshot_date = now.date().isoformat()
    run_timestamp = now.strftime("%Y-%m-%d %H:%M:%S %Z")
    source_label = scrape_source_label()

    sources = [{**s, "city": city} for city, entries in CITY_SOURCES.items() for s in entries]
    all_rows: List[Dict[str, str]] = []
    failed: List[str] = []
    cities, operators, properties = set(), set(), set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            for src in sources:
                print(f"[SOURCE] {src['city']} | {src['operator']} | {src['property']} | {src['url']}")
                if src.get("scraper") == "unilife":
                    rows, err = await scrape_unilife_source(browser, src, snapshot_id, snapshot_date, run_timestamp, source_label)
                else:
                    rows, err = await scrape_non_unilife_source(browser, src, snapshot_id, snapshot_date, run_timestamp, source_label)
                if err:
                    failed.append(err)
                    print(f"[WARN] {err}")
                elif not rows:
                    msg = f"{src['operator']} | {src['property']} (no extractable room rows)"
                    failed.append(msg)
                    print(f"[WARN] {msg}")
                else:
                    print(f"[OK] collected {len(rows)} rows")
                    all_rows.extend(rows)
        finally:
            await browser.close()

    all_rows = [r for r in dedupe_current_run(all_rows) if normalize_space(r["Room Name"]) or normalize_space(r["Price"])]
    if not all_rows:
        print("[WARN] No rows scraped; workbook not updated.")
        return 1

    prev, appended = append_history(workbook_path(), all_rows)
    for r in all_rows:
        cities.add(r["City"])
        operators.add(r["Operator"])
        if normalize_space(r["Property"]):
            properties.add(r["Property"])

    with_price = sum(1 for r in all_rows if normalize_space(r["Price"]))
    with_floor = sum(1 for r in all_rows if normalize_space(r["Floor Level"]))
    with_ay = sum(1 for r in all_rows if normalize_space(r["Academic Year"]))
    with_inc = sum(1 for r in all_rows if normalize_space(r["Incentives"]))

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
    print(f"Rows with Incentives: {with_inc}")
    print(f"Blocked/failed sites: {len(failed)}")
    for f in failed[:40]:
        print(f" - {f}")
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
