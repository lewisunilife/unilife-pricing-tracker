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

PRICE_RE = re.compile(r"(?:£)?\s*\d{2,4}(?:,\d{3})*(?:\.\d{1,2})?")
CONTRACT_RE = re.compile(r"\b\d{1,2}\s*(?:weeks?|months?)\b", re.IGNORECASE)
FLOOR_RE = re.compile(
    r"\b(Ground(?:\s*Floor)?|Lower Ground|Upper Ground|Basement|Level\s*\d+|Floors?\s*\d+\s*-\s*\d+|Floor\s*\d+|\d+(?:st|nd|rd|th)\s*Floor|First|Second|Third|Fourth|Fifth|Sixth|Seventh|Eighth|Ninth|Tenth(?:\s*Floor)?)\b",
    re.IGNORECASE,
)
ACADEMIC_YEAR_RE = re.compile(
    r"\b(?:AY\s*)?((?:20)?\d{2})\s*[/\-]\s*((?:20)?\d{2})\b",
    re.IGNORECASE,
)
INCENTIVE_RE = re.compile(
    r"(plus\s+bookings\s+get\s+free\s+annual\s+bus\s+pass|book\s+today\s*(?:&|and)?\s*get\s+a?\s*free\s+kitchen\s*(?:&|and)\s*bedding\s+pack\s+worth\s*[£Ł]?\s*\d+|kitchen\s*(?:&|and)\s*bedding\s+pack\s+worth\s*[£Ł]?\s*\d+|£\s*\d+(?:[.,]\d{1,2})?\s*cashback|cashback|free\s+annual\s+bus\s+pass|free\s+bus\s+pass|bedding\s+pack(?:\s+included)?|kitchen\s+pack(?:\s+included|(?:\s+worth\s+£?\s*\d+)?)?|voucher)",
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
    value = normalize_space(text)
    for bad in ("Â£", "Ã‚Â£", "Å", "Ł"):
        value = value.replace(bad, "£")
    return value


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


def parse_price_to_weekly_numeric(text: Any) -> Optional[float]:
    if text is None:
        return None
    if isinstance(text, (int, float)) and not pd.isna(text):
        return round(float(text), 2)

    t = normalize_currency(text).lower()
    if not t:
        return None

    period_weekly = bool(re.search(r"\b(pw|p/w|per\s*week|weekly|/week)\b", t))
    period_monthly = bool(re.search(r"\b(pcm|per\s*month|monthly|/month)\b", t))
    if period_weekly and period_monthly:
        return None

    m = PRICE_RE.search(t)
    if not m:
        return None
    raw = m.group(0).replace("£", "").replace(",", "").strip()
    raw = re.sub(r"[^\d.]", "", raw)
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None

    if period_weekly:
        return round(value, 2)
    if period_monthly:
        return round((value * 12) / 52, 2)
    return None


def extract_contract(text: str) -> str:
    m = CONTRACT_RE.search(normalize_space(text))
    return m.group(0).upper() if m else ""


def _num_to_floor_word(num: int) -> str:
    words = {
        0: "Ground",
        1: "First",
        2: "Second",
        3: "Third",
        4: "Fourth",
        5: "Fifth",
        6: "Sixth",
        7: "Seventh",
        8: "Eighth",
        9: "Ninth",
        10: "Tenth",
    }
    return words.get(num, "")


def normalise_floor_level(text: str) -> str:
    t = normalize_space(text)
    if not t:
        return ""

    # ranges like "Floors 3-5"
    rng = re.search(r"\b(?:floors?|levels?)\s*(\d+)\s*(?:-|to)\s*(\d+)\b", t, re.IGNORECASE)
    if rng:
        a, b = int(rng.group(1)), int(rng.group(2))
        wa, wb = _num_to_floor_word(a), _num_to_floor_word(b)
        return f"{wa} to {wb}" if wa and wb else ""

    # ordinals and simple floor references
    if re.search(r"\b(?:lower\s+)?ground(?:\s*floor)?\b", t, re.IGNORECASE):
        return "Ground"
    m_range_words = re.search(
        r"\b(ground|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)\s*(?:to|-)\s*(ground|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)\b",
        t,
        re.IGNORECASE,
    )
    if m_range_words:
        return f"{m_range_words.group(1).title()} to {m_range_words.group(2).title()}"

    m_num = re.search(r"\b(?:floor|level)?\s*(\d+)(?:st|nd|rd|th)?(?:\s*floor)?\b", t, re.IGNORECASE)
    if m_num:
        word = _num_to_floor_word(int(m_num.group(1)))
        if word:
            return word

    m_word = re.search(
        r"\b(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)\s*floor\b",
        t,
        re.IGNORECASE,
    )
    if m_word:
        return m_word.group(1).title()

    m = FLOOR_RE.search(t)
    if not m:
        return ""
    token = normalize_space(m.group(1)).replace(" Floor", "")
    return token.title()


def normalise_academic_year(text: str) -> str:
    m = ACADEMIC_YEAR_RE.search(normalize_space(text))
    if not m:
        return ""
    a = normalize_space(m.group(1))
    b = normalize_space(m.group(2))
    if len(a) == 2:
        a = f"20{a}"
    if len(b) == 4:
        b = b[2:]
    if len(b) == 1:
        b = f"0{b}"
    if len(b) == 2 and len(a) == 4 and a.isdigit() and b.isdigit():
        return f"{a}/{b}"
    return ""


def extract_and_normalise_incentives(*texts: str) -> str:
    joined = " ".join(normalize_currency(t) for t in texts if normalize_space(t))
    found = []
    for m in INCENTIVE_RE.finditer(joined):
        token = normalize_currency(m.group(0))
        if token and token.lower() not in [x.lower() for x in found]:
            found.append(token)
    # Drop shorter duplicates when a longer phrase already contains them.
    out: List[str] = []
    for token in found:
        low = token.lower()
        if any(low in existing.lower() and low != existing.lower() for existing in found):
            continue
        out.append(token)
    return " | ".join(out)


def extract_price(text: Any) -> Optional[float]:
    return parse_price_to_weekly_numeric(text)


def extract_floor(text: str) -> str:
    return normalise_floor_level(text)


def extract_academic_year(text: str) -> str:
    return normalise_academic_year(text)


def extract_incentives(*texts: str) -> str:
    return extract_and_normalise_incentives(*texts)


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
            work[col] = pd.NA
    text_cols = [c for c in OUTPUT_COLUMNS if c != "Price"]
    for col in text_cols:
        work[col] = work[col].apply(normalize_space)
    work = work.replace(to_replace=r"^\s*nan\s*$", value="", regex=True)

    work["Snapshot Date"] = work["Snapshot Date"].str.replace(" 00:00:00", "", regex=False)
    work["Room Name"] = work["Room Name"].apply(clean_room_name)
    work["Floor Level"] = work["Floor Level"].apply(normalise_floor_level)
    work["Academic Year"] = work["Academic Year"].apply(normalise_academic_year)
    work["Incentives"] = work["Incentives"].apply(extract_and_normalise_incentives)
    work["Price"] = work["Price"].apply(parse_price_to_weekly_numeric)

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
    ).apply(normalise_floor_level)

    ay_missing = work["Academic Year"].apply(normalize_space) == ""
    work.loc[ay_missing, "Academic Year"] = (
        work.loc[ay_missing, "Room Name"] + " " + work.loc[ay_missing, "Contract Length"] + " " + work.loc[ay_missing, "Source URL"]
    ).apply(normalise_academic_year)

    inc_missing = work["Incentives"].apply(normalize_space) == ""
    # Only backfill incentives from existing room/contract text when explicit keywords exist.
    work.loc[inc_missing, "Incentives"] = (
        work.loc[inc_missing, "Room Name"] + " " + work.loc[inc_missing, "Contract Length"]
    ).apply(lambda x: extract_and_normalise_incentives(x))

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
    price: Any,
    incentives: str,
    availability: str,
    source_url: str,
    scrape_source: str,
) -> Dict[str, Any]:
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
        "Floor Level": normalise_floor_level(floor_level),
        "Contract Length": normalize_space(contract_length),
        "Academic Year": normalise_academic_year(academic_year),
        "Price": parse_price_to_weekly_numeric(price),
        "Incentives": extract_and_normalise_incentives(incentives),
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
        promo_texts = await page.evaluate(
            r"""
            () => {
              const selectors = ['.banner', '.promo', '.offer', '.announcement', '.hero', '[class*="promo"]', '[class*="offer"]', '[class*="banner"]'];
              const out = [];
              selectors.forEach(s => document.querySelectorAll(s).forEach(n => {
                const t = (n.innerText || '').replace(/\s+/g, ' ').trim();
                if (t && t.length < 260) out.push(t);
              }));
              return out.slice(0, 120);
            }
            """
        )
        property_incentives = extract_incentives(page_text, " ".join(promo_texts))
        card_meta = await page.evaluate(
            r"""
            () => {
              const out = [];
              const cards = [...document.querySelectorAll('[class*="room"], .room, .room-card, article')];
              for (const c of cards) {
                const title = (c.querySelector('h2,h3,h4,.title,[class*="title"]')?.innerText || '').replace(/\s+/g, ' ').trim();
                const text = (c.innerText || '').replace(/\s+/g, ' ').trim();
                if (title && text) out.push({title, text});
              }
              return out.slice(0, 400);
            }
            """
        )
        room_card_incentives: Dict[str, str] = {}
        for c in card_meta:
            room_card_incentives[normalize_key(c.get("title", ""))] = extract_incentives(c.get("text", ""))

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
            room_level_incentives = room_card_incentives.get(normalize_key(room_name), "")

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
                    incentives = extract_incentives(tile_text, col_incentives, room_level_incentives, modal_incentives, property_incentives)
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
                        extract_incentives(room_level_incentives, modal_incentives, property_incentives),
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
                r"(En-suite Rooms|Studio Rooms)\s+from\s+(?:£)?\s*(\d{2,4}(?:[.,]\d{1,2})?)\s+per\s+week",
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


def dedupe_current_run(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for row in rows:
        key_parts = []
        for col in OUTPUT_COLUMNS:
            value = row.get(col, "")
            if col == "Price":
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    key_parts.append("")
                else:
                    key_parts.append(f"{float(value):.2f}")
            else:
                key_parts.append(normalize_space(value))
        key = tuple(key_parts)
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
    all_rows: List[Dict[str, Any]] = []
    failed: List[str] = []
    coverage: List[Dict[str, str]] = []
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
                    low = err.lower()
                    status = "blocked/failed"
                    if "load failed" in low:
                        status = "page unavailable"
                    elif any(x in low for x in ["captcha", "forbidden", "access denied", "403", "401"]):
                        status = "blocked"
                    coverage.append(
                        {
                            "city": src["city"],
                            "operator": src["operator"],
                            "property": src["property"],
                            "status": status,
                            "reason": err,
                        }
                    )
                    print(f"[WARN] {err}")
                elif not rows:
                    msg = f"{src['operator']} | {src['property']} (no extractable room rows)"
                    failed.append(msg)
                    coverage.append(
                        {
                            "city": src["city"],
                            "operator": src["operator"],
                            "property": src["property"],
                            "status": "selector mismatch/no room data",
                            "reason": "scraped but no extractable room rows",
                        }
                    )
                    print(f"[WARN] {msg}")
                else:
                    print(f"[OK] collected {len(rows)} rows")
                    coverage.append(
                        {
                            "city": src["city"],
                            "operator": src["operator"],
                            "property": src["property"],
                            "status": "scraped successfully with rows",
                            "reason": str(len(rows)),
                        }
                    )
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

    with_price = sum(1 for r in all_rows if r.get("Price") is not None and not (isinstance(r.get("Price"), float) and pd.isna(r.get("Price"))))
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


