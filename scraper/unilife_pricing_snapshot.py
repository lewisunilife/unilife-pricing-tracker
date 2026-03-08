import argparse
import asyncio
import datetime as dt
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from playwright.async_api import Browser, Page, TimeoutError, async_playwright

LONDON_TZ = ZoneInfo("Europe/London")
SHEET_NAME = "All Pricing"

OUTPUT_COLUMNS = [
    "Snapshot ID",
    "Snapshot Date",
    "Run Timestamp",
    "City",
    "Operator",
    "Property",
    "Room Name",
    "Contract Length",
    "Price",
    "Availability",
    "Source URL",
    "Scrape Source",
]

CITY_CONFIG: Dict[str, Dict[str, Any]] = {
    # Multi-city ready structure. Add more cities here over time.
    "Southampton": {
        "sources": [
            {
                "operator": "Unilife",
                "type": "unilife_property",
                "url": "https://www.unilife.co.uk/student-accommodation/southampton/bargate-house",
            },
            {
                "operator": "Unilife",
                "type": "unilife_property",
                "url": "https://www.unilife.co.uk/student-accommodation/southampton/castle-way",
            },
            {
                "operator": "Yugo",
                "type": "yugo_property",
                "url": "https://yugo.com/en-us/global/united-kingdom/southampton/austen-house",
            },
            {
                "operator": "Yugo",
                "type": "yugo_property",
                "url": "https://yugo.com/en-us/global/united-kingdom/southampton/crescent-place",
            },
            {
                "operator": "Student Roost",
                "type": "student_roost_property",
                "url": "https://www.studentroost.co.uk/locations/southampton/vincent-place",
            },
            {
                "operator": "Unite Students",
                "type": "unite_city",
                "url": "https://www.unitestudents.com/student-accommodation/southampton",
            },
        ]
    }
}

UNILIFE_FALLBACK_URL_BY_SLUG = {
    "bargate-house": "https://unilife.co.uk/property/unilife-bargate-house/",
    "castle-way": "https://unilife.co.uk/property/castle-way/",
    "sparkford-house": "https://unilife.co.uk/property/unilife-sparkford-house/",
    "high-street": "https://unilife.co.uk/property/unilife-high-street/",
    "riverside-house": "https://unilife.co.uk/property/unilife-riverside-house/",
}

PRICE_RE = re.compile(r"(?:£|Ł)?\s*\d{2,4}(?:[.,]\d{1,2})?\s*(?:pw|p/w|/\s*week|per\s*week)", re.IGNORECASE)
CONTRACT_RE = re.compile(r"\d{1,2}\s*(?:weeks?|months?)", re.IGNORECASE)


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def workbook_path() -> Path:
    return repo_root() / "data" / "Unilife_Pricing_Snapshot.xlsx"


def scrape_source_label() -> str:
    return "GitHub Actions" if os.getenv("GITHUB_ACTIONS", "").lower() == "true" else "Local"


def infer_city_from_url(url: str) -> str:
    low = (url or "").lower()
    # Handle legacy Unilife property slugs that do not include city path segments.
    if "bargate-house" in low or "castle-way" in low:
        return "Southampton"
    if "sparkford-house" in low or "high-street" in low:
        return "Winchester"
    if "riverside-house" in low:
        return "Guildford"
    for city in ["southampton", "winchester", "guildford", "birmingham", "bristol"]:
        if f"/{city}" in low or city in low:
            return city.title()
    return "Unknown"


def infer_operator_from_url(url: str) -> str:
    low = (url or "").lower()
    if "unilife" in low:
        return "Unilife"
    if "yugo" in low:
        return "Yugo"
    if "studentroost" in low:
        return "Student Roost"
    if "unitestudents" in low:
        return "Unite Students"
    return "Unknown"


def normalize_price(text: str) -> str:
    raw = normalize_space(text)
    if not raw:
        return ""
    m = PRICE_RE.search(raw)
    if m:
        token = normalize_space(m.group(0)).replace("Ł", "£")
        if "£" not in token:
            token = f"£{token}"
        token = re.sub(r"\s+", " ", token).replace(" /", "/")
        return token
    digits = re.search(r"\d{2,4}(?:[.,]\d{1,2})?", raw)
    if digits and re.search(r"week|pw|p/w", raw, re.IGNORECASE):
        suffix = "per week" if "per week" in raw.lower() else "pw"
        return f"£{digits.group(0)} {suffix}" if suffix == "per week" else f"£{digits.group(0)}{suffix}"
    return ""


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


def parse_run_timestamp(value: Any) -> Optional[dt.datetime]:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce", utc=False)
    if pd.isna(parsed):
        return None
    if isinstance(parsed, pd.Timestamp):
        if parsed.tzinfo is None:
            return parsed.to_pydatetime().replace(tzinfo=LONDON_TZ)
        return parsed.to_pydatetime().astimezone(LONDON_TZ)
    return None


def timestamp_to_snapshot_id(ts: Optional[dt.datetime]) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S") if ts else ""


def dataframe_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    for col in OUTPUT_COLUMNS:
        if col not in work.columns:
            work[col] = ""

    if "Snapshot Date" in work.columns:
        work["Snapshot Date"] = work["Snapshot Date"].astype(str).str.replace(" 00:00:00", "", regex=False)
    if "Run Timestamp" in work.columns:
        work["Run Timestamp"] = work["Run Timestamp"].fillna("").astype(str).str.strip()

    missing_id = work["Snapshot ID"].fillna("").astype(str).str.strip() == ""
    work.loc[missing_id, "Snapshot ID"] = work.loc[missing_id, "Run Timestamp"].apply(
        lambda v: timestamp_to_snapshot_id(parse_run_timestamp(v))
    )

    city_series = work["City"].fillna("").astype(str).str.strip()
    missing_city = (city_series == "") | (city_series.str.lower() == "unknown")
    work.loc[missing_city, "City"] = work.loc[missing_city, "Source URL"].apply(infer_city_from_url)

    missing_operator = work["Operator"].fillna("").astype(str).str.strip() == ""
    work.loc[missing_operator, "Operator"] = work.loc[missing_operator, "Source URL"].apply(infer_operator_from_url)

    missing_source = work["Scrape Source"].fillna("").astype(str).str.strip() == ""
    work.loc[missing_source, "Scrape Source"] = "Local"

    return work[OUTPUT_COLUMNS]


def read_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    current = pd.read_excel(path, sheet_name=SHEET_NAME, engine="openpyxl")
    return dataframe_to_schema(current)


def append_history(path: Path, new_rows: List[Dict[str, str]]) -> Tuple[int, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = read_history(path)
    new_df = pd.DataFrame(new_rows, columns=OUTPUT_COLUMNS)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined.to_excel(path, index=False, sheet_name=SHEET_NAME, engine="openpyxl")
    return len(existing), len(new_df)


def clean_existing_workbook_for_production(path: Path) -> Dict[str, int]:
    if not path.exists():
        return {"before": 0, "after": 0, "removed_blank_ts": 0, "removed_dup_batches": 0}

    df = pd.read_excel(path, sheet_name=SHEET_NAME, engine="openpyxl")
    df = dataframe_to_schema(df)
    before = len(df)

    non_blank = df["Run Timestamp"].fillna("").astype(str).str.strip() != ""
    df = df[non_blank].copy()
    removed_blank = before - len(df)

    df = df.drop_duplicates(subset=OUTPUT_COLUMNS, keep="first")

    signature_cols = [
        "City",
        "Operator",
        "Property",
        "Room Name",
        "Contract Length",
        "Price",
        "Availability",
        "Source URL",
    ]
    df["_ts"] = df["Run Timestamp"].apply(parse_run_timestamp)
    df["_day"] = df["_ts"].apply(lambda x: x.date().isoformat() if x else "")

    rows_to_keep = []
    removed_dup_batches = 0
    for _, day_df in df.groupby("_day", dropna=False):
        run_groups: Dict[str, pd.DataFrame] = {ts: g.copy() for ts, g in day_df.groupby("Run Timestamp", dropna=False)}
        signature_map: Dict[str, Tuple[Tuple[Any, ...], ...]] = {}
        for ts, g in run_groups.items():
            sig = tuple(
                sorted(tuple(normalize_space(str(row[c])) for c in signature_cols) for _, row in g.iterrows())
            )
            signature_map[ts] = sig

        sig_to_best_ts: Dict[Tuple[Tuple[Any, ...], ...], str] = {}
        for ts, sig in signature_map.items():
            old = sig_to_best_ts.get(sig)
            if old is None:
                sig_to_best_ts[sig] = ts
                continue
            t_new = parse_run_timestamp(ts)
            t_old = parse_run_timestamp(old)
            if t_old is None or (t_new is not None and t_new > t_old):
                sig_to_best_ts[sig] = ts

        keep_ts = set(sig_to_best_ts.values())
        for ts, g in run_groups.items():
            if ts in keep_ts:
                rows_to_keep.append(g)
            else:
                removed_dup_batches += len(g)

    cleaned = pd.concat(rows_to_keep, ignore_index=True) if rows_to_keep else df.iloc[0:0].copy()

    # 4) Additional cleanup heuristic for local validation batches:
    # within the same day, for the same operator set, keep only the latest run timestamp.
    cleaned["_ts"] = cleaned["Run Timestamp"].apply(parse_run_timestamp)
    cleaned["_day"] = cleaned["_ts"].apply(lambda x: x.date().isoformat() if x else "")
    cleaned["_source"] = cleaned["Scrape Source"].fillna("").astype(str).str.strip()
    final_keep_ts: set[str] = set()
    for (_, source), g in cleaned.groupby(["_day", "_source"], dropna=False):
        if source.lower() != "local":
            final_keep_ts.update(g["Run Timestamp"].astype(str).tolist())
            continue
        ts_to_ops = {}
        for ts, ts_group in g.groupby("Run Timestamp", dropna=False):
            op_key = tuple(sorted(set(ts_group["Operator"].astype(str).tolist())))
            ts_to_ops.setdefault(op_key, []).append(str(ts))
        for _, ts_list in ts_to_ops.items():
            best_ts = max(ts_list, key=lambda t: parse_run_timestamp(t) or dt.datetime.min.replace(tzinfo=LONDON_TZ))
            final_keep_ts.add(best_ts)

    before_local_batch_pass = len(cleaned)
    cleaned = cleaned[cleaned["Run Timestamp"].astype(str).isin(final_keep_ts)].copy()
    removed_dup_batches += before_local_batch_pass - len(cleaned)
    cleaned = cleaned.drop(columns=["_ts", "_day", "_source"], errors="ignore")[OUTPUT_COLUMNS]
    cleaned.to_excel(path, index=False, sheet_name=SHEET_NAME, engine="openpyxl")

    return {
        "before": before,
        "after": len(cleaned),
        "removed_blank_ts": removed_blank,
        "removed_dup_batches": removed_dup_batches,
    }


async def safe_goto(page: Page, url: str, timeout: int = 90000) -> bool:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        await page.wait_for_timeout(2200)
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
                    await btn.click(timeout=1200)
                    await page.wait_for_timeout(160)
        except Exception:
            pass


async def resolve_unilife_url(page: Page, provided_url: str) -> Tuple[str, bool]:
    ok = await safe_goto(page, provided_url)
    if not ok:
        return provided_url, False
    title = normalize_space(await page.title()).lower()
    h1 = normalize_space(await page.locator("h1").first.inner_text()).lower() if await page.locator("h1").count() else ""
    if "page not found" not in title and not h1.startswith("404"):
        return page.url, True
    slug = provided_url.rstrip("/").split("/")[-1]
    fallback = UNILIFE_FALLBACK_URL_BY_SLUG.get(slug)
    if not fallback:
        return provided_url, False
    print(f"[INFO] {provided_url} returned 404, retrying live URL: {fallback}")
    ok = await safe_goto(page, fallback)
    return (page.url if ok else fallback), ok


def make_row(
    snapshot_id: str,
    snapshot_date: str,
    run_timestamp: str,
    city: str,
    operator: str,
    property_name: str,
    room_name: str,
    contract_length: str,
    price: str,
    availability: str,
    source_url: str,
    scrape_source: str,
) -> Dict[str, str]:
    return {
        "Snapshot ID": snapshot_id,
        "Snapshot Date": snapshot_date,
        "Run Timestamp": run_timestamp,
        "City": city,
        "Operator": operator,
        "Property": normalize_space(property_name),
        "Room Name": normalize_space(room_name),
        "Contract Length": normalize_space(contract_length),
        "Price": normalize_space(price),
        "Availability": normalize_space(availability),
        "Source URL": source_url,
        "Scrape Source": scrape_source,
    }


async def scrape_unilife_property(
    browser: Browser,
    city: str,
    operator: str,
    source_url: str,
    snapshot_id: str,
    snapshot_date: str,
    run_timestamp: str,
    scrape_source: str,
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    rows: List[Dict[str, str]] = []
    page = await browser.new_page()
    try:
        live_url, ok = await resolve_unilife_url(page, source_url)
        if not ok:
            return [], f"{operator} | {city} | {source_url} (load failed)"
        await click_to_reveal(page)
        await page.wait_for_timeout(900)
        property_name = normalize_space(await page.locator("h1").first.inner_text()).replace("\n", " ") if await page.locator("h1").count() else ""
        if not property_name:
            property_name = normalize_space((await page.title()).split("|")[0])

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
            room_name = re.sub(r"\bcontact\s*us\b", "", room_name, flags=re.IGNORECASE)
            room_name = re.sub(r"[£Ł]?\s*\d{2,4}(?:[.,]\d{1,2})?\s*(?:pw|p/w|/\s*week|per\s*week)", "", room_name, flags=re.IGNORECASE)
            room_name = normalize_space(room_name)
            if not room_name:
                continue

            full_text = normalize_space(block.get("full_text", "")).lower()
            availability = normalize_space(block.get("availability", ""))
            if not availability:
                if "sold out" in full_text or "fully booked" in full_text:
                    availability = "Sold Out"
                elif "waitlist" in full_text or "wait list" in full_text:
                    availability = "Waitlist"
                elif "available" in full_text:
                    availability = "Available"

            base_price = normalize_price(block.get("base_price", ""))
            books = block.get("books", []) or []
            if books:
                for text in books:
                    contract_match = CONTRACT_RE.search(text)
                    contract = contract_match.group(0).upper() if contract_match else ""
                    price = normalize_price(text) or base_price
                    rows.append(make_row(snapshot_id, snapshot_date, run_timestamp, city, operator, property_name, room_name, contract, price, availability, live_url, scrape_source))
            else:
                rows.append(make_row(snapshot_id, snapshot_date, run_timestamp, city, operator, property_name, room_name, "", base_price, availability, live_url, scrape_source))

        return rows, None
    except Exception as exc:
        return [], f"{operator} | {city} | {source_url} ({exc})"
    finally:
        await page.close()


async def scrape_yugo_property(
    browser: Browser,
    city: str,
    operator: str,
    source_url: str,
    snapshot_id: str,
    snapshot_date: str,
    run_timestamp: str,
    scrape_source: str,
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    page = await browser.new_page()
    rows: List[Dict[str, str]] = []
    try:
        if not await safe_goto(page, source_url):
            return [], f"{operator} | {city} | {source_url} (load failed)"
        await click_to_reveal(page)
        await page.wait_for_timeout(1200)
        property_name = normalize_space(await page.locator("h1").first.inner_text()) if await page.locator("h1").count() else normalize_space((await page.title()).split("|")[0])
        lines = [normalize_space(x) for x in (await page.inner_text("body")).splitlines() if normalize_space(x)]
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.upper() == line and len(line) <= 45 and re.search(r"(STUDIO|SUITE|ROOM)", line, re.IGNORECASE):
                room_name = line.title()
                if re.search(r"book a room", room_name, re.IGNORECASE):
                    i += 1
                    continue
                availability, price, contract = "", "", ""
                for j in range(i + 1, min(i + 6, len(lines))):
                    s = lines[j]
                    if re.search(r"sold out", s, re.IGNORECASE):
                        availability = "Sold Out"
                    if not price:
                        price = normalize_price(s)
                    if not contract:
                        c = CONTRACT_RE.search(s)
                        contract = c.group(0).upper() if c else ""
                rows.append(make_row(snapshot_id, snapshot_date, run_timestamp, city, operator, property_name, room_name, contract, price, availability, source_url, scrape_source))
            i += 1
        return rows, None
    except Exception as exc:
        return [], f"{operator} | {city} | {source_url} ({exc})"
    finally:
        await page.close()


async def scrape_student_roost_property(
    browser: Browser,
    city: str,
    operator: str,
    source_url: str,
    snapshot_id: str,
    snapshot_date: str,
    run_timestamp: str,
    scrape_source: str,
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    page = await browser.new_page()
    rows: List[Dict[str, str]] = []
    try:
        if not await safe_goto(page, source_url):
            return [], f"{operator} | {city} | {source_url} (load failed)"
        await click_to_reveal(page)
        await page.wait_for_timeout(900)
        title = await page.title()
        property_name = normalize_space(title.split("-")[0]) if "-" in title else normalize_space(title)
        full_text = " ".join((await page.inner_text("body")).split())
        matches = re.findall(
            r"(En-suite Rooms|Studio Rooms)\s+from\s+(?:£|Ł)?\s*(\d{2,4}(?:[.,]\d{1,2})?)\s+per\s+week",
            full_text,
            flags=re.IGNORECASE,
        )
        for room_name, amount in matches:
            rows.append(
                make_row(
                    snapshot_id,
                    snapshot_date,
                    run_timestamp,
                    city,
                    operator,
                    property_name,
                    room_name.title(),
                    "",
                    f"£{amount} per week",
                    "",
                    source_url,
                    scrape_source,
                )
            )
        unique = {}
        for r in rows:
            key = (r["Room Name"], r["Price"], r["Availability"])
            unique[key] = r
        return list(unique.values()), None
    except Exception as exc:
        return [], f"{operator} | {city} | {source_url} ({exc})"
    finally:
        await page.close()


async def scrape_unite_city(
    browser: Browser,
    city: str,
    operator: str,
    source_url: str,
    snapshot_id: str,
    snapshot_date: str,
    run_timestamp: str,
    scrape_source: str,
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    page = await browser.new_page()
    rows: List[Dict[str, str]] = []
    try:
        if not await safe_goto(page, source_url):
            return [], f"{operator} | {city} | {source_url} (load failed)"
        await click_to_reveal(page)
        await page.wait_for_timeout(1000)
        lines = [normalize_space(x) for x in (await page.inner_text("body")).splitlines() if normalize_space(x)]

        full_text = normalize_space(await page.inner_text("body"))
        base_price = ""
        m = re.search(r"starting from\s+(?:£|Ł)?\s*(\d{3,4})\s*/\s*week", full_text, re.IGNORECASE)
        if not m:
            m = re.search(r"from\s+(?:£|Ł)?\s*(\d{3,4})\s*/\s*week", full_text, re.IGNORECASE)
        if m:
            base_price = f"£{m.group(1)} /week"

        allowed_room_names = {
            "Studio",
            "Ensuite",
            "Non Ensuite: Two Bed Flat",
            "Partly Accessible Studio",
            "Wheelchair Accessible Studio",
        }
        room_candidates = [line for line in lines if line in allowed_room_names]

        property_links = await page.eval_on_selector_all(
            "a[href]",
            "(els) => Array.from(new Set(els.map(e => e.href))).filter(h => h && h.includes('/southampton/') && h.includes('unitestudents.com'))",
        )

        city_property = f"{city} Portfolio"
        for room in sorted(set(room_candidates)):
            rows.append(make_row(snapshot_id, snapshot_date, run_timestamp, city, operator, city_property, room, "", base_price, "", source_url, scrape_source))

        for link in sorted(set(property_links)):
            sub = await browser.new_page()
            try:
                if await safe_goto(sub, link, timeout=60000):
                    sub_text = await sub.inner_text("body")
                    sub_title = normalize_space((await sub.title()).split("|")[0])
                    if re.search(r"sold out", sub_text, re.IGNORECASE):
                        rows.append(make_row(snapshot_id, snapshot_date, run_timestamp, city, operator, sub_title, "", "", "", "Sold Out", link, scrape_source))
            except Exception:
                pass
            finally:
                await sub.close()

        return rows, None
    except Exception as exc:
        return [], f"{operator} | {city} | {source_url} ({exc})"
    finally:
        await page.close()


def dedupe_current_run(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out: List[Dict[str, str]] = []
    for row in rows:
        key = tuple(row.get(col, "") for col in OUTPUT_COLUMNS)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


async def scrape_city_sources(
    browser: Browser,
    city: str,
    sources: List[Dict[str, str]],
    snapshot_id: str,
    snapshot_date: str,
    run_timestamp: str,
    source_label: str,
) -> Tuple[List[Dict[str, str]], List[str]]:
    rows: List[Dict[str, str]] = []
    failures: List[str] = []
    for src in sources:
        operator, kind, url = src["operator"], src["type"], src["url"]
        print(f"[SOURCE] {city} | {operator} | {kind} | {url}")
        if kind == "unilife_property":
            source_rows, err = await scrape_unilife_property(browser, city, operator, url, snapshot_id, snapshot_date, run_timestamp, source_label)
        elif kind == "yugo_property":
            source_rows, err = await scrape_yugo_property(browser, city, operator, url, snapshot_id, snapshot_date, run_timestamp, source_label)
        elif kind == "student_roost_property":
            source_rows, err = await scrape_student_roost_property(browser, city, operator, url, snapshot_id, snapshot_date, run_timestamp, source_label)
        elif kind == "unite_city":
            source_rows, err = await scrape_unite_city(browser, city, operator, url, snapshot_id, snapshot_date, run_timestamp, source_label)
        else:
            source_rows, err = [], f"{operator} | {city} | {url} (unknown source type: {kind})"
        if err:
            print(f"[WARN] {err}")
            failures.append(err)
        if source_rows:
            print(f"[OK] collected {len(source_rows)} rows")
            rows.extend(source_rows)
    return rows, failures


async def run_scrape() -> int:
    if not should_run_for_london_9am():
        return 0

    now_london = dt.datetime.now(LONDON_TZ)
    snapshot_id = now_london.strftime("%Y-%m-%dT%H:%M:%S")
    snapshot_date = now_london.date().isoformat()
    run_timestamp = now_london.strftime("%Y-%m-%d %H:%M:%S %Z")
    source_label = scrape_source_label()

    all_rows: List[Dict[str, str]] = []
    failed_sources: List[str] = []
    cities_scraped: List[str] = []
    operators_scraped: set[str] = set()
    properties_scraped: set[str] = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            for city, cfg in CITY_CONFIG.items():
                cities_scraped.append(city)
                city_rows, city_failures = await scrape_city_sources(
                    browser, city, cfg.get("sources", []), snapshot_id, snapshot_date, run_timestamp, source_label
                )
                all_rows.extend(city_rows)
                failed_sources.extend(city_failures)
        finally:
            await browser.close()

    all_rows = [r for r in all_rows if normalize_space(r.get("Room Name", "")) or normalize_space(r.get("Price", ""))]
    all_rows = dedupe_current_run(all_rows)
    if not all_rows:
        print("[WARN] No rows scraped in this run. Workbook not updated.")
        return 1

    for row in all_rows:
        operators_scraped.add(row["Operator"])
        if row["Property"]:
            properties_scraped.add(row["Property"])

    previous_rows, appended_rows = append_history(workbook_path(), all_rows)
    with_price = sum(1 for r in all_rows if normalize_space(r.get("Price", "")))

    print("\nRun Summary")
    print(f"Snapshot ID: {snapshot_id}")
    print(f"Snapshot Date: {snapshot_date}")
    print(f"Run Timestamp: {run_timestamp}")
    print(f"Cities scraped: {', '.join(sorted(set(cities_scraped)))}")
    print(f"Operators scraped: {', '.join(sorted(operators_scraped))}")
    print(f"Properties scraped: {len(properties_scraped)}")
    print(f"Rows appended: {appended_rows}")
    print(f"Rows with prices: {with_price}")
    print(f"Rows without prices: {appended_rows - with_price}")
    print(f"Blocked/failed sites: {len(failed_sources)}")
    for fail in failed_sources[:20]:
        print(f" - {fail}")
    print(f"Workbook path: {workbook_path()}")
    print(f"Historical total rows: {previous_rows + appended_rows}")

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multi-city student accommodation pricing tracker")
    parser.add_argument(
        "--clean-existing",
        action="store_true",
        help="Clean the existing workbook for production (remove blank timestamps and duplicate test batches).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.clean_existing:
        stats = clean_existing_workbook_for_production(workbook_path())
        print("Workbook cleanup complete")
        print(f"Before rows: {stats['before']}")
        print(f"Removed blank Run Timestamp rows: {stats['removed_blank_ts']}")
        print(f"Removed duplicate batch rows: {stats['removed_dup_batches']}")
        print(f"After rows: {stats['after']}")
        raise SystemExit(0)

    code = asyncio.run(run_scrape())
    raise SystemExit(code)


if __name__ == "__main__":
    main()
