import asyncio
import datetime as dt
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from playwright.async_api import Browser, Page, TimeoutError, async_playwright

PROVIDED_PROPERTY_URLS = [
    "https://www.unilife.co.uk/student-accommodation/southampton/bargate-house",
    "https://www.unilife.co.uk/student-accommodation/southampton/castle-way",
    "https://www.unilife.co.uk/student-accommodation/winchester/sparkford-house",
    "https://www.unilife.co.uk/student-accommodation/winchester/high-street",
    "https://www.unilife.co.uk/student-accommodation/guildford/riverside-house",
]

FALLBACK_URL_BY_SLUG = {
    "bargate-house": "https://unilife.co.uk/property/unilife-bargate-house/",
    "castle-way": "https://unilife.co.uk/property/castle-way/",
    "sparkford-house": "https://unilife.co.uk/property/unilife-sparkford-house/",
    "high-street": "https://unilife.co.uk/property/unilife-high-street/",
    "riverside-house": "https://unilife.co.uk/property/unilife-riverside-house/",
}

OUTPUT_COLUMNS = [
    "Snapshot Date",
    "Run Timestamp",
    "Property",
    "Room Name",
    "Contract Length",
    "Price",
    "Availability",
    "Source URL",
]

SHEET_NAME = "All Pricing"
LONDON_TZ = ZoneInfo("Europe/London")

PRICE_RE = re.compile(r"([£Ł]?\s*\d{2,4}(?:[.,]\d{1,2})?\s*(?:pw|p/w|per\s*week))", re.IGNORECASE)
CONTRACT_RE = re.compile(r"(\d{1,2}\s*(?:weeks?|months?))", re.IGNORECASE)


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def normalize_price(text: str) -> str:
    value = normalize_space(text)
    if not value:
        return ""
    number_match = re.search(r"(\d{2,4}(?:[.,]\d{1,2})?)", value)
    if not number_match:
        return ""
    amount = number_match.group(1)
    suffix = "pw" if re.search(r"pw|p/w|per\s*week", value, re.IGNORECASE) else ""
    return f"£{amount}{suffix}"


def clean_room_name(text: str) -> str:
    value = normalize_space(text)
    value = re.sub(r"\bbook\s*now\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bcontact\s*us\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"[£Ł]?\s*\d{2,4}(?:[.,]\d{1,2})?\s*(?:pw|p/w|per\s*week)", "", value, flags=re.IGNORECASE)
    return normalize_space(value)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def workbook_path() -> Path:
    return repo_root() / "data" / "Unilife_Pricing_Snapshot.xlsx"


def should_run_for_london_9am() -> bool:
    """For scheduled workflows, only run when London local time is exactly 09:00."""
    event_name = os.getenv("GITHUB_EVENT_NAME", "").strip().lower()
    enforce = os.getenv("ENFORCE_LONDON_9AM", "").strip().lower() in {"1", "true", "yes"}

    if event_name != "schedule" and not enforce:
        return True

    now_london = dt.datetime.now(LONDON_TZ)
    is_target_minute = now_london.hour == 9 and now_london.minute == 0

    if not is_target_minute:
        print(
            f"[INFO] Skipping run: current Europe/London time is {now_london.strftime('%Y-%m-%d %H:%M:%S %Z')} (needs 09:00)."
        )
        return False

    return True


async def safe_goto(page: Page, url: str) -> bool:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(2500)
        return True
    except Exception as exc:
        print(f"[WARN] Failed opening {url}: {exc}")
        return False


async def click_to_reveal(page: Page) -> None:
    for text in ["Accept", "Accept All", "Allow all", "View Rooms", "Rooms", "Book Now", "Show more", "Load more"]:
        try:
            loc = page.get_by_role("button", name=re.compile(rf"^{re.escape(text)}$", re.IGNORECASE))
            count = await loc.count()
            for i in range(min(count, 8)):
                btn = loc.nth(i)
                if await btn.is_visible():
                    await btn.click(timeout=1200)
                    await page.wait_for_timeout(200)
        except Exception:
            pass


async def resolve_live_url(page: Page, provided_url: str) -> Tuple[str, bool]:
    ok = await safe_goto(page, provided_url)
    if not ok:
        return provided_url, False

    title = normalize_space(await page.title()).lower()
    h1 = ""
    if await page.locator("h1").count():
        h1 = normalize_space(await page.locator("h1").first.inner_text()).lower()

    if "page not found" not in title and not h1.startswith("404"):
        return page.url, True

    slug = provided_url.rstrip("/").split("/")[-1]
    fallback = FALLBACK_URL_BY_SLUG.get(slug)
    if not fallback:
        return provided_url, False

    print(f"[INFO] {provided_url} returned 404, retrying live URL: {fallback}")
    ok = await safe_goto(page, fallback)
    return (page.url if ok else fallback), ok


async def collect_modal_blocks(page: Page) -> List[Dict[str, str]]:
    """Each .modal-content__inner contains one room type and its booking rows."""
    return await page.evaluate(
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


async def collect_room_card_fallback(page: Page) -> List[Dict[str, str]]:
    data = await page.evaluate(
        r"""
        () => {
          const cards = [...document.querySelectorAll('.rooms-grid-block__room')];
          return cards.map(card => ({
            room_name: (card.querySelector('.top, h3, h2')?.innerText || '').replace(/\s+/g, ' ').trim(),
            base_price: (card.querySelector('.price')?.innerText || '').replace(/\s+/g, ' ').trim(),
            availability: (card.querySelector('.available')?.innerText || '').replace(/\s+/g, ' ').trim(),
          }));
        }
        """
    )

    rows: List[Dict[str, str]] = []
    for row in data:
        room_name = clean_room_name(row.get("room_name", ""))
        if not room_name:
            continue
        rows.append(
            {
                "room_name": room_name,
                "base_price": normalize_price(row.get("base_price", "")) if row.get("base_price") else "",
                "availability": normalize_space(row.get("availability", "")),
            }
        )
    return rows


def rows_from_modal_blocks(
    blocks: List[Dict[str, str]],
    snapshot_date: str,
    run_timestamp: str,
    property_name: str,
    source_url: str,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    for block in blocks:
        room_name = clean_room_name(block.get("title", ""))
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

        base_price = normalize_price(block.get("base_price", "")) if block.get("base_price") else ""
        books = block.get("books", []) or []

        if books:
            for text in books:
                contract_match = CONTRACT_RE.search(text)
                contract = normalize_space(contract_match.group(1)).upper() if contract_match else ""

                price_match = PRICE_RE.search(text)
                price = normalize_price(price_match.group(1)) if price_match else base_price

                rows.append(
                    {
                        "Snapshot Date": snapshot_date,
                        "Run Timestamp": run_timestamp,
                        "Property": property_name,
                        "Room Name": room_name,
                        "Contract Length": contract,
                        "Price": price,
                        "Availability": availability,
                        "Source URL": source_url,
                    }
                )
        else:
            rows.append(
                {
                    "Snapshot Date": snapshot_date,
                    "Run Timestamp": run_timestamp,
                    "Property": property_name,
                    "Room Name": room_name,
                    "Contract Length": "",
                    "Price": base_price,
                    "Availability": availability,
                    "Source URL": source_url,
                }
            )

    return rows


async def scrape_property(
    browser: Browser,
    provided_url: str,
    snapshot_date: str,
    run_timestamp: str,
) -> List[Dict[str, str]]:
    page = await browser.new_page()
    rows: List[Dict[str, str]] = []

    try:
        source_url, ok = await resolve_live_url(page, provided_url)
        if not ok:
            print(f"[WARN] Could not load property page: {provided_url}")
            return []

        await click_to_reveal(page)
        await page.wait_for_timeout(1500)

        property_name = ""
        if await page.locator("h1").count():
            property_name = normalize_space(await page.locator("h1").first.inner_text()).replace("\n", " ")
        if not property_name:
            property_name = normalize_space((await page.title()).split("|")[0])

        blocks = await collect_modal_blocks(page)
        rows = rows_from_modal_blocks(blocks, snapshot_date, run_timestamp, property_name, source_url)

        # Fallback if modal blocks are unavailable.
        if not rows:
            fallback_rooms = await collect_room_card_fallback(page)
            for item in fallback_rooms:
                rows.append(
                    {
                        "Snapshot Date": snapshot_date,
                        "Run Timestamp": run_timestamp,
                        "Property": property_name,
                        "Room Name": item["room_name"],
                        "Contract Length": "",
                        "Price": item["base_price"],
                        "Availability": item["availability"],
                        "Source URL": source_url,
                    }
                )

        rows = [r for r in rows if normalize_space(r.get("Room Name", ""))]
        print(f"[OK] {property_name}: {len(rows)} raw rows")

    except TimeoutError:
        print(f"[WARN] Timeout scraping {provided_url}")
    except Exception as exc:
        print(f"[WARN] Error scraping {provided_url}: {exc}")
    finally:
        await page.close()

    return rows


def dedupe_current_run(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Deduplicate only rows produced by the current execution."""
    seen = set()
    out: List[Dict[str, str]] = []
    for row in rows:
        key = tuple(row.get(col, "") for col in OUTPUT_COLUMNS)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def read_existing_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    existing = pd.read_excel(path, sheet_name=SHEET_NAME, engine="openpyxl")
    for col in OUTPUT_COLUMNS:
        if col not in existing.columns:
            existing[col] = ""
    return existing[OUTPUT_COLUMNS]


def append_history(path: Path, new_rows: List[Dict[str, str]]) -> Tuple[int, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_df = read_existing_history(path)
    new_df = pd.DataFrame(new_rows, columns=OUTPUT_COLUMNS)

    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined.to_excel(path, index=False, sheet_name=SHEET_NAME, engine="openpyxl")

    return len(existing_df), len(new_df)


async def run_scrape() -> int:
    if not should_run_for_london_9am():
        # Clean skip for non-9AM London schedule tick.
        return 0

    now_london = dt.datetime.now(LONDON_TZ)
    snapshot_date = now_london.date().isoformat()
    run_timestamp = now_london.strftime("%Y-%m-%d %H:%M:%S %Z")
    out_path = workbook_path()

    all_rows: List[Dict[str, str]] = []
    processed = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            for url in PROVIDED_PROPERTY_URLS:
                print(f"Processing: {url}")
                rows = await scrape_property(browser, url, snapshot_date, run_timestamp)
                all_rows.extend(rows)
                processed += 1
        finally:
            await browser.close()

    all_rows = dedupe_current_run(all_rows)

    if not all_rows:
        print("[WARN] No rows scraped in this run. Workbook was not modified.")
        return 1

    previous_rows, appended_rows = append_history(out_path, all_rows)
    total_rows = previous_rows + appended_rows
    priced = sum(1 for r in all_rows if normalize_space(r.get("Price", "")))

    print("\nSnapshot complete")
    print(f"Workbook path: {out_path}")
    print(f"Run timestamp (London): {run_timestamp}")
    print(f"Total properties processed: {processed}")
    print(f"Rows appended this run: {appended_rows}")
    print(f"Rows with prices this run: {priced}")
    print(f"Rows without prices this run: {appended_rows - priced}")
    print(f"Total historical rows in workbook: {total_rows}")

    return 0


def main() -> None:
    code = asyncio.run(run_scrape())
    raise SystemExit(code)


if __name__ == "__main__":
    main()
