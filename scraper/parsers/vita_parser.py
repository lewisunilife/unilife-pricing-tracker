import re
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import Page

from . import common

ROOM_LINK_RE = re.compile(r"/view-room-", re.IGNORECASE)
TOTAL_PAYABLE_RE = re.compile(r"total\s+amount\s+payable\s*[£Ł]\s*([\d,]+(?:\.\d{1,2})?)", re.IGNORECASE)
ANNUAL_WEEKLY_RE = re.compile(r"annual\s*[£Ł]\s*([\d,]+(?:\.\d{1,2})?)\s*per\s*week", re.IGNORECASE)
FALLBACK_WEEKLY_RE = re.compile(r"[£Ł]\s*([\d,]+(?:\.\d{1,2})?)\s*/\s*pw", re.IGNORECASE)
BOOKING_LENGTH_RE = re.compile(r"booking\s*length\s*(\d{1,2}\s*weeks?)", re.IGNORECASE)
FLOOR_RE = re.compile(
    r"\bfloor\s*:?\s*(lower\s*ground|ground|[0-9]{1,2}(?:st|nd|rd|th)?(?:\s*floor)?|first|second|third|fourth|fifth|sixth)\b",
    re.IGNORECASE,
)


async def _dismiss_cookies(page: Page) -> None:
    for label in ("Accept All", "Accept", "Allow all"):
        try:
            button = page.get_by_role("button", name=re.compile(re.escape(label), re.IGNORECASE))
            if await button.count():
                await button.first.click(timeout=1400)
                await page.wait_for_timeout(250)
        except Exception:
            continue


def _clean_vita_room_name(raw: Any) -> str:
    text = common.normalize_space(raw)
    if not text:
        return ""
    text = re.sub(r"^richmond\s+house\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\broom\s*rh-\d+\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(room code|floor|size|academic year)\b.*$", "", text, flags=re.IGNORECASE)
    text = common.normalize_space(text)
    if len(text) > 60:
        text = common.normalize_space(re.split(r"[.;:]", text, maxsplit=1)[0])
    cleaned = common.clean_room_name(text)
    value = cleaned or text
    if value.isupper():
        value = value.title()
    return value


def _parse_total_payable(text: str) -> Optional[float]:
    value = common.normalize_currency_text(text)
    match = TOTAL_PAYABLE_RE.search(value)
    if not match:
        return None
    try:
        return round(float(match.group(1).replace(",", "")), 2)
    except ValueError:
        return None


def _extract_floor(text: str) -> str:
    value = common.normalize_currency_text(text)
    match = FLOOR_RE.search(value)
    if not match:
        return ""
    return common.normalise_floor_level(f"Floor {match.group(1)}")


def _extract_annual_weekly_price(text: str) -> Optional[float]:
    value = common.normalize_currency_text(text)
    hit = ANNUAL_WEEKLY_RE.search(value)
    if hit:
        return common.parse_price_to_weekly_numeric(f"{hit.group(1)} per week")
    fallback = FALLBACK_WEEKLY_RE.search(value)
    if fallback:
        return common.parse_price_to_weekly_numeric(f"{fallback.group(1)} pw")
    return common.parse_price_to_weekly_numeric(value)


def _normalise_availability(text: str, has_book_now: bool, price: Optional[float]) -> str:
    value = common.normalize_currency_text(text)
    inferred = common.infer_availability(value)
    if inferred in {"Available", "Limited Availability", "Sold Out", "Waitlist", "Unavailable"}:
        return inferred
    if has_book_now:
        return "Available"
    if price is not None:
        return "Available"
    return "Unavailable"


async def _collect_room_links(page: Page) -> List[Dict[str, str]]:
    return await page.evaluate(
        r"""
        () => {
          const out = [];
          const seen = new Set();
          const norm = (v) => (v || '').replace(/\s+/g, ' ').trim();
          const anchors = [...document.querySelectorAll('a[href*="view-room-"]')];
          for (const a of anchors) {
            const href = (a.href || '').trim();
            if (!href || seen.has(href)) continue;
            seen.add(href);
            let scope = a;
            for (let i = 0; i < 5; i++) {
              if (!scope?.parentElement) break;
              scope = scope.parentElement;
              const txt = norm(scope.innerText || '');
              if (/view room/i.test(txt) && /\/pw|per week|sold out|available/i.test(txt)) break;
            }
            out.push({
              url: href,
              card_text: norm(scope?.innerText || a.innerText || ''),
            });
          }
          return out.slice(0, 60);
        }
        """
    )


async def _duration_options(view_page: Page) -> Tuple[Optional[int], List[str]]:
    select_count = await view_page.locator("select").count()
    for idx in range(select_count):
        options = await view_page.locator("select").nth(idx).locator("option").all_inner_texts()
        labels = [common.normalize_space(x) for x in options if re.search(r"\b\d{1,2}\s*weeks?\b", x, re.IGNORECASE)]
        deduped: List[str] = []
        for label in labels:
            if label and label not in deduped:
                deduped.append(label)
        if deduped:
            return idx, deduped
    return None, []


async def _select_duration(view_page: Page, select_idx: Optional[int], label: str) -> None:
    if select_idx is None or not label:
        return
    try:
        await view_page.locator("select").nth(select_idx).select_option(label=label, timeout=2500)
        await view_page.wait_for_timeout(850)
    except Exception:
        try:
            await view_page.locator("select").nth(select_idx).select_option(index=0, timeout=1800)
            await view_page.wait_for_timeout(600)
        except Exception:
            return


async def _extract_booking_enrichment(context, room_url: str, duration: str) -> Dict[str, Any]:
    page = await context.new_page()
    try:
        if not await common.safe_goto(page, room_url, timeout=120000):
            return {}
        await _dismiss_cookies(page)
        await page.wait_for_timeout(1000)

        select_idx, _ = await _duration_options(page)
        await _select_duration(page, select_idx, duration)

        has_book_now = False
        try:
            book_btn = page.get_by_role("button", name=re.compile(r"^book now$", re.IGNORECASE))
            has_book_now = await book_btn.count() > 0
            if has_book_now:
                await book_btn.first.click(timeout=4000)
                await page.wait_for_timeout(2500)
        except Exception:
            pass

        current_url = common.normalize_space(page.url)
        if "booking2.vitastudent.com/brochure-callback" in current_url:
            await common.safe_goto(page, "https://booking2.vitastudent.com/booking-details", timeout=60000)
            await page.wait_for_timeout(2200)
        elif "booking2.vitastudent.com" in current_url and "booking-details" not in current_url:
            await common.safe_goto(page, "https://booking2.vitastudent.com/booking-details", timeout=60000)
            await page.wait_for_timeout(2200)

        body = common.normalize_currency_text(await page.inner_text("body"))
        booking_contract = ""
        hit = BOOKING_LENGTH_RE.search(body)
        if hit:
            booking_contract = common.extract_contract_length(hit.group(1))
        return {
            "booking_price": _extract_annual_weekly_price(body),
            "booking_contract_value": _parse_total_payable(body),
            "booking_contract_length": booking_contract,
            "booking_academic_year": common.normalise_academic_year(body),
            "booking_floor": _extract_floor(body),
            "booking_availability": _normalise_availability(body, has_book_now=has_book_now, price=_extract_annual_weekly_price(body)),
            "booking_source_url": common.normalize_space(page.url),
        }
    finally:
        await page.close()


def _merge_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str, str, Optional[float], str], Dict[str, Any]] = {}
    for row in rows:
        key = (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Academic Year", "")),
            row.get("Price"),
            common.normalize_space(row.get("Floor Level", "")),
        )
        existing = merged.get(key)
        if not existing:
            merged[key] = row
            continue
        if existing.get("Contract Value") is None and row.get("Contract Value") is not None:
            existing["Contract Value"] = row.get("Contract Value")
            existing["Source URL"] = row.get("Source URL", existing.get("Source URL", ""))
        if not common.normalize_space(existing.get("Floor Level", "")) and common.normalize_space(row.get("Floor Level", "")):
            existing["Floor Level"] = row.get("Floor Level", "")
        if (existing.get("Price") is None) and (row.get("Price") is not None):
            existing["Price"] = row.get("Price")
        existing["Incentives"] = common.extract_and_normalise_incentives(existing.get("Incentives", ""), row.get("Incentives", ""))
        if common.normalize_space(existing.get("Availability", "")) in {"", "Unknown", "Unavailable"} and common.normalize_space(
            row.get("Availability", "")
        ) in {"Available", "Limited Availability", "Waitlist", "Sold Out"}:
            existing["Availability"] = row.get("Availability")
    return list(merged.values())


async def _parse_room_page(view_page: Page, room_link: Dict[str, str]) -> List[Dict[str, Any]]:
    room_url = common.normalize_space(room_link.get("url", ""))
    if not room_url or not ROOM_LINK_RE.search(room_url):
        return []

    if not await common.safe_goto(view_page, room_url, timeout=120000):
        return []
    await _dismiss_cookies(view_page)
    await view_page.wait_for_timeout(1200)

    room_body = common.normalize_currency_text(await view_page.inner_text("body"))
    room_name = _clean_vita_room_name(await view_page.inner_text("h1"))
    if not room_name:
        room_name = _clean_vita_room_name(room_link.get("card_text", ""))
    if not room_name:
        return []

    floor_value = _extract_floor(room_body)
    academic_year = common.normalise_academic_year(room_body)
    card_text = common.normalize_currency_text(room_link.get("card_text", ""))
    room_incentives = common.extract_and_normalise_incentives(card_text, room_body)
    has_book_now = await view_page.get_by_role("button", name=re.compile(r"^book now$", re.IGNORECASE)).count() > 0

    select_idx, duration_labels = await _duration_options(view_page)
    if not duration_labels:
        duration_from_text = common.extract_contract_length(room_body)
        duration_labels = [duration_from_text] if duration_from_text else [""]

    rows: List[Dict[str, Any]] = []
    for duration in duration_labels:
        await _select_duration(view_page, select_idx, duration)
        selected_body = common.normalize_currency_text(await view_page.inner_text("body"))

        contract_length = common.extract_contract_length(duration) or common.extract_contract_length(selected_body)
        selected_ay = common.normalise_academic_year(selected_body) or academic_year
        selected_floor = _extract_floor(selected_body) or floor_value
        price = _extract_annual_weekly_price(selected_body) or _extract_annual_weekly_price(card_text)
        availability = _normalise_availability(selected_body, has_book_now=has_book_now, price=price)
        incentives = common.extract_and_normalise_incentives(room_incentives, selected_body)

        contract_value = None
        source_url = common.normalize_space(view_page.url) or room_url
        if availability not in {"Sold Out", "Unavailable"} and has_book_now:
            booking = await _extract_booking_enrichment(view_page.context, room_url, duration)
            if booking:
                contract_value = booking.get("booking_contract_value")
                if booking.get("booking_price") is not None:
                    price = booking.get("booking_price")
                if booking.get("booking_contract_length"):
                    contract_length = booking.get("booking_contract_length")
                if booking.get("booking_academic_year"):
                    selected_ay = booking.get("booking_academic_year")
                if booking.get("booking_floor"):
                    selected_floor = booking.get("booking_floor")
                if booking.get("booking_availability"):
                    availability = booking.get("booking_availability")
                if booking.get("booking_source_url"):
                    source_url = booking.get("booking_source_url")

        rows.append(
            {
                "Property": "Richmond House",
                "Room Name": room_name,
                "Contract Length": contract_length,
                "Academic Year": selected_ay,
                "Price": price,
                "Contract Value": contract_value,
                "Floor Level": selected_floor,
                "Incentives": incentives,
                "Availability": availability,
                "Source URL": source_url,
                "__missing_price_reason": common.classify_missing_price_reason(selected_body, availability) if price is None else "",
            }
        )

    return _merge_rows(rows)


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    await _dismiss_cookies(page)
    await page.wait_for_timeout(1200)

    room_links = await _collect_room_links(page)
    if not room_links:
        return [], "parser_selector_failure"

    view_page = await page.context.new_page()
    rows: List[Dict[str, Any]] = []
    try:
        for room_link in room_links:
            rows.extend(await _parse_room_page(view_page, room_link))
    finally:
        await view_page.close()

    rows = _merge_rows(rows)
    if not rows:
        return [], "hidden_deeper_in_flow"
    return rows, ""
