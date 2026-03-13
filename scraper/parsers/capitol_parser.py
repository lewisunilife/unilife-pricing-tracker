import copy
import datetime as dt
import html
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from playwright.async_api import BrowserContext, Page

from . import common

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)
LOG_PREFIX = "[CAPITOL]"
ROOM_CARD_SELECTOR = ".fp-card"
BOOKING_LINK_RE = re.compile(r"\bbook\b", re.IGNORECASE)
TERM_SELECT_RE = re.compile(
    r"<select[^>]+id=[\"']lease_start_window_id[\"'][^>]*>(.*?)</select>",
    re.IGNORECASE | re.DOTALL,
)
OPTION_RE = re.compile(r"<option[^>]+value=[\"']([^\"']+)[\"'][^>]*>(.*?)</option>", re.IGNORECASE | re.DOTALL)
TERM_SEGMENT_RE = re.compile(r"lease_start_window\[id\]/\d+/?", re.IGNORECASE)
DATE_RANGE_RE = re.compile(r"(\d{2}/\d{2}/\d{4})\s*[-–]\s*(\d{2}/\d{2}/\d{4})")
DETAIL_RENT_RE = re.compile(
    r"\bRent\s+([£Ł]\s*\d{2,5}(?:\.\d{1,2})?\s*(?:/\s*week|per\s*week))",
    re.IGNORECASE,
)
DETAIL_LEASE_TERM_RE = re.compile(
    r"\bLease Term\s+(AY\d{2}/\d{2}\s*-\s*\d{1,3}\s*weeks?\s*\(\d{2}/\d{2}/\d{4}\s*[-–]\s*\d{2}/\d{2}/\d{4}\))",
    re.IGNORECASE,
)
WEEKLY_PRICE_RE = re.compile(r"[£Ł]\s*(\d{2,5}(?:\.\d{1,2})?)\s*/?\s*w(?:k|eek)\b", re.IGNORECASE)
_RESULT_CACHE: Dict[str, Tuple[List[Dict[str, Any]], str]] = {}

STEALTH_INIT_SCRIPT = """
() => {
  Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  Object.defineProperty(navigator, 'languages', { get: () => ['en-GB', 'en'] });
  Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
  Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
  Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
  Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4] });
  window.chrome = window.chrome || { runtime: {} };
}
"""


def _log(message: str) -> None:
    print(f"{LOG_PREFIX} {message}")


def _normalise_url(value: Any) -> str:
    return common.normalize_space(value).split("#", 1)[0].rstrip("/")


def _looks_like_portal(url: str) -> bool:
    return "portal.capitolstudents.com" in common.normalize_space(url).lower()


def _looks_like_challenge(text: Any) -> bool:
    low = common.normalize_space(text).lower()
    return any(
        token in low
        for token in [
            "performing security verification",
            "protect against malicious bots",
            "cloudflare",
            "ray id",
        ]
    )


def _split_room_identity(room_name: str) -> Tuple[str, str]:
    parts = [common.normalize_space(part) for part in room_name.split(" - ") if common.normalize_space(part)]
    if len(parts) <= 1:
        return room_name, ""
    return parts[0], " - ".join(parts[1:])


def _room_name_with_term(base_room_name: str, academic_year: str, contract_length: str) -> str:
    parts: List[str] = []
    ay = common.normalise_academic_year(academic_year)
    if ay:
        parts.append(f"AY{ay}")
    contract = common.extract_contract_length(contract_length)
    weeks_match = re.match(r"(\d{1,3})\s+WEEK", contract, flags=re.IGNORECASE)
    if weeks_match:
        parts.append(f"{weeks_match.group(1)}W")
    elif contract:
        parts.append(common.normalize_space(contract).replace(" ", ""))
    if not parts:
        return base_room_name
    return f"{base_room_name} [{' | '.join(parts)}]"


def _calculate_contract_value(weekly_price: Optional[float], contract_length: str) -> Optional[float]:
    if weekly_price is None:
        return None
    length_text = common.extract_contract_length(contract_length)
    match = re.match(r"(\d{1,3})\s+WEEK", length_text, flags=re.IGNORECASE)
    if not match:
        return None
    return round(weekly_price * int(match.group(1)), 2)


def _extract_weekly_price(text: Any) -> Optional[float]:
    value = common.normalize_currency_text(text)
    if not value:
        return None
    weekly_price = common.parse_price_to_weekly_numeric(value)
    if weekly_price is not None:
        return weekly_price
    match = WEEKLY_PRICE_RE.search(value)
    if not match:
        return None
    try:
        return round(float(match.group(1)), 2)
    except Exception:
        return None


def _parse_date_range(text: Any) -> Tuple[str, str]:
    match = DATE_RANGE_RE.search(common.normalize_space(text))
    if not match:
        return "", ""
    start = match.group(1)
    end = match.group(2)
    for value in [start, end]:
        try:
            dt.datetime.strptime(value, "%d/%m/%Y")
        except ValueError:
            return "", ""
    return start, end


def _extract_term_options(html_text: str) -> List[Tuple[str, str]]:
    match = TERM_SELECT_RE.search(html_text or "")
    if not match:
        return []

    terms: List[Tuple[str, str]] = []
    seen = set()
    for value, label_html in OPTION_RE.findall(match.group(1)):
        term_id = common.normalize_space(value)
        label = common.normalize_space(re.sub(r"<[^>]+>", " ", html.unescape(label_html)))
        label_low = label.lower()
        if not term_id or not label:
            continue
        if "all terms" in label_low or "select" in label_low:
            continue
        key = (term_id, label.lower())
        if key in seen:
            continue
        seen.add(key)
        terms.append((term_id, label))
    return terms


def _term_url(detail_url: str, term_id: str) -> str:
    clean = _normalise_url(detail_url)
    if TERM_SEGMENT_RE.search(clean):
        return TERM_SEGMENT_RE.sub(f"lease_start_window[id]/{term_id}/", clean)
    return clean


def _value_after_label(lines: List[str], label: str) -> str:
    wanted = label.lower()
    for index, line in enumerate(lines):
        low = line.lower().rstrip(":")
        if low == wanted or low.startswith(f"{wanted}:"):
            if ":" in line:
                after = common.normalize_space(line.split(":", 1)[1])
                if after:
                    return after
            for next_line in lines[index + 1 : index + 5]:
                candidate = common.normalize_space(next_line)
                if not candidate:
                    continue
                if candidate.lower().rstrip(":") == wanted:
                    continue
                return candidate
    return ""


def _detail_lines(text: str) -> List[str]:
    return [common.normalize_space(line) for line in text.splitlines() if common.normalize_space(line)]


def _dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            common.normalize_space(row.get("Property", "")),
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Academic Year", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Source URL", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _row_key(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        common.normalize_space(row.get("Property", "")),
        common.normalize_space(row.get("Room Name", "")),
        common.normalize_space(row.get("Academic Year", "")),
        common.normalize_space(row.get("Contract Length", "")),
    )


def _merge_rows(primary_rows: List[Dict[str, Any]], secondary_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    order: List[Tuple[str, str, str, str]] = []
    for row in primary_rows:
        key = _row_key(row)
        if key not in merged:
            order.append(key)
        merged[key] = row
    for row in secondary_rows:
        key = _row_key(row)
        existing = merged.get(key)
        if existing is None:
            merged[key] = row
            order.append(key)
            continue
        existing_price = existing.get("Price")
        incoming_price = row.get("Price")
        if incoming_price is not None and existing_price is None:
            merged[key] = row
            continue
        if incoming_price is not None and existing_price is not None:
            merged[key] = row
    return [merged[key] for key in order]


async def _new_stealth_context(page: Page) -> Optional[BrowserContext]:
    browser = page.context.browser
    if browser is None:
        return None
    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1366, "height": 768},
        locale="en-GB",
        timezone_id="Europe/London",
        extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
    )
    context.set_default_timeout(45000)
    context.set_default_navigation_timeout(90000)
    await context.add_init_script(STEALTH_INIT_SCRIPT)
    return context


async def _accept_cookies(page: Page) -> None:
    for label in ["Allow All", "Accept All", "Accept All Cookies", "I Accept All Cookies"]:
        try:
            button = page.get_by_role("button", name=re.compile(re.escape(label), re.IGNORECASE))
            if await button.count():
                await button.first.click(timeout=1500)
                await page.wait_for_timeout(600)
                return
        except Exception:
            continue


async def _goto(page: Page, url: str) -> Tuple[bool, str]:
    last_text = ""
    for attempt in range(2):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            await _accept_cookies(page)
            deadline = time.monotonic() + (15 if attempt == 0 else 20)
            while True:
                await page.wait_for_timeout(2000 if attempt == 0 else 3000)
                last_text = await page.inner_text("body")
                if not _looks_like_challenge(last_text):
                    return True, last_text
                if time.monotonic() >= deadline:
                    _log(f"challenge_detected attempt={attempt + 1} url={url}")
                    break
        except Exception as exc:
            _log(f"navigation_failed attempt={attempt + 1} url={url} error={exc}")
    return False, last_text


async def _collect_links(page: Page) -> List[Dict[str, str]]:
    links = await page.evaluate(
        """() => {
            const out = [];
            const seen = new Set();
            for (const anchor of Array.from(document.querySelectorAll('a[href]'))) {
                const href = (anchor.href || '').trim();
                const text = (anchor.innerText || anchor.textContent || '').replace(/\\s+/g, ' ').trim();
                if (!href || seen.has(`${href}|${text}`)) {
                    continue;
                }
                seen.add(`${href}|${text}`);
                out.push({ href, text });
            }
            return out;
        }"""
    )
    return links or []


def _property_matches(text: str, href: str, property_name: str) -> bool:
    prop_key = common.normalize_key(property_name)
    if not prop_key:
        return True
    hay = f"{common.normalize_key(text)} {common.normalize_key(href)}"
    return prop_key in hay


def _booking_priority(url: str, text: str) -> int:
    low_url = url.lower()
    low_text = text.lower()
    score = 0
    if _looks_like_portal(url):
        score += 100
    if "2026/27" in low_text or "202627" in low_url or "book-202627" in low_url:
        score += 50
    if BOOKING_LINK_RE.search(low_text):
        score += 20
    if "roomsbooking" in low_url:
        score -= 20
    return score


async def _discover_booking_targets(page: Page, src: Dict[str, str]) -> List[str]:
    source_url = _normalise_url(src.get("url", ""))
    property_name = common.normalize_space(src.get("property", ""))
    if not source_url:
        return []
    if _looks_like_portal(source_url):
        _log(f"property_discovered name={property_name or '-'} url={source_url}")
        return [source_url]

    ok, body_text = await _goto(page, source_url)
    if not ok:
        return []

    if _looks_like_portal(page.url) and "room types" in body_text.lower():
        return [_normalise_url(page.url)]

    links = await _collect_links(page)
    current_path = urlparse(source_url).path.rstrip("/").lower()
    if current_path.endswith("/southampton"):
        property_links = [
            item["href"]
            for item in links
            if "/locations/southampton/" in item["href"].lower()
            and _property_matches(item.get("text", ""), item.get("href", ""), property_name)
        ]
        if property_links:
            property_url = _normalise_url(property_links[0])
            _log(f"property_discovered name={property_name or '-'} url={property_url}")
            ok, _ = await _goto(page, property_url)
            if not ok:
                return []
            links = await _collect_links(page)
    else:
        _log(f"property_discovered name={property_name or '-'} url={_normalise_url(page.url) or source_url}")

    booking_links: List[str] = []
    seen = set()
    sorted_links = sorted(
        links,
        key=lambda item: _booking_priority(item.get("href", ""), item.get("text", "")),
        reverse=True,
    )
    for item in sorted_links:
        href = _normalise_url(item.get("href", ""))
        text = common.normalize_space(item.get("text", ""))
        if not href or href in seen:
            continue
        if _booking_priority(href, text) <= 0:
            continue
        seen.add(href)
        booking_links.append(href)
    return booking_links


async def _extract_room_cards(page: Page) -> List[Dict[str, str]]:
    cards = await page.locator(ROOM_CARD_SELECTOR).evaluate_all(
        """(nodes) => nodes.map((node) => {
            const getText = (selector) => {
                const el = node.querySelector(selector);
                return el ? (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim() : '';
            };
            const anchor = node.querySelector('a[href]');
            const text = (node.innerText || node.textContent || '').replace(/\\s+/g, ' ').trim();
            return {
                room_name: getText('h1,h2,h3,h4,h5,[class*="title"],[class*="name"]'),
                price_text: getText('[class*="price"], .price'),
                href: anchor ? anchor.href : '',
                text,
            };
        })"""
    )

    room_cards: List[Dict[str, str]] = []
    seen = set()
    for card in cards or []:
        base_name = common.clean_room_name(card.get("room_name", "")) or common.clean_room_name(card.get("text", ""))
        detail_url = _normalise_url(card.get("href", ""))
        if not base_name or not detail_url:
            continue
        key = (base_name.lower(), detail_url)
        if key in seen:
            continue
        seen.add(key)
        room_type, room_category = _split_room_identity(base_name)
        room_cards.append(
            {
                "room_name": base_name,
                "room_type": room_type,
                "room_category": room_category,
                "price_text": common.normalize_currency_text(card.get("price_text", "") or card.get("text", "")),
                "detail_url": detail_url,
            }
        )
    return room_cards


async def _selected_term_text(page: Page) -> str:
    try:
        return common.normalize_space(
            await page.evaluate(
                """() => {
                    const select = document.querySelector('#lease_start_window_id');
                    if (!select || !select.selectedOptions || !select.selectedOptions.length) {
                        return '';
                    }
                    return select.selectedOptions[0].textContent || select.selectedOptions[0].innerText || '';
                }"""
            )
        )
    except Exception:
        return ""


async def _select_single_term(page: Page, term_id: str) -> bool:
    changed = await page.evaluate(
        """(targetValue) => {
            const items = Array.from(document.querySelectorAll('.selector-item'));
            if (!items.length) {
                return false;
            }
            let target = null;
            for (const item of items) {
                const value = (item.getAttribute('data-value') || '').trim();
                if (value && value !== targetValue && item.classList.contains('selected')) {
                    item.click();
                }
                if (value === targetValue) {
                    target = item;
                }
            }
            if (!target) {
                return false;
            }
            if (!target.classList.contains('selected')) {
                target.click();
            }
            return true;
        }""",
        term_id,
    )
    if not changed:
        return False
    await page.wait_for_timeout(3500)
    return True


def _build_listing_row(
    property_name: str,
    room_card: Dict[str, str],
    term_id: str,
    term_label: str,
) -> Optional[Dict[str, Any]]:
    price_text = common.normalize_currency_text(room_card.get("price_text", ""))
    weekly_price = _extract_weekly_price(price_text)
    if weekly_price is None:
        return None

    academic_year = common.normalise_academic_year(term_label) or common.normalise_academic_year(price_text)
    contract_length = common.extract_contract_length(term_label) or common.extract_contract_length(price_text)
    room_name = _room_name_with_term(room_card["room_name"], academic_year, contract_length)
    start_date, end_date = _parse_date_range(term_label or price_text)
    availability = common.infer_availability(price_text)
    if availability == "Unknown":
        availability = "Available"

    return {
        "Property": property_name,
        "Room Name": room_name,
        "Floor Level": "",
        "Contract Length": contract_length,
        "Academic Year": academic_year,
        "Price": weekly_price,
        "Contract Value": _calculate_contract_value(weekly_price, contract_length),
        "Incentives": "",
        "Availability": availability,
        "Source URL": _term_url(room_card.get("detail_url", ""), term_id),
        "Room Type": room_card.get("room_type", ""),
        "Room Category": room_card.get("room_category", ""),
        "Start Date": start_date,
        "End Date": end_date,
    }


async def _build_term_row(
    page: Page,
    property_name: str,
    room_card: Dict[str, str],
    term_url: str,
    term_label: str,
) -> Optional[Dict[str, Any]]:
    ok, body_text = await _goto(page, term_url)
    if not ok:
        _log(f"detail_page_failed url={term_url}")
        return None

    normalized_body = common.normalize_currency_text(body_text)
    lines = _detail_lines(normalized_body)
    selected_term = await _selected_term_text(page)
    lease_term = (
        common.normalize_space(DETAIL_LEASE_TERM_RE.search(normalized_body).group(1))
        if DETAIL_LEASE_TERM_RE.search(normalized_body)
        else ""
    ) or (
        _value_after_label(lines, "Lease Term")
        or selected_term
        or term_label
    )
    weekly_rent_text = (
        common.normalize_space(DETAIL_RENT_RE.search(normalized_body).group(1))
        if DETAIL_RENT_RE.search(normalized_body)
        else ""
    ) or (
        _value_after_label(lines, "Weekly Rent")
        or _value_after_label(lines, "Rent")
        or room_card.get("price_text", "")
    )
    weekly_price = _extract_weekly_price(weekly_rent_text) or _extract_weekly_price(room_card.get("price_text", ""))
    academic_year = common.normalise_academic_year(lease_term) or common.normalise_academic_year(selected_term) or common.normalise_academic_year(term_label)
    contract_length = common.extract_contract_length(lease_term) or common.extract_contract_length(selected_term) or common.extract_contract_length(term_label)
    availability = common.infer_availability(body_text)
    if availability == "Unknown" and weekly_price is not None:
        availability = "Available"
    start_date, end_date = _parse_date_range(lease_term or selected_term or term_label)

    if not contract_length and not academic_year and weekly_price is None:
        return None

    room_name = _room_name_with_term(room_card["room_name"], academic_year, contract_length)
    return {
        "Property": property_name,
        "Room Name": room_name,
        "Floor Level": "",
        "Contract Length": contract_length,
        "Academic Year": academic_year,
        "Price": weekly_price,
        "Contract Value": _calculate_contract_value(weekly_price, contract_length),
        "Incentives": "",
        "Availability": availability,
        "Source URL": _normalise_url(page.url) or term_url,
        "Room Type": room_card.get("room_type", ""),
        "Room Category": room_card.get("room_category", ""),
        "Start Date": start_date,
        "End Date": end_date,
    }


async def _rows_for_room_card(page: Page, property_name: str, room_card: Dict[str, str]) -> List[Dict[str, Any]]:
    detail_url = room_card.get("detail_url", "")
    if not detail_url:
        return []

    ok, _ = await _goto(page, detail_url)
    if not ok:
        _log(f"detail_page_failed url={detail_url}")
        return []

    html_text = await page.content()
    term_options = _extract_term_options(html_text)
    if not term_options:
        selected_term = await _selected_term_text(page)
        if selected_term:
            current_term_match = re.search(r"lease_start_window\[id\]/(\d+)", page.url, flags=re.IGNORECASE)
            current_term_id = current_term_match.group(1) if current_term_match else ""
            term_options = [(current_term_id, selected_term)]

    _log(f"lease_terms_found room={room_card['room_name']} count={len(term_options)}")

    term_rows: List[Dict[str, Any]] = []
    seen_term_urls = set()
    if not term_options:
        fallback_row = await _build_term_row(page, property_name, room_card, detail_url, "")
        return [fallback_row] if fallback_row else []

    for term_id, term_label in term_options:
        term_url = _term_url(detail_url, term_id) if term_id else detail_url
        if term_url in seen_term_urls:
            continue
        seen_term_urls.add(term_url)
        row = await _build_term_row(page, property_name, room_card, term_url, term_label)
        if row:
            term_rows.append(row)
    return term_rows


async def _parse_portal(page: Page, portal_url: str, property_name: str) -> Tuple[List[Dict[str, Any]], str]:
    cache_key = _normalise_url(portal_url)
    cached = _RESULT_CACHE.get(cache_key)
    if cached:
        return copy.deepcopy(cached[0]), cached[1]

    context = await _new_stealth_context(page)
    if context is None:
        return [], "capitol_browser_context_missing"

    listing_page = await context.new_page()
    room_cards_count = 0
    term_failures = 0
    try:
        ok, body_text = await _goto(listing_page, portal_url)
        if not ok:
            return [], "capitol_portal_unavailable"
        if _looks_like_challenge(body_text):
            return [], "capitol_portal_blocked"
        if "room types" not in body_text.lower():
            return [], "capitol_room_grid_missing"

        resolved_portal_url = _normalise_url(listing_page.url) or cache_key
        room_cards = await _extract_room_cards(listing_page)
        room_cards_count = len(room_cards)
        _log(f"room_cards_discovered property={property_name} count={room_cards_count}")
        if not room_cards:
            return [], "capitol_room_cards_missing"

        listing_rows: List[Dict[str, Any]] = []
        term_options = _extract_term_options(await listing_page.content())
        _log(f"lease_terms_found property={property_name} count={len(term_options)}")
        if not term_options:
            selected_term = await _selected_term_text(listing_page)
            current_term_match = re.search(r"lease_start_window\[id\]/(\d+)", listing_page.url, flags=re.IGNORECASE)
            current_term_id = current_term_match.group(1) if current_term_match else ""
            if current_term_id and selected_term:
                term_options = [(current_term_id, selected_term)]

        for term_id, term_label in term_options:
            if not await _select_single_term(listing_page, term_id):
                term_failures += 1
                continue
            term_room_cards = await _extract_room_cards(listing_page)
            _log(f"room_cards_discovered property={property_name} term={term_label} count={len(term_room_cards)}")
            for room_card in term_room_cards:
                row = _build_listing_row(property_name, room_card, term_id, term_label)
                if row:
                    listing_rows.append(row)

        _log("detail_pages_opened count=0 reason=listing_term_rows_used_for_workflow_reliability")

        rows = list(listing_rows)
        rows = _dedupe_rows(rows)
        _log(f"rows_emitted property={property_name} count={len(rows)}")
        if rows:
            reason = (
                f"capitol rows={len(rows)} room_cards={room_cards_count} "
                f"detail_pages=0 term_failures={term_failures}"
            )
            _RESULT_CACHE[cache_key] = (copy.deepcopy(rows), reason)
            if resolved_portal_url != cache_key:
                _RESULT_CACHE[resolved_portal_url] = (copy.deepcopy(rows), reason)
            return rows, reason
        return [], (
            f"capitol_term_rows_missing room_cards={room_cards_count} "
            f"detail_pages=0 term_failures={term_failures}"
        )
    finally:
        await listing_page.close()
        await context.close()


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    property_name = common.normalize_space(src.get("property", ""))
    booking_targets = await _discover_booking_targets(page, src)
    if not booking_targets:
        return [], "capitol_booking_link_missing"

    last_reason = "capitol_booking_link_missing"
    for booking_target in booking_targets:
        rows, reason = await _parse_portal(page, booking_target, property_name)
        if rows:
            return rows, reason
        last_reason = reason or last_reason
    return [], last_reason
