import asyncio
import datetime as dt
import html
import re
from typing import Any, Dict, List, Optional, Tuple
import urllib.request
from urllib.parse import urlparse

from playwright.async_api import Page

from . import common

PRESTIGE_API_BASE = "https://api.prestigestudentliving.com/wp-json/wp/v2/locations"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)
DATE_RANGE_RE = re.compile(
    r"(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\s+to\s+(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})",
    re.IGNORECASE,
)
LI_RE = re.compile(r"<li>(.*?)</li>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")


def _path_parts(url: str) -> List[str]:
    return [part for part in urlparse(url).path.strip("/").split("/") if part]


def _normalise_key(value: Any) -> str:
    return common.normalize_key(common.normalize_space(value))


def _property_url_from_source(url: str) -> str:
    parsed = urlparse(url)
    parts = _path_parts(url)
    if len(parts) >= 3 and parts[0] == "student-accommodation":
        return f"{parsed.scheme}://{parsed.netloc}/{'/'.join(parts[:3])}"
    return url.split("?", 1)[0].split("#", 1)[0].rstrip("/")


def _property_slug(property_url: str) -> str:
    parts = _path_parts(property_url)
    if len(parts) >= 3:
        return parts[2]
    return parts[-1] if parts else ""


def _clean_room_name(raw_room_name: Any, property_name: str) -> str:
    text = common.normalize_space(raw_room_name)
    prop = common.normalize_space(property_name)
    if prop:
        text = re.sub(rf"(?:,|\-|\||\u2013)\s*{re.escape(prop)}\s*$", "", text, flags=re.IGNORECASE)
        text = re.sub(rf"\s+{re.escape(prop)}\s*$", "", text, flags=re.IGNORECASE)
    text = common.normalize_space(text).strip(",-| ")
    cleaned = common.clean_room_name(text)
    return cleaned or text


def _room_slug(room_post_name: str, property_slug_value: str) -> str:
    slug = common.normalize_space(room_post_name).strip("/")
    suffix = f"-{property_slug_value}"
    if slug.lower().endswith(suffix.lower()):
        slug = slug[: -len(suffix)]
    return slug.strip("-")


def _room_page_url(property_url: str, room_post_name: str) -> str:
    property_base = property_url.rstrip("/")
    slug = _room_slug(room_post_name, _property_slug(property_url))
    return f"{property_base}/{slug}" if slug else property_base


def _api_headers(property_url: str) -> Dict[str, str]:
    parsed = urlparse(property_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Referer": property_url,
        "Origin": origin,
    }


def _parse_display_date(text: Any) -> str:
    value = common.normalize_space(text)
    if not value:
        return ""
    value = re.sub(r"\bSept\b", "Sep", value, flags=re.IGNORECASE)
    for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            parsed = dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
        return parsed.strftime("%d/%m/%Y")
    return ""


def _parse_date_range(text: Any) -> Tuple[str, str]:
    value = common.normalize_space(text)
    if not value:
        return "", ""
    match = DATE_RANGE_RE.search(value)
    if not match:
        return "", ""
    return _parse_display_date(match.group(1)), _parse_display_date(match.group(2))


def _parse_deposit(text: Any) -> Optional[float]:
    value = common.normalize_currency_text(text)
    if not value:
        return None
    hit = re.search(r"[£Ł]\s*(\d{1,6}(?:\.\d{1,2})?)", value)
    if not hit:
        return None
    try:
        return round(float(hit.group(1)), 2)
    except Exception:
        return None


def _parse_weekly_price(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return round(float(value), 2)
        except Exception:
            return None

    text = common.normalize_currency_text(value)
    if not text:
        return None
    hit = re.search(r"[£Ł]\s*(\d{2,6}(?:\.\d{1,2})?)", text)
    if hit:
        try:
            return round(float(hit.group(1)), 2)
        except Exception:
            return None
    if re.search(r"\bpp\/pw\b|\bpppw\b|\bppw\b|\bpw\b", text, flags=re.IGNORECASE):
        bare = re.search(r"(\d{2,6}(?:\.\d{1,2})?)", text)
        if bare:
            try:
                return round(float(bare.group(1)), 2)
            except Exception:
                return None
    return common.parse_price_to_weekly_numeric(text)


def _contract_length_from_days(days: Any) -> str:
    try:
        numeric = float(days)
    except Exception:
        return ""
    if numeric <= 0:
        return ""
    weeks = round(numeric / 7)
    if weeks <= 0:
        return ""
    return f"{int(weeks)} WEEK" if int(weeks) == 1 else f"{int(weeks)} WEEKS"


def _calculate_contract_value(weekly_price: Optional[float], contract_length: str) -> Optional[float]:
    if weekly_price is None:
        return None
    length_text = common.extract_contract_length(contract_length)
    if not length_text:
        return None
    hit = re.match(r"(\d{1,3})\s+WEEK", length_text, flags=re.IGNORECASE)
    if not hit:
        return None
    weeks = int(hit.group(1))
    return round(weekly_price * weeks, 2)


def _availability(quantity_available: Any, book_url: str = "") -> str:
    try:
        quantity = int(str(quantity_available).strip())
    except Exception:
        quantity = None
    if quantity is not None:
        return "Available" if quantity > 0 else "Sold Out"
    return "Available" if common.normalize_space(book_url) else "Unknown"


async def _discover_property_urls(page: Page, src: Dict[str, str]) -> List[str]:
    source_url = common.normalize_space(src.get("url", ""))
    property_url = _property_url_from_source(source_url)
    source_parts = _path_parts(source_url)
    if len(source_parts) >= 3:
        return [property_url]

    if len(source_parts) >= 2 and source_parts[0] == "student-accommodation":
        city_slug = source_parts[1]
        urls = await page.evaluate(
            """(citySlug) => {
                const out = new Set();
                for (const anchor of Array.from(document.querySelectorAll('a[href]'))) {
                    try {
                        const url = new URL(anchor.href, window.location.href);
                        const parts = url.pathname.split('/').filter(Boolean);
                        if (
                            parts.length === 3 &&
                            parts[0] === 'student-accommodation' &&
                            parts[1] === citySlug
                        ) {
                            out.add(url.href.split('#')[0].split('?')[0].replace(/\\/$/, ''));
                        }
                    } catch (error) {
                    }
                }
                return Array.from(out);
            }""",
            city_slug,
        )
        if urls:
            target_property = _normalise_key(src.get("property", ""))
            if target_property:
                matched = [
                    url for url in urls
                    if target_property in _normalise_key(url.split("/")[-1].replace("-", " "))
                ]
                return matched or urls
            return urls
    return [property_url]


async def _fetch_location_payload(page: Page, property_url: str) -> Tuple[Optional[Dict[str, Any]], str]:
    slug = _property_slug(property_url)
    if not slug:
        return None, "prestige_property_slug_missing"

    api_url = f"{PRESTIGE_API_BASE}?slug={slug}&with_rooms=true"
    try:
        response = await page.request.get(api_url, headers=_api_headers(property_url), timeout=90000)
    except Exception:
        return None, "prestige_api_unavailable"
    if not response.ok:
        return None, f"prestige_api_http_{response.status}"
    try:
        payload = await response.json()
    except Exception:
        return None, "prestige_api_invalid_json"
    if not isinstance(payload, list) or not payload:
        return None, "prestige_api_empty_payload"
    if not isinstance(payload[0], dict):
        return None, "prestige_api_invalid_payload"
    return payload[0], ""


def _http_headers(property_url: str) -> Dict[str, str]:
    headers = _api_headers(property_url)
    headers.update(
        {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
        }
    )
    return headers


def _strip_tags(fragment: str) -> str:
    value = html.unescape(TAG_RE.sub(" ", fragment))
    return common.normalize_space(value)


def _extract_contract_cards_from_html(html_text: str) -> List[Dict[str, Any]]:
    idx = html_text.find("Available Contracts")
    if idx == -1:
        return []

    section = html_text[idx:]
    stop_tokens = ["All prices are subject to change.", "Sales Hotline", "</main>"]
    end = len(section)
    for token in stop_tokens:
        token_idx = section.find(token)
        if token_idx != -1:
            end = min(end, token_idx)
    section = section[:end]

    cards: List[Dict[str, Any]] = []
    for item in LI_RE.findall(section):
        text = _strip_tags(item)
        if not text or "Book Now" not in text:
            continue
        paragraphs = [
            common.normalize_space(html.unescape(value))
            for value in re.findall(r"<p[^>]*>(.*?)</p>", item, flags=re.IGNORECASE | re.DOTALL)
        ]
        paragraphs = [_strip_tags(value) for value in paragraphs if _strip_tags(value)]
        strongs = [
            _strip_tags(value)
            for value in re.findall(r"<strong[^>]*>(.*?)</strong>", item, flags=re.IGNORECASE | re.DOTALL)
        ]
        href_match = re.search(r'<a[^>]+href="([^"]+)"', item, flags=re.IGNORECASE)
        cards.append(
            {
                "paragraphs": paragraphs,
                "strongs": [value for value in strongs if value],
                "href": html.unescape(href_match.group(1)) if href_match else "",
                "text": text,
            }
        )
    return cards


def _fetch_room_html(room_url: str, property_url: str) -> str:
    request = urllib.request.Request(room_url, headers=_http_headers(property_url))
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8", "ignore")
    except Exception:
        return ""


def _build_row_from_card(
    property_name: str,
    room_name: str,
    room_url: str,
    quantity_available: Any,
    card: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    paragraphs = [common.normalize_space(value) for value in (card.get("paragraphs") or []) if common.normalize_space(value)]
    strongs = [common.normalize_space(value) for value in (card.get("strongs") or []) if common.normalize_space(value)]
    text = common.normalize_currency_text(card.get("text", ""))
    book_url = common.normalize_space(card.get("href", ""))

    title = next((value for value in paragraphs if common.normalise_academic_year(value)), "")
    date_text = next((value for value in paragraphs if DATE_RANGE_RE.search(value)), "")
    contract_length = ""
    for candidate in paragraphs:
        contract_length = common.extract_contract_length(candidate)
        if contract_length:
            break
    if not contract_length:
        contract_length = common.extract_contract_length(title) or common.extract_contract_length(text)

    price_text = " ".join(strongs) or text
    weekly_price = _parse_weekly_price(price_text)
    academic_year = common.normalise_academic_year(title or text)
    start_date, end_date = _parse_date_range(date_text)
    contract_value = _calculate_contract_value(weekly_price, contract_length)
    deposit_text = next((value for value in paragraphs if "deposit" in value.lower()), "")
    deposit = _parse_deposit(deposit_text)

    if not contract_length and not weekly_price and not academic_year:
        return None

    return {
        "Property": property_name,
        "Room Name": room_name,
        "Floor Level": "",
        "Contract Length": contract_length,
        "Academic Year": academic_year,
        "Price": weekly_price,
        "Contract Value": contract_value,
        "Incentives": "",
        "Availability": _availability(quantity_available, book_url),
        "Source URL": room_url,
        "Start Date": start_date,
        "End Date": end_date,
        "Deposit": deposit,
        "__base_room_name": room_name,
        "__option_identity": "|".join(
            part
            for part in [
                common.normalize_space(book_url),
                common.normalize_space(academic_year),
                common.normalize_space(contract_length),
                common.normalize_space(start_date),
                common.normalize_space(end_date),
            ]
            if part
        ),
    }


def _build_rows_from_api_contracts(
    property_name: str,
    room_name: str,
    room_url: str,
    quantity_available: Any,
    contracts: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for wrapper in contracts:
        contract = (wrapper or {}).get("contract", {}) or {}
        if contract.get("showInShortStaysTab"):
            continue
        title = common.normalize_space(contract.get("title", ""))
        start_date = _parse_display_date(contract.get("startDate", ""))
        end_date = _parse_display_date(contract.get("endDate", ""))
        contract_length = (
            common.extract_contract_length(title)
            or _contract_length_from_days(contract.get("minContractDays"))
            or common.extract_contract_length(f"{start_date} - {end_date}")
        )
        academic_year = common.normalise_academic_year(title)
        for price_item in contract.get("prices", []) or [{}]:
            weekly_price = _parse_weekly_price(price_item.get("pricePerPersonPerWeek"))
            contract_value = _calculate_contract_value(weekly_price, contract_length)
            deposit = _parse_deposit(price_item.get("depositPerPerson"))
            row = {
                "Property": property_name,
                "Room Name": room_name,
                "Floor Level": "",
                "Contract Length": contract_length,
                "Academic Year": academic_year,
                "Price": weekly_price,
                "Contract Value": contract_value,
                "Incentives": "",
                "Availability": _availability(quantity_available, room_url),
                "Source URL": room_url,
                "Start Date": start_date,
                "End Date": end_date,
                "Deposit": deposit,
                "__base_room_name": room_name,
                "__option_identity": "|".join(
                    part
                    for part in [
                        common.normalize_space(title),
                        common.normalize_space(academic_year),
                        common.normalize_space(contract_length),
                        common.normalize_space(start_date),
                        common.normalize_space(end_date),
                    ]
                    if part
                ),
            }
            rows.append(row)
    return rows


def _merge_rows(visible_rows: List[Dict[str, Any]], api_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = list(visible_rows)
    seen = {
        (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Academic Year", "")),
            row.get("Price"),
        )
        for row in visible_rows
    }
    for row in api_rows:
        key = (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Academic Year", "")),
            row.get("Price"),
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)
    return merged


def _academic_year_token(value: str) -> str:
    text = common.normalize_space(value)
    return f"AY{text.replace('/', '-')}" if text else ""


def _contract_length_token(value: str) -> str:
    text = common.extract_contract_length(value)
    if not text:
        return ""
    match = re.match(r"(\d{1,3})\s+WEEK", text, flags=re.IGNORECASE)
    if match:
        return f"T{match.group(1)}W"
    return common.normalize_space(text).replace(" ", "")


def _start_date_token(value: str) -> str:
    text = common.normalize_space(value)
    if not text:
        return ""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            parsed = dt.datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed.strftime("%b%Y")
    return re.sub(r"[^A-Za-z0-9]+", "", text)


async def _rows_for_room(
    page: Page,
    property_name: str,
    property_url: str,
    room: Dict[str, Any],
) -> List[Dict[str, Any]]:
    room_acf = room.get("acf", {}) or {}
    room_name = _clean_room_name(room.get("post_title", "") or room_acf.get("roomType", ""), property_name)
    if not room_name:
        return []

    room_url = _room_page_url(property_url, common.normalize_space(room.get("post_name", "")))
    quantity_available = room_acf.get("quantityAvailable", "")
    html_text = await asyncio.to_thread(_fetch_room_html, room_url, property_url)
    visible_rows = [
        row
        for row in (
            _build_row_from_card(property_name, room_name, room_url, quantity_available, card)
            for card in _extract_contract_cards_from_html(html_text)
        )
        if row
    ]
    api_rows = _build_rows_from_api_contracts(
        property_name=property_name,
        room_name=room_name,
        room_url=room_url,
        quantity_available=quantity_available,
        contracts=room_acf.get("contracts", []) or [],
    )
    return _merge_rows(visible_rows, api_rows)


def _disambiguate_room_names(rows: List[Dict[str, Any]]) -> None:
    grouped: Dict[Tuple[str, str], List[int]] = {}
    for idx, row in enumerate(rows):
        key = (
            _normalise_key(row.get("Property", "")),
            _normalise_key(row.get("__base_room_name", row.get("Room Name", ""))),
        )
        grouped.setdefault(key, []).append(idx)

    for indices in grouped.values():
        if len(indices) <= 1:
            continue
        for idx in indices:
            row = rows[idx]
            suffix_parts = [
                _academic_year_token(row.get("Academic Year", "")),
                _contract_length_token(row.get("Contract Length", "")),
                _start_date_token(row.get("Start Date", "")),
            ]
            suffix = " | ".join(part for part in suffix_parts if part)
            if suffix:
                row["Room Name"] = f"{row['__base_room_name']} [{suffix}]"


def _dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            common.normalize_space(row.get("Property", "")),
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Academic Year", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Start Date", "")),
            common.normalize_space(row.get("End Date", "")),
            common.normalize_space(row.get("__option_identity", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    property_urls = await _discover_property_urls(page, src)
    if not property_urls:
        return [], "prestige_property_urls_missing"

    rows: List[Dict[str, Any]] = []
    for property_url in property_urls:
        location, reason = await _fetch_location_payload(page, property_url)
        if not location:
            continue

        property_name = common.normalize_space(location.get("name", "")) or common.normalize_space(src.get("property", ""))
        room_entries = location.get("rooms", []) or []
        for room in room_entries:
            rows.extend(await _rows_for_room(page, property_name, property_url, room))

    if not rows:
        return [], "prestige_room_contracts_missing"

    _disambiguate_room_names(rows)
    rows = _dedupe_rows(rows)
    for row in rows:
        row.pop("__base_room_name", None)
    return rows, ""
