import datetime as dt
import html
import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

from playwright.async_api import Page

from . import common

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)
JSONLD_RE = re.compile(
    r"<script[^>]*type=\"application/ld\+json\"[^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
ACADEMIC_YEAR_LABEL_RE = re.compile(r"\b20\d{2}\s*-\s*20\d{2}\b")
BOOKING_URL_RE = re.compile(r'href="([^"]*?/booking/details[^"]+)"', re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
CURRENCY_RE = re.compile(r"[£Ł]\s*(\d[\d,]*(?:\.\d{1,2})?)")
WEEKS_RE = re.compile(r"(\d{1,3})\s+WEEK", re.IGNORECASE)
PROPERTY_ID_RE = re.compile(r'"propertyId":"([^"]+)"')
CITY_CODE_RE = re.compile(r'"cityCode":"([^"]+)"')

KNOWN_ROOM_TYPE_PREFIXES = [
    "WHEELCHAIR ACCESSIBLE STUDIO",
    "PARTLY ACCESSIBLE STUDIO",
    "ACCESSIBLE STUDIO",
    "ENSUITE",
    "EN-SUITE",
    "STUDIO",
]

CLICK_TEXT_JS = """
(text) => {
  const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
  const target = normalize(text);
  for (const element of Array.from(document.querySelectorAll('button, [role="tab"], a, label, div, span'))) {
    if (normalize(element.innerText) !== target) {
      continue;
    }
    const style = window.getComputedStyle(element);
    if (style.visibility === 'hidden' || style.display === 'none') {
      continue;
    }
    const rect = element.getBoundingClientRect();
    if (!rect.width || !rect.height) {
      continue;
    }
    element.click();
    return true;
  }
  return false;
}
"""

VISIBLE_ROOM_URLS_JS = """
() => {
  const out = new Set();
  for (const anchor of Array.from(document.querySelectorAll('a[href]'))) {
    try {
      const url = new URL(anchor.href, window.location.href);
      if (!/\\/room\\//i.test(url.pathname)) {
        continue;
      }
      out.add(url.href.split('#')[0]);
    } catch (error) {
    }
  }
  return Array.from(out);
}
"""


def _normalise_key(value: Any) -> str:
    return common.normalize_key(common.normalize_space(value))


def _property_url(value: str) -> str:
    parsed = urlparse(common.normalize_space(value))
    if not parsed.scheme or not parsed.netloc:
        return common.normalize_space(value)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")


async def _fetch_text(page: Page, url: str, referer: str = "") -> str:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(1200)
        return await page.content()
    except Exception:
        return ""


def _display_label(value: Any) -> str:
    text = common.normalize_space(value).replace("EN-SUITE", "ENSUITE")
    if not text:
        return ""
    return " ".join(part.capitalize() for part in text.split())


def _strip_tags(value: Any) -> str:
    return common.normalize_space(TAG_RE.sub(" ", html.unescape(common.normalize_space(value))))


def _extract_money(value: Any) -> Optional[float]:
    text = common.normalize_currency_text(value)
    if not text:
        return None
    hit = CURRENCY_RE.search(text)
    if hit:
        try:
            return round(float(hit.group(1).replace(",", "")), 2)
        except Exception:
            return None
    bare = re.fullmatch(r"\d{2,7}(?:\.\d{1,2})?", text.replace(",", ""))
    if bare:
        try:
            return round(float(bare.group(0)), 2)
        except Exception:
            return None
    return None


def _parse_display_date(value: Any) -> str:
    text = common.normalize_space(value)
    if not text:
        return ""
    text = re.sub(r"\bSept\b", "Sep", text, flags=re.IGNORECASE)
    for fmt in ("%d %b %Y", "%d %B %Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            parsed = dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
        return parsed.strftime("%d/%m/%Y")
    return ""


def _calculate_contract_value(weekly_price: Optional[float], contract_length: str) -> Optional[float]:
    if weekly_price is None:
        return None
    length_text = common.extract_contract_length(contract_length)
    match = WEEKS_RE.match(length_text)
    if not match:
        return None
    return round(weekly_price * int(match.group(1)), 2)


def _extract_academic_year_labels(property_html: str) -> List[str]:
    labels: List[str] = []
    seen = set()
    for token in ACADEMIC_YEAR_LABEL_RE.findall(property_html):
        label = common.normalize_space(token)
        if not label or label in seen:
            continue
        seen.add(label)
        labels.append(label)
    labels.sort(key=lambda value: common.normalise_academic_year(value))
    return labels


def _split_offer_name(name: Any) -> Tuple[str, str]:
    upper = common.normalize_space(name).upper().replace("EN-SUITE", "ENSUITE")
    if not upper:
        return "", ""
    for prefix in KNOWN_ROOM_TYPE_PREFIXES:
        normalized_prefix = prefix.replace("EN-SUITE", "ENSUITE")
        if upper == normalized_prefix:
            return normalized_prefix, ""
        if upper.startswith(f"{normalized_prefix} "):
            return normalized_prefix, upper[len(normalized_prefix) + 1 :].strip()
    parts = upper.split(" ", 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def _extract_room_combinations(property_html: str) -> List[Tuple[str, str]]:
    match = JSONLD_RE.search(property_html)
    if not match:
        return []
    try:
        payload = json.loads(html.unescape(match.group(1)))
    except Exception:
        return []

    offers = ((payload or {}).get("mainEntity") or {}).get("offers") or []
    combinations: List[Tuple[str, str]] = []
    seen = set()
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        room_type_raw, room_category_raw = _split_offer_name(offer.get("name", ""))
        if not room_type_raw or not room_category_raw:
            continue
        key = (room_type_raw, room_category_raw)
        if key in seen:
            continue
        seen.add(key)
        combinations.append(key)
    return combinations


def _extract_property_context(property_html: str) -> Tuple[str, str]:
    property_id_match = PROPERTY_ID_RE.search(property_html)
    city_code_match = CITY_CODE_RE.search(property_html)
    property_id = common.normalize_space(property_id_match.group(1) if property_id_match else "")
    city_code = common.normalize_space(city_code_match.group(1) if city_code_match else "")
    return property_id, city_code


async def _fetch_room_combinations_from_api(
    page: Page,
    property_url: str,
    property_id: str,
    city_code: str,
    academic_year_labels: List[str],
) -> List[Tuple[str, str]]:
    combinations: List[Tuple[str, str]] = []
    seen = set()

    for academic_year_label in academic_year_labels:
        api_url = (
            "https://www.unitestudents.com/api/configurator/roomOptionsV2"
            f"?cityCode={quote(city_code, safe='')}"
            f"&academicYear={quote(academic_year_label, safe='')}"
            f"&propertyId={quote(property_id, safe='')}"
        )
        try:
            response = await page.request.get(
                api_url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                    "Accept-Language": "en-GB,en;q=0.9",
                    "Referer": property_url,
                },
                timeout=90000,
            )
        except Exception:
            continue
        if not response.ok:
            continue
        try:
            payload = await response.json()
        except Exception:
            continue

        for group in (payload or {}).get("data", []) or []:
            if not isinstance(group, dict):
                continue
            for room_classification in group.get("roomClassifications", []) or []:
                if not isinstance(room_classification, dict):
                    continue
                room_type_raw = common.normalize_space(room_classification.get("roomTypeName") or group.get("name"))
                room_category_raw = common.normalize_space(room_classification.get("classification"))
                if not room_type_raw or not room_category_raw:
                    continue
                key = (room_type_raw.upper().replace("EN-SUITE", "ENSUITE"), room_category_raw.upper())
                if key in seen:
                    continue
                seen.add(key)
                combinations.append(key)
    return combinations


async def _dismiss_cookie_banner(page: Page) -> None:
    for name in ("Accept All Cookies", "Allow All Cookies"):
        try:
            button = page.get_by_role("button", name=name)
            if await button.count():
                await button.first.click(timeout=2000)
                await page.wait_for_timeout(500)
                return
        except Exception:
            continue


async def _collect_visible_room_urls(page: Page, property_url: str, academic_year_labels: List[str]) -> List[str]:
    try:
        await page.goto(property_url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(2500)
        await _dismiss_cookie_banner(page)
    except Exception:
        return []

    visible_urls: List[str] = []
    seen = set()

    async def record_visible() -> None:
        try:
            urls = await page.evaluate(VISIBLE_ROOM_URLS_JS)
        except Exception:
            return
        for url in urls or []:
            clean = common.normalize_space(url).split("#", 1)[0]
            if not clean or clean in seen:
                continue
            seen.add(clean)
            visible_urls.append(clean)

    await record_visible()
    for label in academic_year_labels:
        try:
            clicked = await page.evaluate(CLICK_TEXT_JS, label)
        except Exception:
            clicked = False
        if not clicked:
            continue
        await page.wait_for_timeout(1200)
        await record_visible()
    return visible_urls


def _build_generated_room_urls(
    property_url: str,
    academic_year_labels: List[str],
    room_combinations: List[Tuple[str, str]],
) -> List[str]:
    urls: List[str] = []
    seen = set()
    property_base = property_url.rstrip("/")
    for academic_year_label in academic_year_labels:
        for room_type_raw, room_category_raw in room_combinations:
            url = (
                f"{property_base}/room/{quote(room_type_raw, safe='')}"
                f"/{quote(room_category_raw, safe='')}?academicYear={quote(academic_year_label, safe='')}"
            )
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
    return urls


def _extract_booking_urls(room_html: str, room_url: str) -> List[str]:
    booking_urls: List[str] = []
    seen = set()
    origin = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(room_url))
    for hit in BOOKING_URL_RE.findall(room_html):
        url = urljoin(origin, html.unescape(hit)).split("#", 1)[0]
        if url in seen:
            continue
        seen.add(url)
        booking_urls.append(url)
    return booking_urls


def _extract_detail_value(html_text: str, label: str) -> str:
    match = re.search(rf">{re.escape(label)}</p><p[^>]*>(.*?)</p>", html_text, re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return _strip_tags(match.group(1))


def _academic_year_from_url(url: str, fallback: str = "") -> str:
    query = parse_qs(urlparse(url).query)
    for candidate in query.get("academicYear", []):
        year = common.normalise_academic_year(unquote(candidate))
        if year:
            return year
    return common.normalise_academic_year(fallback)


def _room_name(base_room_name: str, academic_year: str, contract_length: str) -> str:
    suffix_parts = []
    ay = common.normalise_academic_year(academic_year)
    if ay:
        suffix_parts.append(f"AY{ay.replace('/', '-')}")
    contract = common.extract_contract_length(contract_length)
    match = WEEKS_RE.match(contract)
    if match:
        suffix_parts.append(f"T{match.group(1)}W")
    elif contract:
        suffix_parts.append(common.normalize_key(contract).upper())
    if not suffix_parts:
        return base_room_name
    return f"{base_room_name} [{' | '.join(suffix_parts)}]"


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


def _ensure_unique_room_names(rows: List[Dict[str, Any]]) -> None:
    grouped: Dict[Tuple[str, str], List[int]] = {}
    for idx, row in enumerate(rows):
        key = (
            _normalise_key(row.get("Property", "")),
            _normalise_key(row.get("Room Name", "")),
        )
        grouped.setdefault(key, []).append(idx)

    for indices in grouped.values():
        if len(indices) <= 1:
            continue
        for idx in indices:
            row = rows[idx]
            token = _start_date_token(row.get("Start Date", ""))
            if token:
                row["Room Name"] = f"{row['Room Name']} | {token}"


def _dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
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
        deduped.append(row)
    return deduped


async def _row_from_booking_url(
    page: Page,
    property_name: str,
    room_url: str,
    booking_url: str,
) -> Optional[Dict[str, Any]]:
    booking_html = await _fetch_text(page, booking_url, referer=room_url)
    if not booking_html:
        return None

    query = parse_qs(urlparse(booking_url).query)
    room_type = _display_label(_extract_detail_value(booking_html, "Room type") or query.get("roomType", [""])[0])
    room_category = _display_label(_extract_detail_value(booking_html, "Room class") or query.get("roomClass", [""])[0])
    academic_year = _academic_year_from_url(booking_url, room_url)
    contract_length = common.extract_contract_length(_extract_detail_value(booking_html, "Duration"))
    weekly_price = _extract_money(_extract_detail_value(booking_html, "Price per week"))
    contract_value = _extract_money(_extract_detail_value(booking_html, "Total price"))
    if contract_value is None:
        contract_value = _calculate_contract_value(weekly_price, contract_length)

    if not room_type and not room_category:
        return None
    if not academic_year or not contract_length or weekly_price is None:
        return None

    base_room_name = " | ".join(part for part in [room_type, room_category] if part)
    start_date = _parse_display_date(_extract_detail_value(booking_html, "Check in date"))
    end_date = _parse_display_date(_extract_detail_value(booking_html, "Check out date"))
    return {
        "Property": common.normalize_space(query.get("buildingName", [""])[0]) or property_name,
        "Room Name": _room_name(base_room_name, academic_year, contract_length),
        "Floor Level": "",
        "Contract Length": contract_length,
        "Academic Year": academic_year,
        "Price": weekly_price,
        "Contract Value": contract_value,
        "Incentives": "",
        "Availability": "Available",
        "Source URL": booking_url,
        "Start Date": start_date,
        "End Date": end_date,
        "__base_room_name": base_room_name,
        "__room_type": room_type,
        "__room_category": room_category,
    }


async def _rows_for_room_url(page: Page, property_name: str, property_url: str, room_url: str) -> List[Dict[str, Any]]:
    room_html = await _fetch_text(page, room_url, referer=property_url)
    if not room_html or "How long do you need your room for?" not in room_html:
        return []

    booking_urls = _extract_booking_urls(room_html, room_url)
    if not booking_urls:
        return []

    rows: List[Dict[str, Any]] = []
    for booking_url in booking_urls:
        row = await _row_from_booking_url(page, property_name, room_url, booking_url)
        if row:
            rows.append(row)
    return rows


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    property_url = _property_url(src.get("url", ""))
    property_name = common.normalize_space(src.get("property", ""))
    if not property_url:
        return [], "unite_property_url_missing"

    try:
        await page.goto(property_url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(2500)
        await _dismiss_cookie_banner(page)
        property_html = await page.locator("html").inner_html(timeout=30000)
    except Exception:
        property_html = ""
    if not property_html:
        return [], "unite_property_page_unavailable"

    academic_year_labels = _extract_academic_year_labels(property_html)
    property_id, city_code = _extract_property_context(property_html)
    room_combinations: List[Tuple[str, str]] = []
    if property_id and city_code and academic_year_labels:
        room_combinations = await _fetch_room_combinations_from_api(
            page,
            property_url=property_url,
            property_id=property_id,
            city_code=city_code,
            academic_year_labels=academic_year_labels,
        )
    if not room_combinations:
        room_combinations = _extract_room_combinations(property_html)
    if not academic_year_labels or not room_combinations:
        return [], "unite_property_data_missing"

    room_urls = _build_generated_room_urls(property_url, academic_year_labels, room_combinations)
    room_urls.extend(await _collect_visible_room_urls(page, property_url, academic_year_labels))

    unique_room_urls: List[str] = []
    seen_urls = set()
    for room_url in room_urls:
        clean = common.normalize_space(room_url).split("#", 1)[0]
        if not clean or clean in seen_urls:
            continue
        seen_urls.add(clean)
        unique_room_urls.append(clean)

    rows: List[Dict[str, Any]] = []
    for room_url in unique_room_urls:
        rows.extend(await _rows_for_room_url(page, property_name, property_url, room_url))

    if not rows:
        return [], "unite_room_contracts_missing"

    _ensure_unique_room_names(rows)
    rows = _dedupe_rows(rows)
    for row in rows:
        row.pop("__base_room_name", None)
    return rows, ""
