import copy
import datetime as dt
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import Page

from . import common

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)
LOG_PREFIX = "[CANVAS]"
NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
MONTH_YEAR_RE = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+(\d{2,4})\b",
    re.IGNORECASE,
)
BOOKING_TYPE_HEADING_RE = re.compile(r"SELECT\s+BOOKING\s+TYPE", re.IGNORECASE)
RESULT_CACHE: Dict[str, Tuple[List[Dict[str, Any]], str]] = {}

MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _log(message: str) -> None:
    print(f"{LOG_PREFIX} {message}")


def _request_headers(url: str, referer: str = "") -> Dict[str, str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def _canonical_property_url(value: Any) -> str:
    url = common.normalize_space(value)
    if not url:
        return ""
    return url.split("#", 1)[0].rstrip("/")


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


async def _fetch_property_html(page: Page, property_url: str) -> str:
    try:
        await page.goto(property_url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(2500)
        await _dismiss_cookie_banner(page)
        html_text = await page.content()
        if common.normalize_space(html_text):
            return html_text
    except Exception as exc:
        _log(f"page_navigation_fallback url={property_url} error={exc}")

    try:
        response = await page.request.get(
            property_url,
            headers=_request_headers(property_url, referer=property_url),
            timeout=30000,
        )
        if response.ok:
            html_text = await response.text()
            if common.normalize_space(html_text):
                return html_text
    except Exception as exc:
        _log(f"request_fetch_failed url={property_url} error={exc}")
    return ""


async def _extract_booking_types(page: Page) -> List[str]:
    try:
        labels = await page.evaluate(
            """() => {
                const headings = Array.from(document.querySelectorAll('h2,h3,h4'));
                const target = headings.find((node) =>
                  /select\\s+booking\\s+type/i.test((node.innerText || node.textContent || '').replace(/\\s+/g, ' ').trim())
                );
                if (!target) {
                  return [];
                }
                const section = target.closest('section, div');
                if (!section) {
                  return [];
                }
                const out = [];
                for (const button of Array.from(section.querySelectorAll('button'))) {
                  const text = (button.innerText || button.textContent || '').replace(/\\s+/g, ' ').trim();
                  if (!text) {
                    continue;
                  }
                  out.push(text);
                }
                return out;
            }"""
        )
    except Exception:
        return []

    booking_types: List[str] = []
    seen = set()
    for raw in labels or []:
        label = common.normalize_space(raw)
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        booking_types.append(label)
    return booking_types


def _deep_get(value: Any, *path: Any) -> Any:
    current = value
    for part in path:
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and isinstance(part, int):
            current = current[part] if 0 <= part < len(current) else None
        else:
            return None
        if current is None:
            return None
    return current


def _extract_next_data(html_text: str) -> Dict[str, Any]:
    match = NEXT_DATA_RE.search(html_text or "")
    if not match:
        return {}
    try:
        return json.loads(match.group(1))
    except Exception:
        return {}


def _component_props(next_data: Dict[str, Any]) -> Dict[str, Any]:
    return _deep_get(next_data, "props", "pageProps", "componentProps") or {}


def _find_integrated_cards_root(component_props: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    for value in component_props.values():
        results = _deep_get(value, "integratedCardsResult", "item", "children", "results") or []
        if (
            _deep_get(results, 1, "name") == "Floor Plan"
            and _deep_get(results, 3, "name") == "Lease Terms"
        ):
            return results
    return None


def _find_property_context(component_props: Dict[str, Any], property_name: str) -> Dict[str, str]:
    property_key = common.normalize_key(property_name)
    for value in component_props.values():
        cities = _deep_get(value, "interactiveLocationMapGraphQLResult", "item", "children", "results") or []
        for city in cities:
            for property_node in _deep_get(city, "children", "results") or []:
                select_property = _deep_get(
                    property_node,
                    "nameOfThePropertyPage",
                    "jsonValue",
                    "fields",
                    "selectProperty",
                ) or {}
                candidate_name = common.normalize_key(select_property.get("displayName") or select_property.get("name") or "")
                if property_key and property_key not in candidate_name:
                    continue
                return {
                    "property_id": common.normalize_space(
                        _deep_get(select_property, "fields", "propertyID", "value")
                        or select_property.get("name")
                    ),
                    "availability_url_template": common.normalize_space(
                        _deep_get(select_property, "fields", "availabilityURL", "value")
                    ),
                    "property_display_name": common.normalize_space(select_property.get("displayName") or property_name),
                }
    return {
        "property_id": "",
        "availability_url_template": "",
        "property_display_name": common.normalize_space(property_name),
    }


def _normalise_title_case(value: Any) -> str:
    text = common.normalize_space(value)
    if not text:
        return ""
    if text.isupper() or text.lower() == text:
        text = " ".join(part.capitalize() for part in text.split())
    text = (
        text.replace("Ensuite", "En Suite")
        .replace("En suite", "En Suite")
        .replace("Townhouse", "Townhouse")
    )
    return common.normalize_space(text)


def _infer_room_type(room_label: str, category_label: str) -> str:
    category = _normalise_title_case(category_label)
    if category:
        return category
    low = common.normalize_space(room_label).lower()
    if "studio" in low:
        return "Studio"
    if "en suite" in low or "ensuite" in low:
        return "En Suite"
    if "townhouse" in low:
        return "Shared Townhouse"
    return ""


def _parse_term_map(raw: Any) -> Dict[str, str]:
    text = common.normalize_space(raw).replace("&amp;", "&")
    values: Dict[str, str] = {}
    if not text:
        return values
    for token in text.split("&"):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        key = common.normalize_space(key)
        value = common.normalize_space(value)
        if key:
            values[key] = value
    return values


def _term_value(raw: Any, term_id: str) -> str:
    return common.normalize_space(_parse_term_map(raw).get(common.normalize_space(term_id), ""))


def _term_price(raw: Any, term_id: str) -> Optional[float]:
    value = _term_value(raw, term_id)
    if not value:
        return None
    try:
        amount = float(value)
    except Exception:
        return None
    if amount <= 0:
        return None
    return round(amount, 2)


def _normalise_availability(value: Any) -> str:
    text = common.normalize_space(value)
    if not text:
        return ""
    low = text.lower()
    if "wait" in low:
        return "Waitlist"
    if "sold" in low:
        return "Sold Out"
    if "unavailable" in low:
        return "Unavailable"
    if "available" in low:
        return "Available"
    return text


def _contract_value(weekly_price: Optional[float], contract_length: str) -> Optional[float]:
    if weekly_price is None:
        return None
    match = re.match(r"(\d{1,3})\s+WEEK", common.normalize_space(contract_length), flags=re.IGNORECASE)
    if not match:
        return None
    return round(weekly_price * int(match.group(1)), 2)


def _academic_year_from_term(term_label: str, start_date: str) -> str:
    parsed_start: Optional[dt.date] = None
    start_text = common.normalize_space(start_date)
    if start_text:
        for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                parsed_start = dt.datetime.strptime(start_text, fmt).date()
                break
            except ValueError:
                continue
    if parsed_start:
        start_year = parsed_start.year if parsed_start.month >= 9 else parsed_start.year - 1
        return f"{start_year}/{(start_year + 1) % 100:02d}"

    match = MONTH_YEAR_RE.search(common.normalize_space(term_label))
    if not match:
        return ""
    month = MONTHS.get(match.group(1).lower())
    year = int(match.group(2))
    if year < 100:
        year += 2000
    if not month:
        return ""
    start_year = year if month >= 9 else year - 1
    return f"{start_year}/{(start_year + 1) % 100:02d}"


def _identity_room_name(room_label: str, unit_number: str, academic_year: str, contract_length: str) -> str:
    base = room_label
    unit = common.normalize_space(unit_number)
    if unit:
        base = f"{base} | {unit}"

    parts: List[str] = []
    ay = common.normalise_academic_year(academic_year)
    if ay:
        parts.append(f"AY{ay.replace('/', '-')}")
    weeks_match = re.match(r"(\d{1,3})\s+WEEK", common.normalize_space(contract_length), flags=re.IGNORECASE)
    if weeks_match:
        parts.append(f"T{weeks_match.group(1)}W")
    elif common.normalize_space(contract_length):
        parts.append(common.normalize_key(contract_length).upper())

    return f"{base} [{' | '.join(parts)}]" if parts else base


def _availability_url(
    template: str,
    property_id: str,
    floorplan_id: str,
    term_id: str,
    lease_start_window_id: str,
    unit_space_id: str,
    space_option_id: str,
    fallback_url: str,
) -> str:
    if not template:
        return fallback_url
    replacements = {
        "{propertyid}": property_id,
        "{floorplanid}": floorplan_id,
        "{leasetermid}": term_id,
        "{leasestartwindowid}": lease_start_window_id,
        "{unitspaceid}": unit_space_id,
        "{spaceoptionid}": space_option_id,
    }
    out = template
    for token, value in replacements.items():
        out = out.replace(token, common.normalize_space(value))
    return common.normalize_space(out) or fallback_url


def _term_debug_label(title: str, item_name: str) -> str:
    return common.normalize_space(" ".join(part for part in [title, item_name] if common.normalize_space(part)))


def _build_rows(
    next_data: Dict[str, Any],
    property_url: str,
    property_name: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    component_props = _component_props(next_data)
    integrated_root = _find_integrated_cards_root(component_props)
    if not integrated_root:
        return [], {"reason": "canvas_integrated_cards_missing"}

    property_context = _find_property_context(component_props, property_name)
    property_display_name = property_context.get("property_display_name") or property_name
    property_id = property_context.get("property_id", "")
    availability_template = property_context.get("availability_url_template", "")

    floorplans = _deep_get(integrated_root, 1, "children", "results") or []
    terms = _deep_get(integrated_root, 3, "children", "results") or []
    floorplan_map: Dict[str, Dict[str, Any]] = {}
    for floorplan in floorplans:
        floorplan_id = common.normalize_space(_deep_get(floorplan, "itemId", "value") or floorplan.get("name"))
        if floorplan_id:
            floorplan_map[floorplan_id] = floorplan

    room_categories_detected: List[str] = []
    contract_lengths_detected: List[str] = []
    lease_term_labels_detected: List[str] = []
    unit_tables_opened = 0
    sold_out_fallback_rows = 0
    rows: List[Dict[str, Any]] = []

    for term in terms:
        try:
            term_id = common.normalize_space(_deep_get(term, "itemId", "value"))
            term_item_name = common.normalize_space(_deep_get(term, "itemName", "value"))
            term_title = common.normalize_space(_deep_get(term, "title", "value"))
            lease_term_labels_detected.append(_term_debug_label(term_title, term_item_name))
            contract_length = common.extract_contract_length(term_item_name or term_title)
            academic_year = _academic_year_from_term(
                term_item_name,
                common.normalize_space(_deep_get(term, "children", "results", 0, "startDate", "value")),
            )
            if contract_length:
                contract_lengths_detected.append(contract_length)

            offers = _deep_get(term, "applyOffer", "jsonValue") or []
            if not offers:
                _log(f"lease_term_no_room_cards term={_term_debug_label(term_title, term_item_name)}")
                continue

            for offer in offers:
                try:
                    floorplan_id = common.normalize_space(
                        _deep_get(offer, "fields", "itemId", "value") or offer.get("name")
                    )
                    floorplan = floorplan_map.get(floorplan_id)
                    if not floorplan:
                        _log(
                            f"room_category_missing_floorplan term={_term_debug_label(term_title, term_item_name)} "
                            f"floorplan_id={floorplan_id or '-'}"
                        )
                        continue

                    room_label = _normalise_title_case(
                        _deep_get(offer, "fields", "overwriteName", "value")
                        or _deep_get(floorplan, "overwriteName", "value")
                        or offer.get("displayName")
                        or _deep_get(floorplan, "itemName", "value")
                    )
                    room_type = _infer_room_type(
                        room_label,
                        _deep_get(offer, "fields", "selectFloorPlanCategory", "fields", "itemName", "value")
                        or _deep_get(offer, "fields", "selectFloorPlanCategory", "jsonValue", "fields", "itemName", "value")
                        or _deep_get(floorplan, "selectFloorPlanCategory", "jsonValue", "fields", "itemName", "value")
                        or "",
                    )
                    if room_label:
                        room_categories_detected.append(room_label)

                    lease_start_window_id = common.normalize_space(_deep_get(term, "children", "results", 0, "Id", "value"))
                    space_option_id = common.normalize_space(_deep_get(floorplan, "spaceOptionId", "value"))
                    unit_rows_emitted = 0

                    units = _deep_get(floorplan, "children", "results") or []
                    if units:
                        unit_tables_opened += 1

                    for unit in units:
                        floor_value = common.normalize_space(_deep_get(unit, "floorNumber", "value"))
                        for unit_space in _deep_get(unit, "children", "results") or []:
                            weekly_price = _term_price(_deep_get(unit_space, "price", "value"), term_id)
                            availability = _normalise_availability(
                                _term_value(_deep_get(unit_space, "unitSpaceAvailabilityStatus", "value"), term_id)
                            )
                            if weekly_price is None and not availability:
                                continue

                            unit_number = common.normalize_space(
                                _deep_get(unit_space, "unitNumber", "value") or _deep_get(unit, "unitNumber", "value")
                            )
                            row = {
                                "Property": property_display_name,
                                "Room Name": _identity_room_name(room_label, unit_number, academic_year, contract_length),
                                "Floor Level": floor_value,
                                "Contract Length": contract_length,
                                "Academic Year": academic_year,
                                "Price": weekly_price,
                                "Contract Value": _contract_value(weekly_price, contract_length),
                                "Incentives": "",
                                "Availability": availability or "Available",
                                "Source URL": _availability_url(
                                    template=availability_template,
                                    property_id=property_id,
                                    floorplan_id=floorplan_id,
                                    term_id=term_id,
                                    lease_start_window_id=lease_start_window_id,
                                    unit_space_id=common.normalize_space(_deep_get(unit_space, "unitSpaceId", "value")),
                                    space_option_id=space_option_id,
                                    fallback_url=property_url,
                                ),
                                "Room Type": room_type,
                                "Room Category": room_label,
                                "Unit Number": unit_number,
                            }
                            rows.append(row)
                            unit_rows_emitted += 1

                    if unit_rows_emitted:
                        continue

                    fallback_price = _term_price(
                        _deep_get(offer, "fields", "price", "value") or _deep_get(floorplan, "price", "value"),
                        term_id,
                    )
                    fallback_availability = _normalise_availability(
                        _term_value(
                            _deep_get(offer, "fields", "availabilityStatus", "value")
                            or _deep_get(floorplan, "availabilityStatus", "value"),
                            term_id,
                        )
                    )
                    if fallback_price is None and fallback_availability not in {"Sold Out", "Unavailable", "Waitlist"}:
                        continue

                    rows.append(
                        {
                            "Property": property_display_name,
                            "Room Name": _identity_room_name(room_label, "", academic_year, contract_length),
                            "Floor Level": "",
                            "Contract Length": contract_length,
                            "Academic Year": academic_year,
                            "Price": fallback_price,
                            "Contract Value": _contract_value(fallback_price, contract_length),
                            "Incentives": "",
                            "Availability": fallback_availability or "Unavailable",
                            "Source URL": property_url,
                            "Room Type": room_type,
                            "Room Category": room_label,
                            "Unit Number": "",
                        }
                    )
                    sold_out_fallback_rows += 1
                except Exception as exc:
                    _log(
                        f"room_category_failed term={_term_debug_label(term_title, term_item_name)} "
                        f"error={exc}"
                    )
                    continue
        except Exception as exc:
            _log(f"lease_term_failed error={exc}")
            continue

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            common.normalize_space(row.get("Property", "")),
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Academic Year", "")),
            common.normalize_space(row.get("Source URL", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    room_categories_detected = list(dict.fromkeys(room_categories_detected))
    contract_lengths_detected = list(dict.fromkeys(contract_lengths_detected))
    lease_term_labels_detected = list(dict.fromkeys(lease_term_labels_detected))
    return deduped, {
        "reason": (
            f"canvas rows={len(deduped)} lease_terms={len(terms)} floorplans={len(floorplans)} "
            f"room_categories={len(room_categories_detected)} unit_tables={unit_tables_opened} "
            f"sold_out_fallback_rows={sold_out_fallback_rows}"
        ),
        "lease_term_labels": lease_term_labels_detected,
        "room_categories": room_categories_detected,
        "contract_lengths": contract_lengths_detected,
        "property_name": property_display_name,
        "unit_tables_opened": unit_tables_opened,
    }


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    source_url = common.normalize_space(src.get("url", ""))
    property_url = _canonical_property_url(source_url)
    property_name = common.normalize_space(src.get("property", "")) or "Canvas Property"
    if not property_url:
        return [], "canvas_property_url_missing"

    cached = RESULT_CACHE.get(property_url)
    if cached and source_url and source_url.rstrip("/") != property_url:
        return [], "canvas_secondary_url_skipped_cached_primary"
    if cached:
        return copy.deepcopy(cached[0]), cached[1]

    html_text = await _fetch_property_html(page, property_url)
    if not html_text:
        return [], "canvas_property_page_unavailable"

    booking_types = await _extract_booking_types(page)
    if not booking_types and BOOKING_TYPE_HEADING_RE.search(html_text):
        booking_types = ["Long Stay"]
    if not booking_types:
        booking_types = ["Long Stay"]
    booking_types = [value for value in booking_types if "group" not in value.lower()] or booking_types

    next_data = _extract_next_data(html_text)
    if not next_data:
        return [], "canvas_next_data_missing"

    rows, meta = _build_rows(next_data, property_url=property_url, property_name=property_name)
    property_display_name = meta.get("property_name") or property_name
    _log(f"property_discovered name={property_display_name} url={property_url}")
    _log(f"booking_types_detected labels={booking_types}")
    _log(f"lease_terms_detected labels={meta.get('lease_term_labels') or []}")
    _log(f"room_categories_detected count={len(meta.get('room_categories') or [])}")
    _log(f"unit_tables_opened count={meta.get('unit_tables_opened') or 0}")
    _log(f"rows_emitted count={len(rows)}")

    reason = meta.get("reason", "canvas_rows_missing")
    RESULT_CACHE[property_url] = (copy.deepcopy(rows), reason)
    return rows, reason
