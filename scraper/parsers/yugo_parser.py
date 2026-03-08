import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

from playwright.async_api import Page

from . import common
from .base import parse_with_selector_plan

YUGO_BASE = "https://yugo.com/en-gb"
SOUTHAMPTON_SLUG = "southampton"

PROPERTY_SLUG_TO_NAME = {
    "austen-house": "Austen House",
    "crescent-place": "Crescent Place",
}

FLOOR_NAME_MAP = {
    "lower gr": "Lower Ground",
    "lower ground": "Lower Ground",
    "ground": "Ground",
    "ground floor": "Ground",
    "floor 01": "First",
    "floor 1": "First",
    "level 1": "First",
    "floor 02": "Second",
    "floor 2": "Second",
    "level 2": "Second",
    "floor 03": "Third",
    "floor 3": "Third",
    "level 3": "Third",
    "floor 04": "Fourth",
    "floor 4": "Fourth",
    "level 4": "Fourth",
    "floor 05": "Fifth",
    "floor 5": "Fifth",
    "level 5": "Fifth",
    "floor 06": "Sixth",
    "floor 6": "Sixth",
    "level 6": "Sixth",
}


async def _dismiss_cookies(page: Page) -> None:
    for label in ("Accept All Cookies", "Accept All", "Accept"):
        try:
            loc = page.get_by_role("button", name=re.compile(re.escape(label), re.IGNORECASE))
            if await loc.count() and await loc.first.is_visible():
                await loc.first.click(timeout=1200)
                await page.wait_for_timeout(250)
                return
        except Exception:
            continue


def _property_slug_from_url(url: str) -> str:
    parts = [p for p in urlparse(url).path.strip("/").split("/") if p]
    if SOUTHAMPTON_SLUG not in parts:
        return ""
    idx = parts.index(SOUTHAMPTON_SLUG)
    if idx + 1 >= len(parts):
        return ""
    return parts[idx + 1].lower()


def _rooms_url_for_property_slug(slug: str) -> str:
    return f"{YUGO_BASE}/global/united-kingdom/{SOUTHAMPTON_SLUG}/{slug}/rooms"


def _normalise_floor_name(raw: Any) -> str:
    text = common.normalize_space(raw).lower()
    if not text:
        return ""
    if text in FLOOR_NAME_MAP:
        return FLOOR_NAME_MAP[text]
    if re.search(r"\blower\s*(?:gr|ground)\b", text):
        return "Lower Ground"
    if re.search(r"\bground\b", text):
        return "Ground"
    return FLOOR_NAME_MAP.get(text, "")


def _normalise_block_name(raw: Any) -> str:
    text = common.normalize_space(raw)
    if not text:
        return ""
    m = re.search(r"\bblock\s*([a-z0-9]+)\b", text, flags=re.IGNORECASE)
    if m:
        return f"Block {m.group(1).upper()}"
    return text


def _room_key(text: Any) -> str:
    key = common.normalize_key(common.normalize_space(text))
    return key.replace("en-suite", "ensuite")


def _clean_room_name(raw: Any) -> str:
    text = common.normalize_space(raw)
    if not text:
        return ""
    text = re.sub(r"\bfrom\s*[\u00A3\u0141]\s*\d[\d,.]*(?:/\s*week|/week|pppw|ppw|pw|p/w)?\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bjoin waitlist\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bsold out\b", "", text, flags=re.IGNORECASE)
    text = common.normalize_space(text)
    cleaned = common.clean_room_name(text.title()) or common.clean_room_name(text)
    return cleaned or text


def _canonical_property_name(slug: str) -> str:
    if slug in PROPERTY_SLUG_TO_NAME:
        return PROPERTY_SLUG_TO_NAME[slug]
    return common.proper_case_property(slug.replace("-", " "), "")


async def _extract_room_cards(
    page: Page,
    rooms_url: str,
    property_slug: str,
) -> Tuple[List[Dict[str, Any]], str, str]:
    ok = await common.safe_goto(page, rooms_url, timeout=120000)
    if not ok:
        return [], "", "rooms page unavailable"

    await _dismiss_cookies(page)
    await common.click_common(page)
    await page.wait_for_timeout(1200)

    body = common.normalize_currency_text(await page.inner_text("body"))
    property_incentives = common.extract_and_normalise_incentives(body)

    cards = await page.evaluate(
        r"""
        (propertySlug) => {
          const out = [];
          const nodes = document.querySelectorAll('article.search-results__item');
          for (const node of nodes) {
            const text = (node.innerText || '').replace(/\s+/g, ' ').trim();
            if (!text) continue;

            const anchors = [...node.querySelectorAll('a[href]')]
              .map(a => ({
                href: (a.href || '').trim(),
                text: (a.innerText || '').replace(/\s+/g, ' ').trim(),
              }))
              .filter(a => a.href && a.href.includes('/southampton/') && a.href.includes('/' + propertySlug + '/'));

            let roomAnchor = anchors.find(a => /(suite|studio|room|flat|house)/i.test(a.text));
            if (!roomAnchor && anchors.length) roomAnchor = anchors[anchors.length - 1];

            const badgeText = [...node.querySelectorAll('*')]
              .map(el => (el.innerText || '').replace(/\s+/g, ' ').trim())
              .filter(t => t && /(cashback|bus pass|bedding|kitchen|voucher|discount|offer)/i.test(t))
              .slice(0, 6)
              .join(' | ');

            const priceMatch = text.match(/from\s*[£Ł]\s*\d[\d,.]*(?:\/\s*week|\/week|pppw|ppw|pw|p\/w)/i);
            out.push({
              text,
              room_title: roomAnchor ? roomAnchor.text : '',
              room_href: roomAnchor ? roomAnchor.href : '',
              price_text: priceMatch ? priceMatch[0] : '',
              sold_out: /\bsold out\b/i.test(text),
              waitlist: /\bwaitlist\b/i.test(text),
              incentive_text: badgeText,
            });
          }
          return out;
        }
        """,
        property_slug,
    )

    rows: List[Dict[str, Any]] = []
    for card in cards:
        card_text = common.normalize_currency_text(card.get("text", ""))
        room_name = _clean_room_name(card.get("room_title", "")) or _clean_room_name(card_text)
        if not room_name:
            continue
        price = common.parse_price_to_weekly_numeric(card.get("price_text", "")) or common.parse_price_to_weekly_numeric(card_text)

        if card.get("sold_out"):
            availability = "Sold Out"
        elif card.get("waitlist"):
            availability = "Waitlist"
        elif price is not None:
            availability = "Available"
        else:
            availability = "Unknown"

        incentives = common.extract_and_normalise_incentives(card.get("incentive_text", ""), card_text)
        if not incentives and property_incentives and availability != "Sold Out":
            incentives = property_incentives

        rows.append(
            {
                "Property": _canonical_property_name(property_slug),
                "Room Name": room_name,
                "Contract Length": "",
                "Academic Year": common.normalise_academic_year(card_text),
                "Price": price,
                "Contract Value": None,
                "Floor Level": "",
                "Incentives": incentives,
                "Availability": availability,
                "Source URL": common.normalize_space(card.get("room_href", "")) or rooms_url,
                "__missing_price_reason": common.classify_missing_price_reason(card_text, availability) if price is None else "",
            }
        )

    if not rows:
        return [], property_incentives, "no room cards"
    return rows, property_incentives, ""


async def _fetch_southampton_residence(page: Page, property_name: str) -> Optional[Dict[str, str]]:
    cities_url = f"{YUGO_BASE}/cities?countryId=598866"
    residences_tpl = f"{YUGO_BASE}/residences?cityId={{city_id}}"
    try:
        city_resp = await page.request.get(cities_url, timeout=45000)
        if not city_resp.ok:
            return None
        city_payload = await city_resp.json()
    except Exception:
        return None

    city_id = ""
    for city in (city_payload or {}).get("cities", []):
        if common.normalize_space(city.get("name", "")).lower() == SOUTHAMPTON_SLUG:
            city_id = str(city.get("contentId", ""))
            break
    if not city_id:
        return None

    try:
        residence_resp = await page.request.get(residences_tpl.format(city_id=city_id), timeout=45000)
        if not residence_resp.ok:
            return None
        residence_payload = await residence_resp.json()
    except Exception:
        return None

    target = property_name.lower()
    for residence in (residence_payload or {}).get("residences", []):
        if common.normalize_space(residence.get("name", "")).lower() != target:
            continue
        return {
            "residence_id": common.normalize_space(residence.get("id", "")),
            "residence_content_id": str(residence.get("contentId", "")),
            "residence_name": common.normalize_space(residence.get("name", "")),
        }
    return None


async def _fetch_rooms_payload(page: Page, residence_id: str) -> List[Dict[str, Any]]:
    if not residence_id:
        return []
    url = f"{YUGO_BASE}/rooms?residenceId={residence_id}"
    try:
        resp = await page.request.get(url, timeout=45000)
        if not resp.ok:
            return []
        payload = await resp.json()
    except Exception:
        return []
    return list((payload or {}).get("rooms", []) or [])


async def _fetch_tenancy_payload(
    page: Page,
    residence_id: str,
    residence_content_id: str,
    room_id: str,
) -> List[Dict[str, Any]]:
    if not (residence_id and residence_content_id and room_id):
        return []
    url = f"{YUGO_BASE}/tenancyOptionsBySSId?residenceId={residence_id}&residenceContentId={residence_content_id}&roomId={room_id}"
    try:
        resp = await page.request.get(url, timeout=45000)
        if not resp.ok:
            return []
        payload = await resp.json()
    except Exception:
        return []
    return list((payload or {}).get("tenancy-options", []) or [])


def _index_token(value: Any) -> str:
    try:
        num = float(value)
    except Exception:
        return common.normalize_space(value)
    if num.is_integer():
        return str(int(num))
    return str(num)


async def _fetch_building_floor_meta(
    page: Page,
    residence_id: str,
    residence_content_id: str,
) -> Tuple[List[str], List[str], Dict[Tuple[str, str], Dict[str, str]]]:
    if not (residence_id and residence_content_id):
        return [], [], {}
    url = f"{YUGO_BASE}/residence-property?residenceId={residence_id}&residenceContentId={residence_content_id}"
    try:
        resp = await page.request.get(url, timeout=45000)
        if not resp.ok:
            return [], [], {}
        payload = await resp.json()
    except Exception:
        return [], [], {}

    building_ids: List[str] = []
    floor_indexes: List[str] = []
    floor_lookup: Dict[Tuple[str, str], Dict[str, str]] = {}

    for building in ((payload or {}).get("property") or {}).get("buildings", []) or []:
        b_id = common.normalize_space(building.get("id", ""))
        block = _normalise_block_name(building.get("name", ""))
        if b_id and b_id not in building_ids:
            building_ids.append(b_id)

        for floor in building.get("floors", []) or []:
            idx = _index_token(floor.get("index", ""))
            floor_name = _normalise_floor_name(floor.get("name", ""))
            if idx and idx not in floor_indexes:
                floor_indexes.append(idx)
            if b_id and idx and floor_name:
                floor_lookup[(b_id, idx)] = {"floor": floor_name, "block": block}
    return building_ids, floor_indexes, floor_lookup


async def _js_date_string(page: Page, iso_date: str) -> str:
    text = common.normalize_space(iso_date)
    if not text:
        return ""
    try:
        return await page.evaluate("d => new Date(d).toString()", text)
    except Exception:
        return text


def _availability_from_flat(flat: Dict[str, Any]) -> str:
    statuses = [common.normalize_space((b or {}).get("bedStatus", "")).upper() for b in flat.get("beds", []) or []]
    available_count = int(flat.get("availableNumOfBedsInFlat") or 0)
    total_count = int(flat.get("totalNumOfBedsInFlat") or 0)

    if any("WAIT" in s for s in statuses):
        return "Waitlist"
    if available_count > 0:
        return "Available"
    if any("UNAVAILABLE" in s for s in statuses):
        return "Unavailable"
    if any("SOLD" in s for s in statuses):
        return "Sold Out"
    if total_count > 0 and available_count == 0:
        return "Sold Out"

    info = common.infer_availability(common.normalize_space(flat.get("bedsInfo", "")))
    if info != "Unknown":
        return info
    return "Unknown"


def _contract_value_from_flat(flat: Dict[str, Any]) -> Optional[float]:
    for bed in flat.get("beds", []) or []:
        total = bed.get("totalPrice")
        if isinstance(total, (int, float)):
            numeric = round(float(total), 2)
            if numeric > 0:
                return numeric
        label = common.normalize_currency_text(bed.get("totalPriceLabel", ""))
        if not label:
            continue
        m = re.search(r"[\u00A3\u0141]\s*(\d{2,7}(?:,\d{3})*(?:\.\d{1,2})?)", label)
        if not m:
            continue
        try:
            numeric = round(float(m.group(1).replace(",", "")), 2)
            if numeric > 0:
                return numeric
        except Exception:
            continue
    return None


def _academic_year_from_group(group: Dict[str, Any]) -> str:
    start = common.normalize_space(group.get("fromYear", ""))
    end = common.normalize_space(group.get("toYear", ""))
    if start and end:
        out = common.normalise_academic_year(f"{start}/{end}")
        if out:
            return out
    return ""


def _contract_length_from_tenancy_name(name: Any) -> str:
    text = common.normalize_space(name)
    if not text:
        return ""
    return common.extract_contract_length(text) or text.upper()


async def _fetch_floor_inventory(
    page: Page,
    *,
    residence_id: str,
    room_type_id: str,
    tenancy_option_id: str,
    tenancy_start_date: str,
    tenancy_end_date: str,
    academic_year_id: str,
    max_num_of_flatmates: int,
    building_ids: List[str],
    floor_indexes: List[str],
    price_per_night_original: Optional[float],
) -> Tuple[List[Dict[str, Any]], str]:
    if not (residence_id and room_type_id and tenancy_option_id and building_ids and floor_indexes):
        return [], "missing booking-flow parameters"

    base_params = {
        "roomTypeId": room_type_id,
        "residenceExternalId": residence_id,
        "tenancyOptionId": tenancy_option_id,
        "tenancyStartDate": tenancy_start_date,
        "tenancyEndDate": tenancy_end_date,
        "academicYearId": academic_year_id,
        "maxNumOfFlatmates": str(max(0, max_num_of_flatmates)),
        "buildingIds": ",".join(building_ids),
        "floorIndexes": ",".join(floor_indexes),
        "sortDirection": "false",
        "pageSize": "8",
        "totalPriceOriginal": "null",
        "pricePerNightOriginal": "null"
        if price_per_night_original is None
        else f"{float(price_per_night_original):.2f}",
        "bedId": "null",
    }

    collected: List[Dict[str, Any]] = []
    page_number = 1
    total_pages = 1
    while page_number <= total_pages:
        params = dict(base_params)
        params["pageNumber"] = str(page_number)
        url = f"{YUGO_BASE}/flats-with-beds?{urlencode(params)}"
        try:
            resp = await page.request.get(url, timeout=45000)
        except Exception:
            return collected, "flats-with-beds request failed"
        if not resp.ok:
            return collected, f"flats-with-beds status {resp.status}"
        try:
            payload = await resp.json()
        except Exception:
            return collected, "flats-with-beds non-json response"

        error_payload = payload.get("error") if isinstance(payload, dict) else None
        if error_payload:
            return collected, common.normalize_space(error_payload.get("errorMessage") or error_payload.get("generalError") or "api error")

        flats = (payload or {}).get("flats", {}) or {}
        collected.extend(flats.get("floors", []) or [])
        try:
            total_pages = int(flats.get("totalPage") or 1)
        except Exception:
            total_pages = 1
        page_number += 1
    return collected, ""


def _group_floor_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not entries:
        return []

    by_floor: Dict[str, List[Dict[str, Any]]] = {}
    for entry in entries:
        floor = common.normalize_space(entry.get("floor", ""))
        by_floor.setdefault(floor, []).append(entry)

    use_block_for_floor: Dict[str, bool] = {}
    for floor, floor_entries in by_floor.items():
        prices = {e.get("price") for e in floor_entries if e.get("price") is not None}
        blocks = {common.normalize_space(e.get("block", "")) for e in floor_entries if common.normalize_space(e.get("block", ""))}
        use_block_for_floor[floor] = len(prices) > 1 and len(blocks) > 1

    grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for entry in entries:
        floor = common.normalize_space(entry.get("floor", ""))
        block = common.normalize_space(entry.get("block", ""))
        floor_label = floor
        if use_block_for_floor.get(floor) and block:
            floor_label = f"{floor} ({block})"

        key = (
            floor_label,
            entry.get("price"),
            common.normalize_space(entry.get("availability", "")),
        )
        contract_value = entry.get("contract_value")
        if key not in grouped:
            grouped[key] = {
                "Floor Level": floor_label,
                "Price": entry.get("price"),
                "Contract Value": contract_value if contract_value is not None else None,
                "Availability": common.normalize_space(entry.get("availability", "")) or "Unknown",
            }
            continue

        current = grouped[key].get("Contract Value")
        if current is None and contract_value is not None:
            grouped[key]["Contract Value"] = contract_value
    return list(grouped.values())


def _fallback_row_from_card(card: Dict[str, Any], property_incentives: str) -> Dict[str, Any]:
    incentives = common.extract_and_normalise_incentives(card.get("Incentives", ""), property_incentives)
    availability = common.normalize_space(card.get("Availability", "")) or "Unknown"
    card_text = " | ".join(
        [
            common.normalize_space(card.get("Room Name", "")),
            common.normalize_space(card.get("Availability", "")),
            common.normalize_space(card.get("Incentives", "")),
        ]
    )
    return {
        "Property": card.get("Property", ""),
        "Room Name": card.get("Room Name", ""),
        "Contract Length": card.get("Contract Length", ""),
        "Academic Year": card.get("Academic Year", ""),
        "Price": card.get("Price", None),
        "Contract Value": card.get("Contract Value", None),
        "Floor Level": card.get("Floor Level", ""),
        "Incentives": incentives,
        "Availability": availability,
        "Source URL": card.get("Source URL", ""),
        "__missing_price_reason": common.classify_missing_price_reason(card_text, availability)
        if card.get("Price") is None
        else "",
    }


def _dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            common.normalize_space(row.get("Property", "")),
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Academic Year", "")),
            row.get("Price"),
            row.get("Contract Value"),
            common.normalize_space(row.get("Floor Level", "")),
            common.normalize_space(row.get("Incentives", "")),
            common.normalize_space(row.get("Availability", "")),
            common.normalize_space(row.get("Source URL", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    slug = _property_slug_from_url(src.get("url", ""))
    if slug not in PROPERTY_SLUG_TO_NAME:
        # Keep city overview URLs from contaminating property-level extraction.
        if f"/{SOUTHAMPTON_SLUG}" in (src.get("url", "") or "").lower():
            return [], "city overview page (property room cards parsed on property URLs)"
        fallback_rows, reason = await parse_with_selector_plan(
            page,
            src,
            title_selectors=["h3", "h4", ".room-title", ".title", '[class*="room-name"]'],
            scope_selectors=[".room-card", '[class*="room"]', "article", '[class*="suite"]'],
        )
        return fallback_rows, reason

    property_name = _canonical_property_name(slug)
    rooms_url = _rooms_url_for_property_slug(slug)
    card_rows, property_incentives, card_reason = await _extract_room_cards(page, rooms_url, slug)
    if not card_rows:
        return [], card_reason or "no yugo room cards"

    residence_info = await _fetch_southampton_residence(page, property_name)
    if not residence_info:
        return _dedupe_rows([_fallback_row_from_card(card, property_incentives) for card in card_rows]), "residence lookup failed"

    rooms_payload = await _fetch_rooms_payload(page, residence_info["residence_id"])
    room_lookup: Dict[str, Dict[str, Any]] = {}
    for room in rooms_payload:
        key = _room_key(room.get("name", ""))
        if key:
            room_lookup[key] = room

    building_ids, floor_indexes, floor_lookup = await _fetch_building_floor_meta(
        page,
        residence_info["residence_id"],
        residence_info["residence_content_id"],
    )

    rows: List[Dict[str, Any]] = []
    for card in card_rows:
        room_name = common.normalize_space(card.get("Room Name", ""))
        room_key = _room_key(room_name)
        matched_room = room_lookup.get(room_key)
        if not matched_room:
            # Soft fallback for slight naming variants (e.g. "En Suite" vs "Ensuite").
            for key, room in room_lookup.items():
                if room_key and (room_key in key or key in room_key):
                    matched_room = room
                    break

        deep_rows: List[Dict[str, Any]] = []
        if matched_room and building_ids and floor_indexes:
            tenancy_groups = await _fetch_tenancy_payload(
                page,
                residence_info["residence_id"],
                residence_info["residence_content_id"],
                common.normalize_space(matched_room.get("id", "")),
            )
            for group in tenancy_groups:
                academic_year = _academic_year_from_group(group)
                for tenancy in group.get("tenancyOption", []) or []:
                    tenancy_option_id = common.normalize_space(tenancy.get("id", ""))
                    start_js = await _js_date_string(page, common.normalize_space(tenancy.get("startDate", "")))
                    end_js = await _js_date_string(page, common.normalize_space(tenancy.get("endDate", "")))
                    if not (tenancy_option_id and start_js and end_js):
                        continue

                    max_num_of_flatmates = int((matched_room.get("maxNumOfBedsInFlat") or 1) - 1)
                    if max_num_of_flatmates < 0:
                        max_num_of_flatmates = 0

                    floor_payload, _api_reason = await _fetch_floor_inventory(
                        page,
                        residence_id=residence_info["residence_id"],
                        room_type_id=common.normalize_space(matched_room.get("id", "")),
                        tenancy_option_id=tenancy_option_id,
                        tenancy_start_date=start_js,
                        tenancy_end_date=end_js,
                        academic_year_id=common.normalize_space(group.get("academicYearId", "")),
                        max_num_of_flatmates=max_num_of_flatmates,
                        building_ids=building_ids,
                        floor_indexes=floor_indexes,
                        price_per_night_original=matched_room.get("minPricePerNight"),
                    )
                    if not floor_payload:
                        continue

                    floor_entries: List[Dict[str, Any]] = []
                    for floor_item in floor_payload:
                        floor_obj = floor_item.get("floor", {}) or {}
                        building_obj = floor_item.get("building", {}) or {}
                        b_id = common.normalize_space(building_obj.get("id", ""))
                        idx = _index_token(floor_obj.get("index", ""))
                        lookup = floor_lookup.get((b_id, idx), {})
                        floor_name = lookup.get("floor") or _normalise_floor_name(floor_obj.get("name", ""))
                        block_name = lookup.get("block") or _normalise_block_name(building_obj.get("name", ""))

                        for flat in floor_item.get("flats", []) or []:
                            price = common.parse_price_to_weekly_numeric(flat.get("weekPriceLabel", ""))
                            if price is None:
                                price = card.get("Price", None)

                            availability = _availability_from_flat(flat)
                            contract_value = _contract_value_from_flat(flat)
                            floor_entries.append(
                                {
                                    "floor": floor_name,
                                    "block": block_name,
                                    "price": price,
                                    "contract_value": contract_value,
                                    "availability": availability,
                                }
                            )

                    for grouped in _group_floor_entries(floor_entries):
                        row = {
                            "Property": property_name,
                            "Room Name": room_name,
                            "Contract Length": _contract_length_from_tenancy_name(tenancy.get("name", "")),
                            "Academic Year": academic_year,
                            "Price": grouped.get("Price"),
                            "Contract Value": grouped.get("Contract Value"),
                            "Floor Level": grouped.get("Floor Level", ""),
                            "Incentives": common.extract_and_normalise_incentives(card.get("Incentives", ""), property_incentives),
                            "Availability": grouped.get("Availability", "Unknown"),
                            "Source URL": card.get("Source URL", "") or rooms_url,
                            "__missing_price_reason": common.classify_missing_price_reason(
                                f"{tenancy.get('name', '')} {grouped.get('Availability', '')}",
                                grouped.get("Availability", ""),
                            )
                            if grouped.get("Price") is None
                            else "",
                        }
                        deep_rows.append(row)

        if deep_rows:
            rows.extend(deep_rows)
        else:
            rows.append(_fallback_row_from_card(card, property_incentives))

    if rows:
        return _dedupe_rows(rows), ""
    return [], "no extractable yugo rows"
