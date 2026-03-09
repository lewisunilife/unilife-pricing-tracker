import re
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

from playwright.async_api import Page

from . import common

HFS_API_BASE = "https://api.wearehomesforstudents.com/wp-json/wp/v2/locations"


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if not path:
        return ""
    parts = [p for p in path.split("/") if p]
    if not parts:
        return ""
    return parts[-1]


def _clean_hfs_room_name(raw_room_name: str, property_name: str) -> str:
    text = common.normalize_space(raw_room_name)
    prop = common.normalize_space(property_name)
    if prop:
        text = re.sub(rf"(?:,|\-|\||\u2013)\s*{re.escape(prop)}\s*$", "", text, flags=re.IGNORECASE)
        text = re.sub(rf"\s+{re.escape(prop)}\s*$", "", text, flags=re.IGNORECASE)
    text = common.normalize_space(text).strip(",-| ")
    cleaned = common.clean_room_name(text)
    return cleaned or text


def _extract_contract_fields(contract_title: str, start_date: str, end_date: str) -> Tuple[str, str]:
    title = common.normalize_space(contract_title)
    contract_length = common.extract_contract_length(title)
    ay = common.normalise_academic_year(title)

    if not contract_length and start_date and end_date:
        try:
            from datetime import date

            start = date.fromisoformat(start_date)
            end = date.fromisoformat(end_date)
            days = (end - start).days
            if days > 0:
                weeks = round(days / 7)
                if 30 <= weeks <= 60:
                    contract_length = f"{int(weeks)} WEEKS"
        except Exception:
            pass

    if not ay and start_date:
        try:
            start_year = int(start_date[:4])
            ay = f"{start_year}/{(start_year + 1) % 100:02d}"
        except Exception:
            ay = ""
    return contract_length, ay


def _extract_location_incentives(location: Dict[str, Any]) -> str:
    snippets: List[str] = []

    description = common.normalize_space(location.get("description", ""))
    if description:
        snippets.append(description)

    for offer in location.get("offers", []) or []:
        snippets.append(common.normalize_space((offer or {}).get("post_title", "")))
        snippets.append(common.normalize_space((offer or {}).get("post_content", "")))
        snippets.append(common.normalize_space(((offer or {}).get("acf", {}) or {}).get("offer_code", "")))

    acf = location.get("acf", {}) or {}
    for key in ["offers", "ad_banner", "secondary_ad_banner", "tertiary_ad_banner"]:
        value = acf.get(key)
        if isinstance(value, list):
            for item in value:
                snippets.append(common.normalize_space((item or {}).get("post_title", "")))
                snippets.append(common.normalize_space((item or {}).get("post_content", "")))
        elif isinstance(value, dict):
            snippets.append(common.normalize_space(value.get("post_title", "")))
            snippets.append(common.normalize_space(value.get("post_content", "")))
        elif isinstance(value, str):
            snippets.append(common.normalize_space(value))

    return common.extract_and_normalise_incentives(*snippets)


def _extract_floor_map(room_acf: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for wrapper in room_acf.get("roomDetails", []) or []:
        detail = (wrapper or {}).get("roomDetail", {}) or {}
        room_ref = common.normalize_space(detail.get("roomName", ""))
        floor = common.normalise_floor_level(detail.get("floor", ""))
        if room_ref and floor:
            out[room_ref] = floor
    return out


def _availability_for_contract(quantity_available: str, title: str, has_price: bool) -> str:
    inferred = common.infer_availability(title)
    if inferred in {"Sold Out", "Waitlist", "Limited Availability", "Unavailable"}:
        return inferred
    if quantity_available.isdigit():
        return "Available" if int(quantity_available) > 0 else "Unavailable"
    if has_price:
        return "Available"
    return "Unknown"


def _parse_hfs_payload(payload: List[Dict[str, Any]], source_url: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not payload:
        return rows

    for location in payload:
        property_name = common.normalize_space(location.get("name", ""))
        location_incentives = _extract_location_incentives(location)
        rooms = location.get("rooms", []) or []

        for room in rooms:
            acf = room.get("acf", {}) or {}
            room_name = _clean_hfs_room_name(room.get("post_title", "") or acf.get("roomType", ""), property_name)
            if not room_name:
                continue

            quantity_available = common.normalize_space(acf.get("quantityAvailable", ""))
            floor_map = _extract_floor_map(acf)
            room_level_incentives = common.extract_and_normalise_incentives(
                location_incentives,
                acf.get("description", ""),
                acf.get("roomType", ""),
            )
            contracts = acf.get("contracts", []) or []

            for contract_wrapper in contracts:
                contract = (contract_wrapper or {}).get("contract", {}) or {}
                title = common.normalize_space(contract.get("title", ""))
                start_date = common.normalize_space(contract.get("startDate", ""))
                end_date = common.normalize_space(contract.get("endDate", ""))
                contract_length, ay = _extract_contract_fields(title, start_date, end_date)
                contract_text = common.normalize_currency_text(
                    " ".join(
                        [
                            title,
                            common.normalize_space(contract.get("customDescription", "")),
                            common.normalize_space(contract.get("academicYear", "")),
                        ]
                    )
                )
                contract_value = common.parse_contract_value_numeric(contract_text)
                incentives = common.extract_and_normalise_incentives(room_level_incentives, contract_text)

                prices = contract.get("prices", []) or []
                grouped_prices: Dict[Tuple[float, str], Dict[str, Any]] = {}
                for price_item in prices:
                    raw_price = common.normalize_space((price_item or {}).get("pricePerPersonPerWeek", ""))
                    if not raw_price:
                        continue
                    value = common.parse_price_to_weekly_numeric(f"{raw_price} per week")
                    if value is None:
                        continue

                    room_ref = common.normalize_space((price_item or {}).get("roomName", ""))
                    floor = floor_map.get(room_ref, "")
                    key = (value, floor)
                    grouped_prices[key] = {"price": value, "floor": floor}

                if grouped_prices:
                    for payload_row in grouped_prices.values():
                        availability = _availability_for_contract(quantity_available, contract_text, has_price=True)
                        rows.append(
                            {
                                "Property": property_name,
                                "Room Name": room_name,
                                "Contract Length": contract_length,
                                "Academic Year": ay,
                                "Price": payload_row["price"],
                                "Contract Value": contract_value,
                                "Floor Level": payload_row["floor"],
                                "Incentives": incentives,
                                "Availability": availability,
                                "Source URL": source_url,
                                "__missing_price_reason": "",
                            }
                        )
                    continue

                availability = _availability_for_contract(quantity_available, contract_text, has_price=False)
                rows.append(
                    {
                        "Property": property_name,
                        "Room Name": room_name,
                        "Contract Length": contract_length,
                        "Academic Year": ay,
                        "Price": None,
                        "Contract Value": contract_value,
                        "Floor Level": "",
                        "Incentives": incentives,
                        "Availability": availability,
                        "Source URL": source_url,
                        "__missing_price_reason": common.classify_missing_price_reason(contract_text, availability),
                    }
                )
    return rows


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    slug = _slug_from_url(src["url"])
    if not slug:
        return [], "no location slug found"

    candidates = [slug]
    if slug == "southampton":
        candidates = [
            "emily-davies",
            "green-wood-court",
            "hamwic-hall",
            "marland-house",
            "queens-gate",
            "the-court-yard",
        ]

    all_rows: List[Dict[str, Any]] = []
    for candidate in candidates:
        api_url = f"{HFS_API_BASE}?slug={candidate}&with_rooms=true"
        try:
            response = await page.request.get(api_url, timeout=90000)
        except Exception:
            continue
        if not response.ok:
            continue
        try:
            payload = await response.json()
        except Exception:
            continue

        parsed = _parse_hfs_payload(payload if isinstance(payload, list) else [], api_url)
        if parsed:
            all_rows.extend(parsed)

    if not all_rows:
        return [], "homes api returned no room contracts"

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in all_rows:
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
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, ""
