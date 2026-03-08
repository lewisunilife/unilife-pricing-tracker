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


def _extract_contract_fields(contract_title: str, start_date: str, end_date: str) -> Tuple[str, str]:
    title = common.normalize_space(contract_title)
    contract_length = common.extract_contract_length(title)
    ay = common.normalise_academic_year(title)

    if not contract_length and start_date and end_date:
        try:
            # Use explicit contract days only when provided directly by API dates.
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


def _parse_hfs_payload(payload: List[Dict[str, Any]], source_url: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not payload:
        return rows

    for location in payload:
        property_name = common.normalize_space(location.get("name", ""))
        rooms = location.get("rooms", []) or []
        for room in rooms:
            acf = room.get("acf", {}) or {}
            room_name = common.clean_room_name(room.get("post_title", "") or acf.get("roomType", ""))
            if not room_name:
                continue

            quantity_available = common.normalize_space(acf.get("quantityAvailable", ""))
            contracts = acf.get("contracts", []) or []

            for contract_wrapper in contracts:
                contract = (contract_wrapper or {}).get("contract", {}) or {}
                title = common.normalize_space(contract.get("title", ""))
                start_date = common.normalize_space(contract.get("startDate", ""))
                end_date = common.normalize_space(contract.get("endDate", ""))
                contract_length, ay = _extract_contract_fields(title, start_date, end_date)

                prices = contract.get("prices", []) or []
                if prices:
                    parsed_prices: List[float] = []
                    for price_item in prices:
                        raw_price = common.normalize_space((price_item or {}).get("pricePerPersonPerWeek", ""))
                        if not raw_price:
                            continue
                        val = common.parse_price_to_weekly_numeric(f"{raw_price} per week")
                        if val is not None and val not in parsed_prices:
                            parsed_prices.append(val)

                    if parsed_prices:
                        for value in parsed_prices:
                            availability = "Available"
                            if quantity_available.isdigit() and int(quantity_available) <= 0:
                                availability = "Unavailable"
                            rows.append(
                                {
                                    "Property": property_name,
                                    "Room Name": room_name,
                                    "Contract Length": contract_length,
                                    "Academic Year": ay,
                                    "Price": value,
                                    "Contract Value": None,
                                    "Floor Level": common.normalise_floor_level(title),
                                    "Incentives": common.extract_and_normalise_incentives(
                                        location.get("description", ""),
                                        title,
                                        contract.get("customDescription", ""),
                                    ),
                                    "Availability": availability,
                                    "Source URL": source_url,
                                    "__missing_price_reason": "",
                                }
                            )
                        continue

                availability = "Unavailable"
                if quantity_available and quantity_available.isdigit() and int(quantity_available) > 0:
                    availability = "Unknown"
                rows.append(
                    {
                        "Property": property_name,
                        "Room Name": room_name,
                        "Contract Length": contract_length,
                        "Academic Year": ay,
                        "Price": None,
                        "Contract Value": None,
                        "Floor Level": common.normalise_floor_level(title),
                        "Incentives": common.extract_and_normalise_incentives(title),
                        "Availability": availability,
                        "Source URL": source_url,
                        "__missing_price_reason": common.classify_missing_price_reason(title, availability),
                    }
                )
    return rows


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    slug = _slug_from_url(src["url"])
    if not slug:
        return [], "no location slug found"

    candidates = [slug]
    if slug == "southampton":
        # City listing fallback used by the source registry.
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
            common.normalize_space(row.get("Availability", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, ""
