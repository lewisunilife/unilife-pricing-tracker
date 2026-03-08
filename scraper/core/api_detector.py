import json
import re
from typing import Any, Dict, List, Tuple

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page

from .normalisers import (
    clean_room_name,
    extract_contract_length,
    normalise_academic_year,
    normalise_availability,
    normalise_floor_level,
    parse_contract_value_numeric,
    parse_price_to_weekly_numeric,
)


API_HINT_RE = re.compile(r"(api|graphql|json|starrez|availability|rooms)", re.IGNORECASE)


def _walk_json(node: Any, out: List[Dict[str, Any]]) -> None:
    if isinstance(node, dict):
        low_keys = {k.lower() for k in node.keys()}
        room_name = ""
        for key in ("roomName", "room_name", "roomType", "room_type", "name", "title"):
            if key in node:
                room_name = clean_room_name(node.get(key, ""))
                if room_name:
                    break

        price = None
        for key in ("price", "weeklyPrice", "weekly_price", "pricePerWeek", "rent"):
            if key in node:
                price = parse_price_to_weekly_numeric(node.get(key))
                if price is not None:
                    break

        if room_name and price is not None:
            text = json.dumps(node)
            out.append(
                {
                    "Room Name": room_name,
                    "Contract Length": extract_contract_length(text),
                    "Price": price,
                    "Contract Value": parse_contract_value_numeric(text),
                    "Floor Level": normalise_floor_level(text),
                    "Academic Year": normalise_academic_year(text),
                    "Incentives": "",
                    "Availability": normalise_availability(text),
                }
            )

        for value in node.values():
            _walk_json(value, out)
    elif isinstance(node, list):
        for value in node:
            _walk_json(value, out)


async def detect_candidates(page: Page) -> List[str]:
    try:
        urls = await page.evaluate(
            r"""
            () => {
              const fromPerf = performance.getEntriesByType('resource').map(x => x.name || '');
              const fromLinks = [...document.querySelectorAll('a[href],script[src],link[href]')]
                .map(n => n.href || n.src || '');
              return [...new Set([...fromPerf, ...fromLinks])].filter(Boolean);
            }
            """
        )
    except PlaywrightError:
        # Some sites trigger a late redirect/challenge; skip API probing for this URL.
        return []
    return [url for url in urls if API_HINT_RE.search(url)]


async def extract_api_rows(page: Page) -> Tuple[List[Dict[str, Any]], str]:
    candidates = await detect_candidates(page)
    if not candidates:
        return [], "no api candidates discovered"

    rows: List[Dict[str, Any]] = []
    for url in candidates[:25]:
        try:
            response = await page.request.get(url, timeout=9000)
        except Exception:
            continue
        if not response.ok:
            continue
        ctype = (response.headers.get("content-type") or "").lower()
        if "json" not in ctype:
            continue
        try:
            payload = await response.json()
        except Exception:
            continue
        _walk_json(payload, rows)
        if rows:
            return rows, f"api rows from {url}"

    return [], "api candidates checked; no confident rows"
