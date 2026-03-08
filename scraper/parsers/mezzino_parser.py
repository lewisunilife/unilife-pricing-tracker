import re
from typing import Any, Dict, List, Tuple

from playwright.async_api import Page

from . import common
from .base import parse_with_selector_plan

MEZZINO_ROOM_PRICE_RE = re.compile(
    r"From\s*[£Ł]?\s*(\d{2,5}(?:\.\d{1,2})?)\s*(?:p/w|pw|per\s*week|weekly)\s+([A-Za-z0-9&\-\s\(\)']+?)\s+(Available|Sold Out|Waitlist|Unavailable)",
    re.IGNORECASE,
)


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    await common.click_common(page)
    await page.wait_for_timeout(1400)
    body = common.normalize_currency_text(await page.inner_text("body"))
    page_ay = common.normalise_academic_year(body)

    rows: List[Dict[str, Any]] = []
    for match in MEZZINO_ROOM_PRICE_RE.finditer(body):
        price_raw = match.group(1)
        room_raw = common.normalize_space(match.group(2))
        availability = common.infer_availability(match.group(3))
        room_name = common.clean_room_name(room_raw)
        if not room_name:
            continue
        price = common.parse_price_to_weekly_numeric(f"{price_raw} per week")
        rows.append(
            {
                "Property": "Cumberland Place",
                "Room Name": room_name,
                "Contract Length": "",
                "Academic Year": page_ay,
                "Price": price,
                "Contract Value": None,
                "Floor Level": "",
                "Incentives": common.extract_and_normalise_incentives(body),
                "Availability": availability,
                "Source URL": page.url or src["url"],
                "__missing_price_reason": common.classify_missing_price_reason(room_raw, availability) if price is None else "",
            }
        )

    if not rows:
        fallback_rows, reason = await parse_with_selector_plan(
            page,
            src,
            title_selectors=["h3", "h4", ".room-title", ".title", ".room__title"],
            scope_selectors=[".room-card", '[class*="room"]', "article", '[class*="tile"]'],
        )
        return fallback_rows, reason

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Academic Year", "")),
            row.get("Price"),
            common.normalize_space(row.get("Availability", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, ""
