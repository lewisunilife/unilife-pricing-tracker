import re
from typing import Any, Dict, List, Tuple

from playwright.async_api import Page

from . import common


def _extract_booking_contract(page_text: str) -> Tuple[str, str]:
    booking_line = ""
    for line in page_text.splitlines():
        clean = common.normalize_space(line)
        if "booking for" in clean.lower():
            booking_line = clean
            break
    if not booking_line:
        return "", ""

    contract = ""
    wk = re.search(r"\b(\d{1,2})\s*wks?\b", booking_line, flags=re.IGNORECASE)
    if wk:
        contract = f"{wk.group(1)} WEEKS"

    ay = common.normalise_academic_year(booking_line)
    if not ay:
        ay_match = re.search(r"\b(20\d{2}|\d{2})\s*[-/]\s*(20\d{2}|\d{2})\b", booking_line)
        if ay_match:
            ay = common.normalise_academic_year(f"{ay_match.group(1)}/{ay_match.group(2)}")

    return contract, ay


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    await common.click_common(page)
    await page.wait_for_timeout(1200)

    body = common.normalize_currency_text(await page.inner_text("body"))
    booking_contract, booking_ay = _extract_booking_contract(body)

    cards = await page.evaluate(
        r"""
        () => {
          const out = [];
          const nodes = document.querySelectorAll('.item-result.ui-card-result');
          for (const node of nodes) {
            const text = (node.innerText || '').replace(/\s+/g, ' ').trim();
            if (!text) continue;
            const roomTypeMatch = text.match(/Room Type:\s*([^|]+?)(?:\s+Available Spaces|\s+Show room details|$)/i);
            const roomCodeMatch = text.match(/\b([A-Z]{1,4}-\d{2,4})\b/);
            const availableSpacesMatch = text.match(/Available Spaces in Flat:\s*(\d+)/i);
            const rateNode = node.querySelector('.rate');
            const priceText = ((rateNode && rateNode.innerText) ? rateNode.innerText : '').replace(/\s+/g, ' ').trim();
            out.push({
              text,
              room_type: roomTypeMatch ? roomTypeMatch[1].trim() : '',
              room_code: roomCodeMatch ? roomCodeMatch[1].trim() : '',
              available_spaces: availableSpacesMatch ? availableSpacesMatch[1].trim() : '',
              price_text: priceText,
            });
          }
          return out;
        }
        """
    )

    rows: List[Dict[str, Any]] = []
    for card in cards:
        text = common.normalize_currency_text(card.get("text", ""))
        room_name = common.clean_room_name(card.get("room_type", "")) or common.clean_room_name(text)
        if not room_name:
            continue

        price = common.parse_price_to_weekly_numeric(card.get("price_text", "")) or common.parse_price_to_weekly_numeric(text)
        spaces = common.normalize_space(card.get("available_spaces", ""))
        availability = "Available"
        if spaces.isdigit() and int(spaces) <= 0:
            availability = "Sold Out"
        elif re.search(r"\bsold out\b", text, flags=re.IGNORECASE):
            availability = "Sold Out"
        else:
            availability = common.infer_availability(text)

        row = {
            "Property": "The Walls",
            "Room Name": room_name,
            "Contract Length": booking_contract or common.extract_contract_length(text),
            "Academic Year": booking_ay or common.normalise_academic_year(text),
            "Price": price,
            "Contract Value": common.parse_contract_value_numeric(text),
            "Floor Level": common.normalise_floor_level(text),
            "Incentives": common.extract_and_normalise_incentives(text),
            "Availability": availability,
            "Source URL": page.url or src["url"],
            "__missing_price_reason": common.classify_missing_price_reason(text, availability) if price is None else "",
        }
        rows.append(row)

    if not rows:
        return [], "no starrez room cards"

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Academic Year", "")),
            row.get("Price"),
            common.normalize_space(row.get("Availability", "")),
            common.normalize_space(row.get("Source URL", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, ""
