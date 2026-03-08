from typing import Dict, List, Tuple

from playwright.async_api import Page

from . import common


async def parse_with_selector_plan(
    page: Page,
    src: Dict[str, str],
    title_selectors: List[str],
    scope_selectors: List[str],
    include_non_room_titles: bool = False,
) -> Tuple[List[Dict[str, str]], str]:
    await common.click_common(page)
    await page.wait_for_timeout(800)
    body = await page.inner_text("body")
    page_ay = common.normalise_academic_year(body)
    property_incentives = common.extract_and_normalise_incentives(body)

    cards = await common.parse_cards_by_selectors(page, title_selectors, scope_selectors)
    rows: List[Dict[str, str]] = []
    for card in cards:
        title = common.clean_room_name(card.get("title", ""))
        text = common.normalize_currency_text(card.get("text", ""))
        if not title:
            # Fallback: choose first room-like line from card text.
            parts = [common.normalize_space(x) for x in text.split(" ")]
            joined = " ".join(parts[:12])
            title = common.clean_room_name(joined)

        if not title and include_non_room_titles:
            raw = common.normalize_space(card.get("title", ""))
            title = raw if common.is_room_like(raw) else ""

        if not title:
            continue

        price = common.parse_price_to_weekly_numeric(card.get("price", ""))
        if price is None:
            price = common.parse_price_to_weekly_numeric(text)

        rows.append(
            {
                "Room Name": title,
                "Contract Length": common.extract_contract_length(text),
                "Price": price,
                "Floor Level": common.normalise_floor_level(text),
                "Academic Year": common.normalise_academic_year(text) or page_ay,
                "Incentives": common.extract_and_normalise_incentives(text, property_incentives),
                "Availability": common.normalize_space(card.get("availability", "")) or common.infer_availability(text),
                "Source URL": src["url"],
            }
        )

    if rows:
        return rows, ""

    # Operator-level fallback: line-pair extraction from visible body text.
    lines = [common.normalize_space(x) for x in body.splitlines() if common.normalize_space(x)]
    for i, line in enumerate(lines):
        if not any(x in line.lower() for x in ["£", "ł", "pw", "per week", "pcm", "per month", "monthly"]):
            continue
        price = common.parse_price_to_weekly_numeric(line)
        if price is None:
            continue

        room = ""
        for j in range(max(0, i - 3), i):
            cand = common.clean_room_name(lines[j])
            if cand:
                room = cand
        if not room:
            continue

        context = " | ".join(lines[max(0, i - 2) : min(len(lines), i + 3)])
        rows.append(
            {
                "Room Name": room,
                "Contract Length": common.extract_contract_length(context),
                "Price": price,
                "Floor Level": common.normalise_floor_level(context),
                "Academic Year": common.normalise_academic_year(context) or page_ay,
                "Incentives": common.extract_and_normalise_incentives(context, property_incentives),
                "Availability": common.infer_availability(context),
                "Source URL": src["url"],
            }
        )

    if rows:
        # local dedupe
        out = []
        seen = set()
        for r in rows:
            k = (r["Room Name"], r["Contract Length"], r["Price"], r["Source URL"])
            if k in seen:
                continue
            seen.add(k)
            out.append(r)
        return out, ""

    return [], "no extractable room rows"
