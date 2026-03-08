from typing import Any, Dict, List, Tuple

from playwright.async_api import Page

from . import common


async def parse_with_selector_plan(
    page: Page,
    src: Dict[str, str],
    title_selectors: List[str],
    scope_selectors: List[str],
    include_non_room_titles: bool = False,
) -> Tuple[List[Dict[str, Any]], str]:
    await common.click_common(page)
    await page.wait_for_timeout(800)
    body = await page.inner_text("body")
    page_ay = common.normalise_academic_year(body)
    property_incentives = common.extract_and_normalise_incentives(body)

    cards = await common.parse_cards_by_selectors(page, title_selectors, scope_selectors)
    rows: List[Dict[str, Any]] = []
    room_incentive_map: Dict[str, str] = {}
    link_candidates: List[Dict[str, str]] = []

    for card in cards:
        title = common.clean_room_name(card.get("title", ""))
        text = common.normalize_currency_text(card.get("text", ""))
        booking_url = common.normalize_space(card.get("booking_url", ""))
        if not title:
            title = common.clean_room_name(text[:120])

        if not title and include_non_room_titles:
            raw = common.normalize_space(card.get("title", ""))
            title = raw if common.is_room_like(raw) else ""

        if not title:
            continue

        card_incentives = common.extract_and_normalise_incentives(text, property_incentives)
        room_incentive_map[common.normalize_key(title)] = card_incentives

        price = common.parse_price_to_weekly_numeric(card.get("price", ""))
        if price is None:
            price = common.parse_price_to_weekly_numeric(text)

        availability = common.normalize_space(card.get("availability", "")) or common.infer_availability(text)
        rows.append(
            {
                "Room Name": title,
                "Contract Length": common.extract_contract_length(text),
                "Price": price,
                "Contract Value": common.parse_contract_value_numeric(text),
                "Floor Level": common.normalise_floor_level(text),
                "Academic Year": common.normalise_academic_year(text) or page_ay,
                "Incentives": card_incentives,
                "Availability": availability,
                "Source URL": src["url"],
                "__missing_price_reason": common.classify_missing_price_reason(text, availability),
            }
        )

        if booking_url:
            link_candidates.append(
                {
                    "href": booking_url,
                    "room_hint": title,
                    "text": common.normalize_space(card.get("title", "")),
                }
            )

    discovered_links = await common.collect_booking_links(page, title_selectors, scope_selectors, max_links=30)
    link_candidates.extend(discovered_links)
    # Dedupe links while preserving first room hint.
    deduped_links: List[Dict[str, str]] = []
    seen_links = set()
    for item in link_candidates:
        href = common.normalize_space(item.get("href", ""))
        if not href or href in seen_links:
            continue
        seen_links.add(href)
        deduped_links.append(item)

    deep_rows: List[Dict[str, Any]] = []
    deep_attempts = 0
    deep_success = 0
    for item in deduped_links[:25]:
        href = common.normalize_space(item.get("href", ""))
        if not href:
            continue
        deep_attempts += 1
        deep_page = await page.context.new_page()
        try:
            ok = await common.safe_goto(deep_page, href, timeout=90000)
            if not ok:
                continue
            parsed = await common.parse_contract_rows_from_page(
                deep_page,
                source_url=href,
                room_hint=item.get("room_hint", ""),
                default_incentives=property_incentives,
            )
            if not parsed:
                continue
            deep_success += 1
            for row in parsed:
                row_room = common.normalize_key(row.get("Room Name", ""))
                if row_room and room_incentive_map.get(row_room):
                    row["Incentives"] = common.extract_and_normalise_incentives(row.get("Incentives", ""), room_incentive_map[row_room])
                deep_rows.append(row)
        finally:
            await deep_page.close()

    if deep_rows:
        deep_room_keys = {common.normalize_key(r.get("Room Name", "")) for r in deep_rows if common.normalize_space(r.get("Room Name", ""))}
        filtered: List[Dict[str, Any]] = []
        for row in rows:
            room_key = common.normalize_key(row.get("Room Name", ""))
            if room_key in deep_room_keys and not common.normalize_space(row.get("Contract Length", "")):
                continue
            filtered.append(row)
        rows = filtered + deep_rows

    if not rows:
        # Operator-level fallback: line-pair extraction from visible body text.
        lines = [common.normalize_space(x) for x in body.splitlines() if common.normalize_space(x)]
        for i, line in enumerate(lines):
            if not any(x in line.lower() for x in ["\u00a3", "\u0141", "pppw", "pw", "per week", "pcm", "per month", "monthly"]):
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
            availability = common.infer_availability(context)
            rows.append(
                {
                    "Room Name": room,
                    "Contract Length": common.extract_contract_length(context),
                    "Price": price,
                    "Contract Value": common.parse_contract_value_numeric(context),
                    "Floor Level": common.normalise_floor_level(context),
                    "Academic Year": common.normalise_academic_year(context) or page_ay,
                    "Incentives": common.extract_and_normalise_incentives(context, property_incentives),
                    "Availability": availability,
                    "Source URL": src["url"],
                    "__missing_price_reason": common.classify_missing_price_reason(context, availability),
                }
            )

    if rows:
        out: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            key = (
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
        return out, ""

    if deep_attempts > 0 and deep_success == 0:
        return [], "hidden_deeper_in_flow"
    return [], "parser_selector_failure"
