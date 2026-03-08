import re
from typing import Any, Dict, List, Tuple

from playwright.async_api import Page

from . import common


def _crm_room_name_from_url(url: str) -> str:
    slug = url.rstrip("/").split("/")[-1]
    slug = re.sub(r"-\d+$", "", slug)
    slug = slug.replace("-", " ")
    return common.clean_room_name(slug.title())


async def _parse_room_detail(page: Page, url: str) -> Dict[str, Any] | None:
    ok = await common.safe_goto(page, url, timeout=90000)
    if not ok:
        return None
    await common.click_common(page)
    await page.wait_for_timeout(700)
    body = common.normalize_currency_text(await page.inner_text("body"))

    room_name = _crm_room_name_from_url(page.url)
    if not room_name:
        heading = await page.evaluate(
            r"""
            () => {
              const node = document.querySelector('h1,h2,.title,[class*="heading"]');
              return (node?.innerText || '').replace(/\s+/g, ' ').trim();
            }
            """
        )
        room_name = common.clean_room_name(heading)
    if not room_name:
        return None

    price = common.parse_price_to_weekly_numeric(body)
    availability = common.infer_availability(body)
    if availability == "Unknown" and price is not None:
        availability = "Available"

    return {
        "Property": "The Bank",
        "Room Name": room_name,
        "Contract Length": common.extract_contract_length(body),
        "Academic Year": common.normalise_academic_year(body),
        "Price": price,
        "Contract Value": common.parse_contract_value_numeric(body),
        "Floor Level": common.normalise_floor_level(room_name),
        "Incentives": common.extract_and_normalise_incentives(body),
        "Availability": availability,
        "Source URL": page.url or url,
        "__missing_price_reason": common.classify_missing_price_reason(body, availability) if price is None else "",
    }


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    await common.click_common(page)
    await page.wait_for_timeout(1200)
    body = common.normalize_currency_text(await page.inner_text("body"))

    room_links = await page.evaluate(
        r"""
        () => {
          const out = [];
          const seen = new Set();
          for (const a of document.querySelectorAll('a[href]')) {
            const href = (a.href || '').trim();
            if (!href) continue;
            if (!/\/southampton\/the-bank\/.+-\d+\/?$/i.test(href)) continue;
            if (seen.has(href)) continue;
            seen.add(href);
            out.push(href);
          }
          return out.slice(0, 40);
        }
        """
    )

    rows: List[Dict[str, Any]] = []
    detail_page = await page.context.new_page()
    try:
        for link in room_links:
            row = await _parse_room_detail(detail_page, link)
            if row:
                rows.append(row)
    finally:
        await detail_page.close()

    if rows:
        deduped: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            key = (
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

    # Secondary fallback: parse cards directly from rooms page text.
    fallback_rows: List[Dict[str, Any]] = []
    cards = await page.evaluate(
        r"""
        () => {
          const out = [];
          const seen = new Set();
          for (const node of document.querySelectorAll('article,[class*="room"],[class*="card"]')) {
            const text = (node.innerText || '').replace(/\s+/g, ' ').trim();
            if (!text || text.length < 25 || text.length > 1200) continue;
            if (!/studio|room/i.test(text)) continue;
            const titleNode = node.querySelector('h2,h3,h4,.title,[class*="title"]');
            const title = (titleNode?.innerText || '').replace(/\s+/g, ' ').trim();
            const key = `${title}||${text}`;
            if (seen.has(key)) continue;
            seen.add(key);
            out.push({title, text});
          }
          return out.slice(0, 120);
        }
        """
    )

    for card in cards:
        text = common.normalize_currency_text(card.get("text", ""))
        room_name = common.clean_room_name(card.get("title", "")) or common.clean_room_name(text)
        if not room_name:
            continue
        price = common.parse_price_to_weekly_numeric(text)
        availability = common.infer_availability(text)
        fallback_rows.append(
            {
                "Property": "The Bank",
                "Room Name": room_name,
                "Contract Length": common.extract_contract_length(text),
                "Academic Year": common.normalise_academic_year(text) or common.normalise_academic_year(body),
                "Price": price,
                "Contract Value": common.parse_contract_value_numeric(text),
                "Floor Level": common.normalise_floor_level(text),
                "Incentives": common.extract_and_normalise_incentives(text),
                "Availability": availability,
                "Source URL": page.url or src["url"],
                "__missing_price_reason": common.classify_missing_price_reason(text, availability) if price is None else "",
            }
        )

    if fallback_rows:
        return fallback_rows, ""
    return [], "crm room links produced no rows"
