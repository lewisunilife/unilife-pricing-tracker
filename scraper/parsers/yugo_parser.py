import re
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

from playwright.async_api import Page

from . import common
from .base import parse_with_selector_plan

ROOM_LINE_RE = re.compile(r"^[A-Z][A-Z0-9 &'/-]{4,}$")
PRICE_LINE_RE = re.compile(r"from\s*[£Ł]?\s*(\d{2,5}(?:\.\d{1,2})?)\s*/?\s*(?:week|pw|p/w)", re.IGNORECASE)


def _property_from_url(url: str) -> str:
    parts = [p for p in urlparse(url).path.strip("/").split("/") if p]
    if not parts:
        return ""
    # property slug usually lives at the end unless path ends in /rooms or a room-detail slug.
    slug = parts[-1]
    if slug.lower() == "rooms" and len(parts) >= 2:
        slug = parts[-2]
    elif re.search(r"-\d{5,}$", slug) and len(parts) >= 2:
        slug = parts[-2]
    slug = slug.replace("-", " ")
    return common.proper_case_property(slug, "")


def _parse_rows_from_lines(body: str, source_url: str, property_name: str) -> List[Dict[str, Any]]:
    lines = [common.normalize_space(x) for x in body.splitlines() if common.normalize_space(x)]
    rows: List[Dict[str, Any]] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        room_candidate = line.upper() == line and ROOM_LINE_RE.match(line)
        if not room_candidate:
            i += 1
            continue

        room_name = common.clean_room_name(line.title()) or common.clean_room_name(line)
        if not room_name:
            i += 1
            continue

        window = " | ".join(lines[i : min(len(lines), i + 6)])
        price = None
        availability = common.infer_availability(window)
        incentives = common.extract_and_normalise_incentives(" | ".join(lines[max(0, i - 3) : min(len(lines), i + 6)]))

        for j in range(i + 1, min(len(lines), i + 6)):
            price_line = lines[j]
            price_hit = PRICE_LINE_RE.search(price_line)
            if not price_hit:
                continue
            price = common.parse_price_to_weekly_numeric(f"{price_hit.group(1)} per week")
            break

        if price is not None and availability == "Unknown":
            availability = "Available"
        if availability == "Waitlist" and price is None:
            availability = "Unavailable"

        rows.append(
            {
                "Property": property_name,
                "Room Name": room_name,
                "Contract Length": common.extract_contract_length(window),
                "Academic Year": common.normalise_academic_year(window),
                "Price": price,
                "Contract Value": None,
                "Floor Level": "",
                "Incentives": incentives,
                "Availability": availability,
                "Source URL": source_url,
                "__missing_price_reason": common.classify_missing_price_reason(window, availability) if price is None else "",
            }
        )
        i += 1
    return rows


async def _collect_target_urls(page: Page, current_url: str) -> List[str]:
    urls = [current_url]
    discovered = await page.evaluate(
        r"""
        () => {
          const out = [];
          const seen = new Set();
          for (const a of document.querySelectorAll('a[href]')) {
            const href = (a.href || '').trim();
            if (!href) continue;
            const low = href.toLowerCase();
            if (!low.includes('/southampton/')) continue;
            if (!(low.includes('/rooms') || /\/southampton\/[^/]+\/[^/]+$/i.test(low))) continue;
            if (seen.has(href)) continue;
            seen.add(href);
            out.push(href);
          }
          return out.slice(0, 40);
        }
        """
    )
    for url in discovered:
        if url not in urls:
            urls.append(url)
    return urls


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    await common.click_common(page)
    await page.wait_for_timeout(1200)

    target_urls = await _collect_target_urls(page, page.url or src["url"])
    all_rows: List[Dict[str, Any]] = []

    deep = await page.context.new_page()
    try:
        for url in target_urls:
            ok = await common.safe_goto(deep, url, timeout=120000)
            if not ok:
                continue
            await common.click_common(deep)
            await deep.wait_for_timeout(800)
            body = await deep.inner_text("body")
            prop = _property_from_url(deep.url or url)
            all_rows.extend(_parse_rows_from_lines(body, deep.url or url, prop))
    finally:
        await deep.close()

    if all_rows:
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

    fallback_rows, reason = await parse_with_selector_plan(
        page,
        src,
        title_selectors=["h3", "h4", ".room-title", ".title", '[class*="room-name"]'],
        scope_selectors=[".room-card", '[class*="room"]', "article", '[class*="suite"]'],
    )
    return fallback_rows, reason
