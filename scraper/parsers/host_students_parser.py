import re
from typing import Any, Dict, List, Tuple
from urllib.parse import parse_qs, urlparse

from playwright.async_api import Page

from . import common
from .base import parse_with_selector_plan

HOST_CONTRACT_RE = re.compile(
    r"(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+([0-9]{1,2}\s*Weeks?\s*\([^)]+\))\s+[£Ł]?\s*(\d{2,5}(?:\.\d{1,2})?)\s*Weekly",
    re.IGNORECASE,
)


def _room_from_url(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    raw = ""
    if "searchcriteria" in query and query["searchcriteria"]:
        raw = query["searchcriteria"][0]
    m = re.search(r"PROPERTYTYPE:([A-Z_]+)", raw, flags=re.IGNORECASE)
    if not m:
        return ""
    token = m.group(1).replace("_", " ").strip().title()
    token = token.replace("Ensuite", "En Suite")
    return token


def _extract_ay_from_contract(text: str) -> str:
    ay = common.normalise_academic_year(text)
    if ay:
        return ay
    m = re.search(r"\((\d{2})\s*-\s*(\d{2})\)", text)
    if not m:
        return ""
    return common.normalise_academic_year(f"20{m.group(1)}/20{m.group(2)}")


def _extract_rows_from_text(text: str, source_url: str, room_hint: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for match in HOST_CONTRACT_RE.finditer(common.normalize_currency_text(text)):
        contract_blob = common.normalize_space(match.group(3))
        price_amount = common.parse_price_to_weekly_numeric(f"{match.group(4)} weekly")
        room_name = room_hint or "Room"
        rows.append(
            {
                "Property": "Southampton Crossings",
                "Room Name": room_name,
                "Contract Length": common.extract_contract_length(contract_blob) or contract_blob.upper(),
                "Academic Year": _extract_ay_from_contract(contract_blob),
                "Price": price_amount,
                "Contract Value": None,
                "Floor Level": "",
                "Incentives": common.extract_and_normalise_incentives(text),
                "Availability": "Available" if price_amount is not None else "Unknown",
                "Source URL": source_url,
                "__missing_price_reason": common.classify_missing_price_reason(contract_blob, "Available")
                if price_amount is None
                else "",
            }
        )
    return rows


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    await common.click_common(page)
    await page.wait_for_timeout(1200)

    rows: List[Dict[str, Any]] = []
    body = await page.inner_text("body")
    rows.extend(_extract_rows_from_text(body, page.url or src["url"], _room_from_url(page.url or src["url"])))

    search_links = await page.evaluate(
        r"""
        () => {
          const out = [];
          const seen = new Set();
          for (const a of document.querySelectorAll('a[href*="search-accommodation"]')) {
            const href = (a.href || '').trim();
            if (!href) continue;
            if (!/PROPERTYTYPE:/i.test(href)) continue;
            if (seen.has(href)) continue;
            seen.add(href);
            out.push(href);
          }
          return out.slice(0, 20);
        }
        """
    )

    deep_page = await page.context.new_page()
    try:
        for link in search_links:
            ok = await common.safe_goto(deep_page, link, timeout=120000)
            if not ok:
                continue
            await common.click_common(deep_page)
            await deep_page.wait_for_timeout(1600)
            deep_text = await deep_page.inner_text("body")
            room_hint = _room_from_url(link)
            rows.extend(_extract_rows_from_text(deep_text, deep_page.url or link, room_hint))
    finally:
        await deep_page.close()

    if not rows:
        # Fall back to the generic parser for partial room capture.
        fallback_rows, reason = await parse_with_selector_plan(
            page,
            src,
            title_selectors=["h3", "h4", ".room-title", ".title"],
            scope_selectors=[".room-card", '[class*="room"]', "article", '[class*="listing"]'],
        )
        return fallback_rows, reason

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Academic Year", "")),
            row.get("Price"),
            common.normalize_space(row.get("Source URL", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, ""
