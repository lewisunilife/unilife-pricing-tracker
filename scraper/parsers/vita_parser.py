import re
from typing import Any, Dict, List, Tuple
from urllib.parse import parse_qs, urlparse

from playwright.async_api import Page

from . import common
from .base import parse_with_selector_plan

VITA_PRICE_OPTION_RE = re.compile(
    r"(Annual|2\s*Instalments?|4\s*Instalments?)\s*(?:[^\d]{0,3})\s*(\d{2,5}(?:\.\d{1,2})?)\s*per\s*week",
    re.IGNORECASE,
)
DURATION_RE = re.compile(r"\b(\d{1,2})\s*Weeks?\b", re.IGNORECASE)


async def _dismiss_cookies(page: Page) -> None:
    for label in ("Accept All", "Accept", "Allow all"):
        try:
            loc = page.get_by_role("button", name=re.compile(re.escape(label), re.IGNORECASE))
            if await loc.count():
                await loc.first.click(timeout=1200)
                await page.wait_for_timeout(250)
        except Exception:
            continue


def _academic_year_from_url(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    raw = " ".join(query.get("academicYear", []))
    return common.normalise_academic_year(raw)


def _duration_options(text: str) -> List[str]:
    out: List[str] = []
    for m in DURATION_RE.finditer(text):
        value = f"{m.group(1)} WEEKS"
        if value not in out:
            out.append(value)
    return out


async def _parse_view_room_page(view_page: Page, url: str) -> List[Dict[str, Any]]:
    ok = await common.safe_goto(view_page, url, timeout=120000)
    if not ok:
        return []
    try:
        await view_page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    await _dismiss_cookies(view_page)
    await view_page.wait_for_timeout(3200)

    base_body = common.normalize_currency_text(await view_page.inner_text("body"))
    if "per week" not in base_body.lower() and "/pw" not in base_body.lower():
        await view_page.wait_for_timeout(2500)
        base_body = common.normalize_currency_text(await view_page.inner_text("body"))
    room_name = await view_page.evaluate(
        r"""
        () => {
          const title = document.querySelector('h1,h2,.vita-room-details__title,.title');
          return (title?.innerText || '').replace(/\s+/g, ' ').trim();
        }
        """
    )
    room_name = common.clean_room_name(room_name)
    if not room_name:
        return []

    ay = common.normalise_academic_year(base_body) or _academic_year_from_url(view_page.url or url)
    duration_values = _duration_options(base_body)
    if not duration_values:
        duration_values = [""]

    rows: List[Dict[str, Any]] = []
    visited = set()
    for duration in duration_values:
        if duration:
            try:
                click_target = view_page.get_by_text(duration, exact=False)
                if await click_target.count():
                    await click_target.first.click(timeout=1800)
                    await view_page.wait_for_timeout(800)
            except Exception:
                pass

        body = common.normalize_currency_text(await view_page.inner_text("body"))
        for m in VITA_PRICE_OPTION_RE.finditer(body):
            plan = common.normalize_space(m.group(1)).upper()
            price = common.parse_price_to_weekly_numeric(f"{m.group(2)} per week")
            if price is None:
                continue
            contract_length = duration
            if plan:
                contract_length = f"{duration} - {plan}" if duration else plan

            availability = common.infer_availability(body)
            if availability == "Unknown":
                availability = "Available"

            key = (room_name, contract_length, ay, price)
            if key in visited:
                continue
            visited.add(key)

            rows.append(
                {
                    "Property": "Richmond House",
                    "Room Name": room_name,
                    "Contract Length": contract_length,
                    "Academic Year": ay,
                    "Price": price,
                    "Contract Value": None,
                    "Floor Level": "",
                    "Incentives": common.extract_and_normalise_incentives(body),
                    "Availability": availability,
                    "Source URL": view_page.url or url,
                    "__missing_price_reason": "",
                }
            )
    return rows


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    await _dismiss_cookies(page)
    await page.wait_for_timeout(1500)

    room_links = await page.evaluate(
        r"""
        () => {
          const out = [];
          const seen = new Set();
          for (const a of document.querySelectorAll('a[href*="view-room-"]')) {
            const href = (a.href || '').trim();
            if (!href || seen.has(href)) continue;
            seen.add(href);
            out.push(href);
          }
          return out.slice(0, 40);
        }
        """
    )

    rows: List[Dict[str, Any]] = []
    view_page = await page.context.new_page()
    try:
        for link in room_links:
            rows.extend(await _parse_view_room_page(view_page, link))
    finally:
        await view_page.close()

    if rows:
        return rows, ""

    fallback_rows, reason = await parse_with_selector_plan(
        page,
        src,
        title_selectors=["h3", "h4", ".room-title", ".title", '[data-testid*="room"]'],
        scope_selectors=[".room-card", '[class*="room"]', "article", '[class*="booking"]'],
    )
    return fallback_rows, reason
