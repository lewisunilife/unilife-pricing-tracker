import re
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

from playwright.async_api import Page

from . import common
from .base import parse_with_selector_plan

ROOM_LINE_RE = re.compile(r"^[A-Z][A-Z0-9 &'/-]{4,}$")
PRICE_LINE_RE = re.compile(
    r"from\s*(?:[^\d]{0,3})\s*(\d{2,5}(?:\.\d{1,2})?)\s*/?\s*(?:week|pw|p/w)",
    re.IGNORECASE,
)

AUSTEN_ROOMS_URL = "https://yugo.com/en-gb/global/united-kingdom/southampton/austen-house/rooms"
AUSTEN_ROOM_NAME_MAP = [
    ("classic en suite", "Classic En Suite"),
    ("classic studio", "Classic Studio"),
    ("premium en suite", "Premium En Suite"),
    ("premium studio", "Premium Studio"),
    ("standard en suite", "Standard En Suite"),
    ("standard studio", "Standard Studio"),
    ("standard room - 2 bed flat", "Standard Room - 2 Bed Flat"),
]


async def _dismiss_cookies(page: Page) -> None:
    for label in ("Accept All Cookies", "Accept All", "Accept"):
        try:
            loc = page.get_by_role("button", name=re.compile(re.escape(label), re.IGNORECASE))
            if await loc.count() and await loc.first.is_visible():
                await loc.first.click(timeout=1200)
                await page.wait_for_timeout(250)
                break
        except Exception:
            continue


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


def _normalise_floor_name(raw: str) -> str:
    text = common.normalize_space(raw).lower()
    mapping = {
        "lower gr": "Lower Ground",
        "lower ground": "Lower Ground",
        "ground": "Ground",
        "floor 01": "First",
        "floor 1": "First",
        "floor 02": "Second",
        "floor 2": "Second",
        "floor 03": "Third",
        "floor 3": "Third",
        "floor 04": "Fourth",
        "floor 4": "Fourth",
        "floor 05": "Fifth",
        "floor 5": "Fifth",
        "floor 06": "Sixth",
        "floor 6": "Sixth",
    }
    return mapping.get(text, "")


def _austen_room_name(raw: str) -> str:
    text = common.normalize_space(raw)
    low = text.lower()
    for key, label in AUSTEN_ROOM_NAME_MAP:
        if key in low:
            return label
    cleaned = common.clean_room_name(text)
    return cleaned


async def _fetch_austen_floor_levels(page: Page) -> str:
    # Booking-flow sourced floor metadata for Austen House.
    cities_url = "https://yugo.com/en-gb/cities?countryId=598866"
    residences_tpl = "https://yugo.com/en-gb/residences?cityId={city_id}"
    property_tpl = "https://yugo.com/en-gb/residence-property?residenceId={residence_id}&residenceContentId={residence_content_id}"
    try:
        cities_resp = await page.request.get(cities_url, timeout=45000)
        if not cities_resp.ok:
            return ""
        cities_payload = await cities_resp.json()
    except Exception:
        return ""

    city_id = ""
    for city in (cities_payload or {}).get("cities", []):
        if common.normalize_space(city.get("name", "")).lower() == "southampton":
            city_id = str(city.get("contentId", ""))
            break
    if not city_id:
        return ""

    try:
        residences_resp = await page.request.get(residences_tpl.format(city_id=city_id), timeout=45000)
        if not residences_resp.ok:
            return ""
        residences_payload = await residences_resp.json()
    except Exception:
        return ""

    residence_id = ""
    residence_content_id = ""
    for residence in (residences_payload or {}).get("residences", []):
        if common.normalize_space(residence.get("name", "")).lower() == "austen house":
            residence_id = common.normalize_space(residence.get("id", ""))
            residence_content_id = str(residence.get("contentId", ""))
            break
    if not residence_id or not residence_content_id:
        return ""

    try:
        property_resp = await page.request.get(
            property_tpl.format(
                residence_id=residence_id,
                residence_content_id=residence_content_id,
            ),
            timeout=45000,
        )
        if not property_resp.ok:
            return ""
        property_payload = await property_resp.json()
    except Exception:
        return ""

    floors: List[Tuple[float, str]] = []
    for building in ((property_payload or {}).get("property") or {}).get("buildings", []) or []:
        for floor in building.get("floors", []) or []:
            name = _normalise_floor_name(floor.get("name", ""))
            if not name:
                continue
            idx = floor.get("index", 999)
            try:
                idx_value = float(idx)
            except Exception:
                idx_value = 999.0
            floors.append((idx_value, name))

    if not floors:
        return ""

    floors.sort(key=lambda x: x[0])
    ordered: List[str] = []
    for _, name in floors:
        if name not in ordered:
            ordered.append(name)
    return " | ".join(ordered)


async def _parse_austen_room_cards(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    target_url = src["url"]
    if "/austen-house/rooms" not in target_url:
        target_url = AUSTEN_ROOMS_URL
    ok = await common.safe_goto(page, target_url, timeout=120000)
    if not ok:
        return [], "austen rooms page unavailable"
    await _dismiss_cookies(page)
    await page.wait_for_timeout(1200)

    floor_level = await _fetch_austen_floor_levels(page)

    cards = await page.evaluate(
        r"""
        () => {
          const out = [];
          const nodes = document.querySelectorAll('article.search-results__item');
          for (const node of nodes) {
            const text = (node.innerText || '').replace(/\s+/g, ' ').trim();
            if (!text) continue;
            const roomAnchor = node.querySelector('a[href*="/austen-house/"][href*="-"]');
            const roomTitle = (roomAnchor?.innerText || '').replace(/\s+/g, ' ').trim();
            const roomHref = (roomAnchor?.href || '').trim();
            out.push({ text, room_title: roomTitle, room_href: roomHref });
          }
          return out;
        }
        """
    )

    rows: List[Dict[str, Any]] = []
    for card in cards:
        card_text = common.normalize_currency_text(card.get("text", ""))
        room_title = common.normalize_space(card.get("room_title", ""))
        room_name = _austen_room_name(room_title) or _austen_room_name(card_text)
        if not room_name:
            continue

        price = common.parse_price_to_weekly_numeric(card_text)
        sold_out = bool(re.search(r"\bsold out\b", card_text, flags=re.IGNORECASE))
        waitlist = bool(re.search(r"\bjoin waitlist|waitlist\b", card_text, flags=re.IGNORECASE))

        if sold_out:
            availability = "Sold Out"
        elif waitlist and price is None:
            availability = "Unavailable"
        else:
            availability = "Available" if price is not None else "Unknown"

        rows.append(
            {
                "Property": "Austen House",
                "Room Name": room_name,
                "Contract Length": "",
                "Academic Year": common.normalise_academic_year(card_text),
                "Price": price,
                "Contract Value": None,
                "Floor Level": floor_level,
                "Incentives": common.extract_and_normalise_incentives(card_text),
                "Availability": availability,
                "Source URL": common.normalize_space(card.get("room_href", "")) or target_url,
                "__missing_price_reason": common.classify_missing_price_reason(card_text, availability) if price is None else "",
            }
        )

    if not rows:
        return [], "no austen room cards parsed"

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            common.normalize_space(row.get("Room Name", "")),
            row.get("Price"),
            common.normalize_space(row.get("Availability", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, ""


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
    # Austen House has a dedicated parser path to avoid room-card cross-contamination.
    if "/austen-house" in (src.get("url", "") or "").lower():
        return await _parse_austen_room_cards(page, src)

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
