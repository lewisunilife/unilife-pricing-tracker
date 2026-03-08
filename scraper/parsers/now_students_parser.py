import re
from typing import Any, Dict, List, Tuple

from playwright.async_api import Page

from . import common


def _clean_now_room_name(raw: str) -> str:
    text = common.normalize_space(raw)
    text = re.sub(r",\s*portswood\s+road.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bportswood\s+road\b.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d+\s*sqm\b", "", text, flags=re.IGNORECASE)
    text = common.normalize_space(text)
    cleaned = common.clean_room_name(text)
    return cleaned or text


async def _collect_year_toggles(page: Page) -> List[str]:
    return await page.evaluate(
        r"""
        () => {
          const out = [];
          const seen = new Set();
          for (const node of document.querySelectorAll('label,button,a,span,p')) {
            const t = (node.textContent || '').replace(/\s+/g, ' ').trim();
            if (!t) continue;
            if (!/\b20\d{2}\s*[-/]\s*20\d{2}\b/.test(t)) continue;
            if (seen.has(t)) continue;
            seen.add(t);
            out.push(t);
          }
          return out.slice(0, 6);
        }
        """
    )


async def _click_year_toggle(page: Page, label: str) -> None:
    if not label:
        return
    try:
        locator = page.get_by_text(label, exact=False)
        if await locator.count():
            await locator.first.click(timeout=2000)
            await page.wait_for_timeout(900)
    except Exception:
        pass


async def _collect_room_cards(page: Page) -> List[Dict[str, str]]:
    return await page.evaluate(
        r"""
        () => {
          const out = [];
          const seen = new Set();
          const selectors = ['article', '.swiper-slide', '.room-card', '[class*="room"]'];
          for (const sel of selectors) {
            for (const node of document.querySelectorAll(sel)) {
              const text = (node.innerText || '').replace(/\s+/g, ' ').trim();
              if (!text || text.length < 40 || text.length > 1400) continue;
              if (!/(studio|room|suite|ensuite|apartment|flat)/i.test(text)) continue;

              const titleNode = node.querySelector('h1,h2,h3,h4,.title,[class*="title"]');
              const title = (titleNode?.innerText || '').replace(/\s+/g, ' ').trim();
              const priceNode = node.querySelector('[class*="price"], .price');
              const priceText = (priceNode?.innerText || '').replace(/\s+/g, ' ').trim();
              const bookingNode = node.querySelector('a[href*="concurrent"],a[href*="book"],a[href*="reserve"],a[href*="tenancy"],a[href*="contract"]');
              const bookingUrl = (bookingNode?.href || '').trim();
              if (!title) continue;
              const key = `${title}||${bookingUrl}`;
              if (seen.has(key)) continue;
              seen.add(key);
              out.push({
                title,
                text,
                price_text: priceText,
                booking_url: bookingUrl,
              });
            }
          }
          return out.slice(0, 120);
        }
        """
    )


async def _extract_option_blocks(page: Page) -> List[str]:
    return await page.evaluate(
        r"""
        () => {
          const out = [];
          const seen = new Set();
          const selectors = [
            'label.new--relative',
            'label[class*="new--relative"]',
            'label[class*="border-2px"]',
            '[id^="availability-"]',
            '[class*="availability"]',
          ];
          for (const sel of selectors) {
            for (const node of document.querySelectorAll(sel)) {
              const text = (node.innerText || '').replace(/\s+/g, ' ').trim();
              if (!text || text.length < 8 || text.length > 800) continue;
              if (!/(week|pppw|pw|per week|available from|sold out|waitlist|flexible|contract|tenancy)/i.test(text)) continue;
              if (seen.has(text)) continue;
              seen.add(text);
              out.push(text);
            }
          }
          return out.slice(0, 120);
        }
        """
    )


async def _parse_booking_page(
    booking_page: Page,
    booking_url: str,
    room_hint: str,
    room_incentives: str,
    property_incentives: str,
) -> List[Dict[str, Any]]:
    await common.click_common(booking_page)
    await booking_page.wait_for_timeout(1000)
    body = common.normalize_currency_text(await booking_page.inner_text("body"))

    heading = await booking_page.evaluate(
        r"""
        () => {
          const nodes = [...document.querySelectorAll('h1,h2,h3,.title,[class*="heading"]')];
          for (const n of nodes) {
            const t = (n.innerText || '').replace(/\s+/g, ' ').trim();
            if (t && t.length < 120) return t;
          }
          return '';
        }
        """
    )
    room_name = _clean_now_room_name(heading or room_hint)
    page_ay = common.normalise_academic_year(body)

    rows: List[Dict[str, Any]] = []
    option_blocks = await _extract_option_blocks(booking_page)
    for block in option_blocks:
        option_text = common.normalize_currency_text(block)
        contract_length = common.extract_contract_length(option_text)
        academic_year = common.normalise_academic_year(option_text) or page_ay
        price = common.parse_price_to_weekly_numeric(option_text)
        contract_value = common.parse_contract_value_numeric(option_text)
        availability = common.infer_availability(option_text)
        if availability == "Unknown" and price is not None:
            availability = "Available"
        incentives = common.extract_and_normalise_incentives(option_text, room_incentives, property_incentives, body)

        if (
            not contract_length
            and not academic_year
            and price is None
            and contract_value is None
            and availability == "Unknown"
            and not incentives
        ):
            continue

        rows.append(
            {
                "Room Name": room_name,
                "Contract Length": contract_length,
                "Academic Year": academic_year,
                "Price": price,
                "Contract Value": contract_value,
                "Floor Level": common.normalise_floor_level(option_text),
                "Incentives": incentives,
                "Availability": availability,
                "Source URL": booking_page.url or booking_url,
                "__missing_price_reason": common.classify_missing_price_reason(option_text, availability) if price is None else "",
            }
        )

    if rows:
        merged: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for row in rows:
            key = (
                common.normalize_space(row.get("Room Name", "")),
                common.normalize_space(row.get("Contract Length", "")),
                common.normalize_space(row.get("Academic Year", "")),
                row.get("Price"),
                common.normalize_space(row.get("Source URL", "")),
            )
            if key not in merged:
                merged[key] = dict(row)
                continue
            base = merged[key]
            if base.get("Contract Value") is None and row.get("Contract Value") is not None:
                base["Contract Value"] = row.get("Contract Value")
            if common.normalize_space(base.get("Availability", "")) == "Unknown" and common.normalize_space(row.get("Availability", "")) != "Unknown":
                base["Availability"] = row.get("Availability")
            base["Incentives"] = common.extract_and_normalise_incentives(base.get("Incentives", ""), row.get("Incentives", ""))
            if not common.normalize_space(base.get("__missing_price_reason", "")):
                base["__missing_price_reason"] = row.get("__missing_price_reason", "")
            merged[key] = base
        return list(merged.values())

    # Shallow fallback if booking options are not rendered.
    fallback_price = common.parse_price_to_weekly_numeric(body)
    fallback_row = {
        "Room Name": room_name,
        "Contract Length": common.extract_contract_length(body),
        "Academic Year": common.normalise_academic_year(body),
        "Price": fallback_price,
        "Contract Value": common.parse_contract_value_numeric(body),
        "Floor Level": common.normalise_floor_level(body),
        "Incentives": common.extract_and_normalise_incentives(body, room_incentives, property_incentives),
        "Availability": common.infer_availability(body),
        "Source URL": booking_page.url or booking_url,
        "__missing_price_reason": common.classify_missing_price_reason(body, common.infer_availability(body))
        if fallback_price is None
        else "",
    }
    if fallback_row["Room Name"]:
        return [fallback_row]
    return []


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    await common.click_common(page)
    await page.wait_for_timeout(1200)
    property_body = common.normalize_currency_text(await page.inner_text("body"))
    property_incentives = common.extract_and_normalise_incentives(property_body)

    year_toggles = await _collect_year_toggles(page)
    # Collect card links for each visible year state.
    all_cards: List[Dict[str, str]] = []
    seen_cards = set()
    for year in [""] + year_toggles:
        if year:
            await _click_year_toggle(page, year)
        cards = await _collect_room_cards(page)
        for card in cards:
            key = (common.normalize_space(card.get("title", "")), common.normalize_space(card.get("booking_url", "")))
            if key in seen_cards:
                continue
            seen_cards.add(key)
            all_cards.append(card)

    if not all_cards:
        return [], "parser_selector_failure"

    rows: List[Dict[str, Any]] = []
    deep_success = 0
    for card in all_cards:
        room_name = _clean_now_room_name(card.get("title", ""))
        room_text = common.normalize_currency_text(card.get("text", ""))
        room_incentives = common.extract_and_normalise_incentives(room_text, property_incentives)
        booking_url = common.normalize_space(card.get("booking_url", ""))
        if not booking_url:
            # Keep room row even without a link when room is visible.
            availability = common.infer_availability(room_text)
            rows.append(
                {
                    "Room Name": room_name,
                    "Contract Length": common.extract_contract_length(room_text),
                    "Academic Year": common.normalise_academic_year(room_text),
                    "Price": common.parse_price_to_weekly_numeric(card.get("price_text", "")) or common.parse_price_to_weekly_numeric(room_text),
                    "Contract Value": common.parse_contract_value_numeric(room_text),
                    "Floor Level": common.normalise_floor_level(room_text),
                    "Incentives": room_incentives,
                    "Availability": availability,
                    "Source URL": src["url"],
                    "__missing_price_reason": common.classify_missing_price_reason(room_text, availability)
                    if (common.parse_price_to_weekly_numeric(card.get("price_text", "")) is None and common.parse_price_to_weekly_numeric(room_text) is None)
                    else "",
                }
            )
            continue

        booking_page = await page.context.new_page()
        try:
            ok = await common.safe_goto(booking_page, booking_url, timeout=90000)
            if not ok:
                continue
            contract_rows = await _parse_booking_page(
                booking_page,
                booking_url=booking_url,
                room_hint=room_name,
                room_incentives=room_incentives,
                property_incentives=property_incentives,
            )
            if contract_rows:
                deep_success += 1
                rows.extend(contract_rows)
        finally:
            await booking_page.close()

    if not rows and deep_success == 0:
        return [], "hidden_deeper_in_flow"

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Academic Year", "")),
            row.get("Price"),
            row.get("Contract Value"),
            common.normalize_space(row.get("Availability", "")),
            common.normalize_space(row.get("Source URL", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    if deduped:
        return deduped, ""
    return [], "parser_selector_failure"
