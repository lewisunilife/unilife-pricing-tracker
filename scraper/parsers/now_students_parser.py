import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

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


def _booking_api_url(booking_url: str, path: str) -> str:
    parsed = urlparse(booking_url)
    query = parsed.query.strip("&")
    q = f"{query}&step=property" if query else "step=property"
    return f"{parsed.scheme}://{parsed.netloc}{path}?{q}"


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = common.normalize_space(value).replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _normalise_floor_list(floors: List[str]) -> str:
    out: List[str] = []
    for raw in floors:
        floor = common.normalise_floor_level(raw)
        if floor and floor.lower() not in [x.lower() for x in out]:
            out.append(floor)
    return " | ".join(out)


def _extract_room_floor_hint(room_text: str) -> str:
    text = common.normalize_currency_text(room_text)
    if not text:
        return ""
    if not re.search(
        r"\b(lower\s*ground|ground|floors?|level\s*\d+|\d{1,2}(?:st|nd|rd|th)\s*floor|first|second|third|fourth|fifth|sixth)\b",
        text,
        flags=re.IGNORECASE,
    ):
        return ""
    return common.normalise_floor_level(text)


def _allowed_now_availability(value: str, price: Optional[float], has_options: Optional[bool] = None) -> str:
    text = common.normalize_currency_text(value)
    low = text.lower()
    inferred = common.infer_availability(text)
    if inferred in {"Available", "Sold Out", "Limited Availability", "Waitlist", "Unavailable"}:
        return inferred
    if "sold out" in low:
        return "Sold Out"
    if "waitlist" in low or "wait list" in low:
        return "Waitlist"
    if "limited" in low or "last few" in low or "few left" in low:
        return "Limited Availability"
    if "unavailable" in low or "not available" in low:
        return "Unavailable"
    if price is not None:
        return "Available"
    if has_options is False:
        return "Unavailable"
    return "Unavailable"


def _extract_contract_value_from_context(*texts: Any) -> Optional[float]:
    for value in texts:
        contract_value = common.parse_contract_value_numeric(value)
        if contract_value is not None:
            return contract_value

    for value in texts:
        text = common.normalize_currency_text(value)
        if not text or not re.search(r"\brent\b", text, flags=re.IGNORECASE):
            continue
        amount_match = re.search(r"[£$]\s*([\d,]+(?:\.\d{1,2})?)", text)
        if not amount_match:
            continue
        try:
            amount = round(float(amount_match.group(1).replace(",", "")), 2)
        except ValueError:
            continue
        if amount >= 500:
            return amount
    return None


def _contract_length_from_option_text(text: str) -> str:
    value = common.normalize_currency_text(text)
    if re.search(r"\bflexible\s*stay\b", value, flags=re.IGNORECASE):
        return "FLEXIBLE STAY"
    return common.extract_contract_length(value)


def _dedupe_now_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str, str, Optional[float], str], List[Dict[str, Any]]] = {}
    for row in rows:
        base_key = (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Academic Year", "")),
            row.get("Price"),
            common.normalize_space(row.get("Availability", "")),
        )
        grouped.setdefault(base_key, []).append(row)

    deduped: List[Dict[str, Any]] = []
    for group_rows in grouped.values():
        merged_rows: List[Dict[str, Any]] = []
        for row in group_rows:
            row_floor = common.normalize_space(row.get("Floor Level", ""))
            matched = False
            for existing in merged_rows:
                existing_floor = common.normalize_space(existing.get("Floor Level", ""))
                if existing_floor and row_floor and existing_floor != row_floor:
                    continue
                # Merge duplicate option rows while preserving richer values.
                if not existing_floor and row_floor:
                    existing["Floor Level"] = row.get("Floor Level", "")
                if existing.get("Contract Value") is None and row.get("Contract Value") is not None:
                    existing["Contract Value"] = row.get("Contract Value")
                existing["Incentives"] = common.extract_and_normalise_incentives(existing.get("Incentives", ""), row.get("Incentives", ""))
                if not common.normalize_space(existing.get("Source URL", "")) and common.normalize_space(row.get("Source URL", "")):
                    existing["Source URL"] = row.get("Source URL", "")
                if existing.get("Price") is None and row.get("Price") is not None:
                    existing["Price"] = row.get("Price")
                matched = True
                break
            if not matched:
                merged_rows.append(dict(row))
        deduped.extend(merged_rows)
    return deduped


def _parse_contract_fields(option_data: Dict[str, Any]) -> Tuple[str, str, Optional[float], str]:
    title = common.normalize_currency_text(common.normalize_space(option_data.get("title", "")))
    contract_length = _contract_length_from_option_text(title)
    if not contract_length:
        min_stay = _as_float(option_data.get("minStay"))
        if min_stay and min_stay >= 1:
            contract_length = f"{int(round(min_stay))} DAYS"

    academic_year = common.normalise_academic_year(title)

    price = common.parse_price_to_weekly_numeric(option_data.get("price"))
    if price is None:
        raw_price = common.normalize_space(option_data.get("price", ""))
        if raw_price:
            price = common.parse_price_to_weekly_numeric(f"{raw_price} pppw")
    if price is None:
        price = common.parse_price_to_weekly_numeric(title)

    option_text = common.normalize_currency_text(
        " ".join(
            [
                title,
                common.normalize_space(option_data.get("moveInDate", "")),
                common.normalize_space(option_data.get("moveOutDate", "")),
                common.normalize_space(option_data.get("description", "")),
                common.normalize_space(option_data.get("contractDescription", "")),
            ]
        )
    )
    return contract_length, academic_year, price, option_text


def _availability_from_option(option_data: Dict[str, Any], room_options: List[Dict[str, Any]], price: Optional[float]) -> str:
    title = common.normalize_currency_text(common.normalize_space(option_data.get("title", "")))
    inferred = common.infer_availability(title)
    room_count = _as_float(option_data.get("numberOfRooms"))
    has_options = bool(room_options)
    if room_count is not None and room_count <= 0 and not has_options:
        has_options = False
    return _allowed_now_availability(title, price=price, has_options=has_options)


def _contract_value_from_rent_plans(plan_options: List[Dict[str, Any]]) -> Optional[float]:
    plans: List[Tuple[str, List[float]]] = []
    for item in plan_options:
        data = (item or {}).get("data", {}) or {}
        title = common.normalize_space(data.get("title", ""))
        instalments = data.get("instalments", []) or []
        amounts: List[float] = []
        for row in instalments:
            amount = _as_float((row or {}).get("amount"))
            if amount is not None:
                amounts.append(amount)
        if amounts:
            plans.append((title, amounts))

    if not plans:
        return None

    for title, amounts in plans:
        if re.search(r"full\s*payment", title, flags=re.IGNORECASE):
            return round(sum(amounts), 2)
    return round(sum(plans[0][1]), 2)


async def _get_json(page: Page, url: str, method: str = "get", payload: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    try:
        if method.lower() == "post":
            response = await page.request.post(url, data=payload or {}, timeout=90000)
        else:
            response = await page.request.get(url, timeout=90000)
    except Exception:
        return None
    if not response.ok:
        return None
    try:
        data = await response.json()
    except Exception:
        return None
    return data if isinstance(data, dict) else None


async def _parse_booking_page_via_api(
    booking_page: Page,
    booking_url: str,
    room_hint: str,
    room_floor_hint: str,
    room_incentives: str,
    property_incentives: str,
) -> List[Dict[str, Any]]:
    room_name = _clean_now_room_name(room_hint)
    if not room_name:
        room_name = "Studio"

    booking_api = _booking_api_url(booking_url, "/tenancy/signing-api/booking")
    rooms_api = _booking_api_url(booking_url, "/tenancy/signing-api/booking/rooms")
    rent_plans_api = _booking_api_url(booking_url, "/tenancy/signing-api/booking/rent-instalments")

    booking_payload = await _get_json(booking_page, booking_api, method="get")
    if not booking_payload:
        return []

    availability = booking_payload.get("availability", {}) or {}
    options = availability.get("options", []) or []
    if not options:
        return []

    rows: List[Dict[str, Any]] = []
    for option in options:
        option_data = (option or {}).get("data", {}) or {}
        contract_length, academic_year, price, option_text = _parse_contract_fields(option_data)

        room_payload = {
            "availabilityId": option_data.get("availabilityId"),
            "startDate": option_data.get("moveInDate"),
            "endDate": option_data.get("moveOutDate"),
            "useDA": bool(option_data.get("useDA", False)),
            "useGuarantorWaiver": bool(option_data.get("useGuarantorWaiver", False)),
        }
        rooms_payload = await _get_json(booking_page, rooms_api, method="post", payload=room_payload) or {}
        room_options = rooms_payload.get("options", []) if isinstance(rooms_payload, dict) else []
        room_options = room_options if isinstance(room_options, list) else []

        floor_level = common.normalise_floor_level(option_text) or room_floor_hint

        room_id = None
        if room_options:
            room_id = (room_options[0] or {}).get("value")

        rent_plan_payload = dict(room_payload)
        rent_plan_payload["occupancy"] = "single"
        if room_id is not None:
            rent_plan_payload["roomId"] = room_id
        rent_plans = await _get_json(booking_page, rent_plans_api, method="post", payload=rent_plan_payload) or {}
        rent_plan_options = rent_plans.get("options", []) if isinstance(rent_plans, dict) else []
        rent_plan_options = rent_plan_options if isinstance(rent_plan_options, list) else []

        contract_value = _contract_value_from_rent_plans(rent_plan_options)
        if contract_value is None:
            contract_value = _extract_contract_value_from_context(option_text)

        availability_status = _availability_from_option(option_data, room_options, price)
        incentives = common.extract_and_normalise_incentives(option_text, room_incentives, property_incentives)

        rows.append(
            {
                "Room Name": room_name,
                "Contract Length": contract_length,
                "Academic Year": academic_year,
                "Price": price,
                "Contract Value": contract_value,
                "Floor Level": floor_level,
                "Incentives": incentives,
                "Availability": _allowed_now_availability(option_text, price=price, has_options=bool(room_options))
                or availability_status,
                "Source URL": booking_url,
                "__missing_price_reason": common.classify_missing_price_reason(option_text, availability_status)
                if price is None
                else "",
            }
        )

    return _dedupe_now_rows(rows)


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


async def _parse_booking_page_dom_fallback(
    booking_page: Page,
    booking_url: str,
    room_hint: str,
    room_floor_hint: str,
    room_incentives: str,
    property_incentives: str,
) -> List[Dict[str, Any]]:
    await common.click_common(booking_page)
    await booking_page.wait_for_timeout(900)
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
    for block in await _extract_option_blocks(booking_page):
        option_text = common.normalize_currency_text(block)
        contract_length = common.extract_contract_length(option_text)
        academic_year = common.normalise_academic_year(option_text) or page_ay
        price = common.parse_price_to_weekly_numeric(option_text)
        contract_value = common.parse_contract_value_numeric(option_text)
        availability = _allowed_now_availability(option_text, price=price)
        incentives = common.extract_and_normalise_incentives(option_text, room_incentives, property_incentives, body)

        if not any([contract_length, academic_year, price is not None, contract_value is not None, incentives, availability != "Unknown"]):
            continue

        rows.append(
            {
                "Room Name": room_name,
                "Contract Length": contract_length,
                "Academic Year": academic_year,
                "Price": price,
                "Contract Value": contract_value,
                "Floor Level": common.normalise_floor_level(option_text) or room_floor_hint,
                "Incentives": incentives,
                "Availability": availability,
                "Source URL": booking_page.url or booking_url,
                "__missing_price_reason": common.classify_missing_price_reason(option_text, availability) if price is None else "",
            }
        )

    if rows:
        return rows

    fallback_price = common.parse_price_to_weekly_numeric(body)
    fallback_row = {
        "Room Name": room_name,
        "Contract Length": common.extract_contract_length(body),
        "Academic Year": common.normalise_academic_year(body) or page_ay,
        "Price": fallback_price,
        "Contract Value": common.parse_contract_value_numeric(body),
        "Floor Level": room_floor_hint,
        "Incentives": common.extract_and_normalise_incentives(body, room_incentives, property_incentives),
        "Availability": _allowed_now_availability(body, price=fallback_price),
        "Source URL": booking_page.url or booking_url,
        "__missing_price_reason": common.classify_missing_price_reason(body, _allowed_now_availability(body, price=fallback_price))
        if fallback_price is None
        else "",
    }
    return [fallback_row] if fallback_row["Room Name"] else []


async def _collect_booking_option_nodes(booking_page: Page) -> List[Dict[str, Any]]:
    return await booking_page.evaluate(
        r"""
        () => {
          const out = [];
          const labels = [...document.querySelectorAll('label.new--relative, label[class*="new--relative"]')];
          labels.forEach((node, idx) => {
            const text = (node.innerText || '').replace(/\s+/g, ' ').trim();
            if (!text || text.length < 6 || text.length > 900) return;
            if (!/(week|pppw|pw|per week|flexible|stay|minimum stay|days|sold out|waitlist|availability)/i.test(text)) return;
            out.push({ index: idx, text });
          });
          return out.slice(0, 120);
        }
        """
    )


async def _extract_selected_booking_state(booking_page: Page) -> Dict[str, str]:
    return await booking_page.evaluate(
        r"""
        () => {
          const norm = (v) => (v || '').replace(/\s+/g, ' ').trim();
          const checked = document.querySelector('input[type="radio"]:checked');
          const checkedLabel = checked ? (checked.closest('label.new--relative') || checked.closest('label')) : null;
          const selectedText = norm(checkedLabel?.innerText || '');
          const roomTitle = norm(document.querySelector('main h1, h1')?.textContent || '');
          const summaryTitle = norm(document.querySelector('aside h3, [role="complementary"] h3')?.textContent || '');
          const summaryBlocks = [...document.querySelectorAll('aside, [role="complementary"]')].map((n) => norm(n.innerText || ''));
          const summaryText = norm(summaryBlocks.join(' '));
          let rentValue = '';
          for (const node of document.querySelectorAll('aside p, [role="complementary"] p, aside div, [role="complementary"] div')) {
            const t = norm(node.innerText || '');
            if (/^£\s*[\d,]+(?:\.\d{1,2})?$/.test(t)) {
              rentValue = t;
              break;
            }
          }
          return {
            selected_text: selectedText,
            room_title: roomTitle,
            summary_title: summaryTitle,
            summary_text: summaryText,
            rent_value: rentValue,
          };
        }
        """
    )


async def _parse_booking_page_via_clickthrough(
    booking_page: Page,
    booking_url: str,
    room_hint: str,
    room_floor_hint: str,
    room_incentives: str,
    property_incentives: str,
) -> List[Dict[str, Any]]:
    await common.click_common(booking_page)
    await booking_page.wait_for_timeout(700)
    option_nodes = await _collect_booking_option_nodes(booking_page)
    if not option_nodes:
        return []

    rows: List[Dict[str, Any]] = []
    option_labels = booking_page.locator("label.new--relative, label[class*='new--relative']")

    for option in option_nodes:
        option_text = common.normalize_currency_text(common.normalize_space(option.get("text", "")))
        option_index = int(option.get("index", 0))
        clicked = False

        if not clicked:
            try:
                if option_index < await option_labels.count():
                    await option_labels.nth(option_index).click(timeout=2500, force=True)
                    clicked = True
            except Exception:
                clicked = False

        if not clicked:
            continue

        await booking_page.wait_for_timeout(500)
        selected = await _extract_selected_booking_state(booking_page)
        selected_text = common.normalize_currency_text(common.normalize_space(selected.get("selected_text", "")))
        summary_text = common.normalize_currency_text(common.normalize_space(selected.get("summary_text", "")))
        rent_value = common.normalize_currency_text(common.normalize_space(selected.get("rent_value", "")))
        context_text = common.normalize_currency_text(" ".join([option_text, selected_text, summary_text]))

        room_name = _clean_now_room_name(
            selected.get("room_title", "") or selected.get("summary_title", "") or room_hint
        )
        if not room_name:
            room_name = _clean_now_room_name(room_hint)
        if not room_name:
            continue

        contract_length = _contract_length_from_option_text(selected_text) or _contract_length_from_option_text(option_text)
        if not contract_length:
            contract_length = _contract_length_from_option_text(context_text)
        academic_year = common.normalise_academic_year(selected_text) or common.normalise_academic_year(option_text)
        if not academic_year:
            academic_year = common.normalise_academic_year(summary_text)
        price = common.parse_price_to_weekly_numeric(selected_text) or common.parse_price_to_weekly_numeric(option_text)
        if price is None:
            price = common.parse_price_to_weekly_numeric(context_text)

        contract_value = _extract_contract_value_from_context(selected_text, option_text, summary_text, rent_value)
        floor_level = common.normalise_floor_level(" ".join([selected_text, option_text]))
        if not floor_level:
            floor_level = room_floor_hint
        incentives = common.extract_and_normalise_incentives(option_text, selected_text, room_incentives, property_incentives)
        availability = _allowed_now_availability(" ".join([selected_text, option_text, summary_text]), price=price)

        rows.append(
            {
                "Room Name": room_name,
                "Contract Length": contract_length,
                "Academic Year": academic_year,
                "Price": price,
                "Contract Value": contract_value,
                "Floor Level": floor_level,
                "Incentives": incentives,
                "Availability": availability,
                "Source URL": common.normalize_space(booking_page.url) or booking_url,
                "__missing_price_reason": common.classify_missing_price_reason(context_text, availability) if price is None else "",
            }
        )

    return _dedupe_now_rows(rows)


async def _parse_booking_page(
    booking_page: Page,
    booking_url: str,
    room_hint: str,
    room_floor_hint: str,
    room_incentives: str,
    property_incentives: str,
) -> List[Dict[str, Any]]:
    click_rows = await _parse_booking_page_via_clickthrough(
        booking_page,
        booking_url=booking_url,
        room_hint=room_hint,
        room_floor_hint=room_floor_hint,
        room_incentives=room_incentives,
        property_incentives=property_incentives,
    )
    if click_rows:
        return click_rows

    api_rows = await _parse_booking_page_via_api(
        booking_page,
        booking_url=booking_url,
        room_hint=room_hint,
        room_floor_hint=room_floor_hint,
        room_incentives=room_incentives,
        property_incentives=property_incentives,
    )
    if api_rows:
        return api_rows
    return await _parse_booking_page_dom_fallback(
        booking_page,
        booking_url=booking_url,
        room_hint=room_hint,
        room_floor_hint=room_floor_hint,
        room_incentives=room_incentives,
        property_incentives=property_incentives,
    )


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    await common.click_common(page)
    await page.wait_for_timeout(1200)
    property_body = common.normalize_currency_text(await page.inner_text("body"))
    property_incentives = common.extract_and_normalise_incentives(property_body)

    year_toggles = await _collect_year_toggles(page)
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
        room_floor_hint = _extract_room_floor_hint(room_text)
        room_incentives = common.extract_and_normalise_incentives(room_text, property_incentives)
        booking_url = common.normalize_space(card.get("booking_url", ""))

        if not booking_url:
            availability = common.infer_availability(room_text)
            price = common.parse_price_to_weekly_numeric(card.get("price_text", "")) or common.parse_price_to_weekly_numeric(room_text)
            rows.append(
                {
                    "Room Name": room_name,
                    "Contract Length": common.extract_contract_length(room_text),
                    "Academic Year": common.normalise_academic_year(room_text),
                    "Price": price,
                    "Contract Value": _extract_contract_value_from_context(room_text),
                    "Floor Level": room_floor_hint,
                    "Incentives": room_incentives,
                    "Availability": _allowed_now_availability(room_text, price=price)
                    if availability == "Unknown"
                    else _allowed_now_availability(availability, price=price),
                    "Source URL": src["url"],
                    "__missing_price_reason": common.classify_missing_price_reason(room_text, availability) if price is None else "",
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
                room_floor_hint=room_floor_hint,
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

    deduped = _dedupe_now_rows(rows)
    return (deduped, "") if deduped else ([], "parser_selector_failure")
