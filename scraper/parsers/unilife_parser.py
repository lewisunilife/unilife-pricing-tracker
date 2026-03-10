import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from playwright.async_api import Page

from . import common


def _room_scoped_property_incentives(room_name: str, property_text: str) -> str:
    room_low = common.normalize_space(room_name).lower()
    tokens = common.extract_and_normalise_incentives(property_text)
    if not tokens:
        return ""
    keep: List[str] = []
    for t in tokens.split(" | "):
        low = t.lower()
        if "premium plus" in low and "premium plus" not in room_low:
            continue
        if ("twodio" in low or "kitchen & bedding pack" in low) and "twodio" not in room_low:
            continue
        if "bus pass" in low and "premium plus" not in room_low:
            continue
        keep.append(t)
    return " | ".join(keep)


def _scope_incentives(room_name: str, incentives_text: str) -> str:
    room_low = common.normalize_space(room_name).lower()
    if not incentives_text:
        return ""
    kept: List[str] = []
    for token in incentives_text.split(" | "):
        low = token.lower()
        if "bus pass" in low and "premium plus" not in room_low:
            continue
        if ("kitchen & bedding pack" in low or "twodio" in low) and "twodio" not in room_low:
            continue
        kept.append(token)
    return " | ".join(kept)


AY_SCAN_RE = re.compile(r"(?:AY\s*)?((?:20)?\d{2})\s*[/\-]\s*((?:20)?\d{2})", re.IGNORECASE)
CONTRACT_ANY_RE = re.compile(r"\b\d{1,3}\s*(?:weeks?|months?|days?)\b", re.IGNORECASE)
CONCURRENT_LINK_RE = re.compile(r"concurrent\.co\.uk/tenancy/signing", re.IGNORECASE)
_FLOOR_TOKEN_TO_NUM = {
    "lower ground": -1,
    "lower gr": -1,
    "ground": 0,
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
    "fifth": 5,
    "sixth": 6,
    "seventh": 7,
    "eighth": 8,
    "ninth": 9,
    "tenth": 10,
}
_NUM_TO_FLOOR = {
    -1: "Lower Ground",
    0: "Ground",
    1: "First",
    2: "Second",
    3: "Third",
    4: "Fourth",
    5: "Fifth",
    6: "Sixth",
    7: "Seventh",
    8: "Eighth",
    9: "Ninth",
    10: "Tenth",
}

_TRACKING_QUERY_KEYS = {"_gl", "_ga", "_gid", "gclid", "fbclid"}


def _canonical_booking_url(url: str) -> str:
    raw = common.normalize_space(url)
    if not raw:
        return ""
    parts = urlsplit(raw)
    if not parts.query:
        return raw
    clean_pairs = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        key_low = key.lower()
        if key_low in _TRACKING_QUERY_KEYS or key_low.startswith("utm_") or key_low.startswith("_ga"):
            continue
        clean_pairs.append((key, value))
    query = urlencode(clean_pairs, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))

def _extract_best_academic_year(*texts: Any) -> str:
    counts: Dict[str, int] = {}
    for text in texts:
        value = common.normalize_space(text)
        if not value:
            continue
        for match in AY_SCAN_RE.finditer(value):
            candidate = common.normalise_academic_year(f"{match.group(1)}/{match.group(2)}")
            if not candidate:
                continue
            counts[candidate] = counts.get(candidate, 0) + 1
    if not counts:
        return ""
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _token_to_floor_num(token: str) -> Optional[int]:
    raw = common.normalize_space(token).lower().replace("forth", "fourth")
    if not raw:
        return None
    if raw in _FLOOR_TOKEN_TO_NUM:
        return _FLOOR_TOKEN_TO_NUM[raw]
    match = re.search(r"(\d{1,2})(?:st|nd|rd|th)?", raw)
    if match:
        return int(match.group(1))
    return None


def _nums_to_floor_text(nums: List[int]) -> str:
    if not nums:
        return ""
    deduped = sorted(set(nums))
    labels = [_NUM_TO_FLOOR.get(n, "") for n in deduped if n in _NUM_TO_FLOOR]
    labels = [label for label in labels if label]
    if not labels:
        return ""
    if len(deduped) >= 2 and deduped == list(range(deduped[0], deduped[-1] + 1)):
        first = _NUM_TO_FLOOR.get(deduped[0], "")
        last = _NUM_TO_FLOOR.get(deduped[-1], "")
        if first and last and first != last:
            return f"{first} to {last}"
    return " | ".join(labels)


def _extract_unilife_floor(text: Any) -> str:
    value = common.normalize_space(text).replace("forth", "fourth")
    if not value:
        return ""
    low = value.lower()

    grouped = re.search(
        r"(?:located\s+on|on)?\s*"
        r"((?:lower\s*gr(?:ound)?|ground|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|\d{1,2}(?:st|nd|rd|th)?)"
        r"(?:\s*(?:,|&|and)\s*"
        r"(?:lower\s*gr(?:ound)?|ground|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|\d{1,2}(?:st|nd|rd|th)?))+)"
        r"\s*floors?",
        low,
        flags=re.IGNORECASE,
    )
    if grouped:
        tokens = re.split(r"\s*(?:,|&|and)\s*", grouped.group(1), flags=re.IGNORECASE)
        nums = [num for num in (_token_to_floor_num(token) for token in tokens) if num is not None]
        floor = _nums_to_floor_text(nums)
        if floor:
            return floor

    explicit_range = re.search(
        r"(lower\s*gr(?:ound)?|ground|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|\d{1,2}(?:st|nd|rd|th)?)"
        r"\s*(?:-|to)\s*"
        r"(lower\s*gr(?:ound)?|ground|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|\d{1,2}(?:st|nd|rd|th)?)\s*floors?",
        low,
        flags=re.IGNORECASE,
    )
    if explicit_range:
        left = _token_to_floor_num(explicit_range.group(1))
        right = _token_to_floor_num(explicit_range.group(2))
        if left is not None and right is not None:
            return _nums_to_floor_text([left, right])

    single = re.search(
        r"(lower\s*gr(?:ound)?|ground|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|\d{1,2}(?:st|nd|rd|th)?)\s*floors?",
        low,
        flags=re.IGNORECASE,
    )
    if single:
        num = _token_to_floor_num(single.group(1))
        if num is not None and num in _NUM_TO_FLOOR:
            return _NUM_TO_FLOOR[num]

    return common.normalise_floor_level(value)


def _extract_contract_length_unilife(text: Any) -> str:
    value = common.normalize_space(text)
    if not value:
        return ""
    match = CONTRACT_ANY_RE.search(value)
    if match:
        return common.normalize_space(match.group(0)).upper()
    if re.search(r"\bsummer\b", value, flags=re.IGNORECASE):
        return "SUMMER"
    if re.search(r"\bflexible\s*stay\b", value, flags=re.IGNORECASE):
        return "FLEXIBLE STAY"
    return common.extract_contract_length(value)


def _price_key(value: Any) -> str:
    if value is None:
        return ""
    try:
        return f"{float(value):.2f}"
    except Exception:
        return common.normalize_space(value)


def _is_valid_unilife_room_name(value: str) -> bool:
    text = common.normalize_space(value)
    if not text:
        return False
    low = text.lower()
    blocked = [
        "book a room as",
        "book to share",
        "deposit",
        "tenant",
        "per person",
        "book now",
        "available from",
        "minimum stay",
    ]
    if any(token in low for token in blocked):
        return False
    return bool(re.search(r"\b(classic|premium|vip|luxury|twodio|studio|ensuite|en-suite)\b", low))


def _unilife_tile_price(tile_text: str, contract_text: str, modal_text: str):
    price = common.parse_price_to_weekly_numeric(tile_text)
    if price is not None:
        return price
    price = common.parse_price_to_weekly_numeric(contract_text)
    if price is not None:
        return price
    price = common.parse_price_to_weekly_numeric(modal_text)
    if price is not None:
        return price

    # Unilife booking tiles are weekly when tied to explicit week contracts.
    if re.search(r"\b\d{1,2}\s*weeks?\b", contract_text, re.IGNORECASE):
        m = re.search(r"[£Ł]\s*(\d{2,4}(?:,\d{3})*(?:\.\d{1,2})?)", tile_text)
        if m:
            try:
                return round(float(m.group(1).replace(",", "")), 2)
            except ValueError:
                return None
    return None


async def _parse_open_modal(page: Page, src: Dict[str, str], property_text: str, page_ay: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    modal = None
    modals = page.locator(".modal-content__inner")
    count = await modals.count()
    for i in range(count):
        candidate = modals.nth(i)
        try:
            if await candidate.is_visible():
                modal = candidate
                break
        except Exception:
            continue
    if modal is None:
        return rows

    room_name = common.clean_room_name(await modal.locator(".title, h1, h2, h3, .top").first.inner_text())
    if not room_name:
        return rows

    modal_text = common.normalize_space(await modal.inner_text())
    modal_ay = _extract_best_academic_year(modal_text, page_ay)
    modal_avail = ""
    if await modal.locator(".available").count():
        modal_avail = common.normalize_space(await modal.locator(".available").first.inner_text())
    if not modal_avail:
        modal_avail = common.infer_availability(modal_text)

    room_incentives = common.extract_and_normalise_incentives(modal_text)
    property_incentives = _room_scoped_property_incentives(room_name, property_text)
    base_price = common.parse_price_to_weekly_numeric(modal_text)

    cols = modal.locator(".contracts-column")
    col_count = await cols.count()
    wrote = False
    for ci in range(col_count):
        col = cols.nth(ci)
        col_header = common.normalize_space(await col.inner_text())
        contract = _extract_contract_length_unilife(col_header)
        tiles = col.locator(".book")
        tile_count = await tiles.count()
        for ti in range(tile_count):
            tile = tiles.nth(ti)
            tile_text = common.normalize_space(await tile.inner_text())
            price = _unilife_tile_price(tile_text, col_header, modal_text) or base_price

            floor = _extract_unilife_floor(tile_text) or _extract_unilife_floor(modal_text)
            ay = _extract_best_academic_year(tile_text, modal_ay)
            availability = common.infer_availability(tile_text) or modal_avail
            incentives = _scope_incentives(room_name, common.extract_and_normalise_incentives(tile_text, room_incentives, property_incentives))

            rows.append(
                {
                    "Room Name": room_name,
                    "Contract Length": contract or _extract_contract_length_unilife(tile_text),
                    "Price": price,
                    "Contract Value": common.parse_contract_value_numeric(tile_text) or common.parse_contract_value_numeric(modal_text),
                    "Floor Level": floor,
                    "Academic Year": ay,
                    "Incentives": incentives,
                    "Availability": availability,
                    "Source URL": src["url"],
                    "__missing_price_reason": common.classify_missing_price_reason(tile_text, availability) if price is None else "",
                }
            )
            wrote = True

    if not wrote:
        rows.append(
            {
                "Room Name": room_name,
                "Contract Length": _extract_contract_length_unilife(modal_text),
                "Price": base_price,
                "Contract Value": common.parse_contract_value_numeric(modal_text),
                "Floor Level": _extract_unilife_floor(modal_text),
                "Academic Year": modal_ay,
                "Incentives": common.extract_and_normalise_incentives(room_incentives, property_incentives),
                "Availability": modal_avail,
                "Source URL": src["url"],
                "__missing_price_reason": common.classify_missing_price_reason(modal_text, modal_avail) if base_price is None else "",
            }
        )
    return rows


def _guess_room_from_context(text: str, known_rooms: List[str]) -> str:
    value = common.normalize_space(text)
    if not value:
        return ""
    low = value.lower()
    for room in sorted(known_rooms, key=len, reverse=True):
        room_low = room.lower()
        if room_low and room_low in low:
            return room

    match = re.search(r"\bavailable\s+([a-z0-9 \-&()'/]+?)\s+book\s+now\b", value, re.IGNORECASE)
    if match:
        guessed = common.clean_room_name(match.group(1))
        if guessed and _is_valid_unilife_room_name(guessed):
            return guessed

    head = common.normalize_space(value.split("Book now")[0])
    head = common.normalize_space(head.split("Ł")[0])
    guessed = common.clean_room_name(head)
    if _is_valid_unilife_room_name(guessed):
        return guessed
    return ""


async def _collect_booking_candidates(
    page: Page,
    property_text: str,
    page_ay: str,
    room_card_incentives: Dict[str, str],
    room_card_floor: Dict[str, str],
    room_card_price: Dict[str, float],
    known_rooms: List[str],
) -> List[Dict[str, Any]]:
    raw_links = await page.evaluate(
        r"""
        () => {
          const out = [];
          const anchors = [...document.querySelectorAll('a[href*="concurrent.co.uk/tenancy/signing"]')];
          for (const a of anchors) {
            const href = (a.href || '').split('#')[0].trim();
            if (!href) continue;
            let node = a;
            let bestText = (a.innerText || '').replace(/\s+/g, ' ').trim();
            let cardText = '';
            let roomHint = '';
            for (let i = 0; i < 8 && node; i++) {
              const t = (node.innerText || '').replace(/\s+/g, ' ').trim();
              if (t && t.length > bestText.length && t.length < 2400) bestText = t;
              if (!cardText && t && t.length < 1200 && /room|studio|book now|available|week/i.test(t)) {
                cardText = t;
              }
              if (!roomHint) {
                const title = node.querySelector('h1,h2,h3,h4,.title,[class*="title"]');
                if (title && title.innerText) {
                  roomHint = title.innerText.replace(/\s+/g, ' ').trim();
                }
              }
              node = node.parentElement;
            }
            out.push({
              href,
              link_text: (a.innerText || '').replace(/\s+/g, ' ').trim(),
              parent_text: bestText,
              card_text: cardText,
              room_hint: roomHint,
            });
          }
          return out.slice(0, 220);
        }
        """
    )

    merged: Dict[str, Dict[str, Any]] = {}
    for item in raw_links:
        href_raw = common.normalize_space(item.get("href", ""))
        href = _canonical_booking_url(href_raw)
        if not href or not CONCURRENT_LINK_RE.search(href):
            continue
        if href not in merged:
            merged[href] = {"href": href, "room_hint": "", "contexts": []}
        room_hint = common.clean_room_name(item.get("room_hint", ""))
        if room_hint and not _is_valid_unilife_room_name(room_hint):
            room_hint = ""
        if room_hint and not merged[href]["room_hint"]:
            merged[href]["room_hint"] = room_hint
        for key in ["parent_text", "card_text", "link_text"]:
            text = common.normalize_space(item.get(key, ""))
            if text and text not in merged[href]["contexts"]:
                merged[href]["contexts"].append(text)

    candidates: List[Dict[str, Any]] = []
    for href, payload in merged.items():
        context_text = " | ".join(payload.get("contexts", []))
        room_hint = common.normalize_space(payload.get("room_hint", ""))
        if not room_hint:
            room_hint = _guess_room_from_context(context_text, known_rooms)
        if room_hint and not _is_valid_unilife_room_name(room_hint):
            room_hint = ""
        room_key = common.normalize_key(room_hint)

        incentives = common.extract_and_normalise_incentives(
            context_text,
            property_text,
            room_card_incentives.get(room_key, ""),
        )
        incentives = _scope_incentives(room_hint, incentives)

        candidates.append(
            {
                "href": href,
                "room_hint": room_hint,
                "context_text": context_text,
                "contract_hint": _extract_contract_length_unilife(context_text),
                "price_hint": room_card_price.get(room_key)
                if room_key in room_card_price
                else common.parse_price_to_weekly_numeric(context_text),
                "floor_hint": _extract_unilife_floor(context_text) or room_card_floor.get(room_key, ""),
                "ay_hint": _extract_best_academic_year(context_text, page_ay),
                "availability_hint": common.infer_availability(context_text),
                "incentives_hint": incentives,
            }
        )
    candidates.sort(key=lambda c: (0 if common.normalize_space(c.get("room_hint", "")) else 1, 0 if common.normalize_space(c.get("contract_hint", "")) else 1))
    return candidates[:24]


def _select_contract_value_target(rows: List[Dict[str, Any]], candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    subset = rows[:]

    room_hint = common.normalize_space(candidate.get("room_hint", ""))
    if room_hint:
        matching = [r for r in subset if common.normalize_space(r.get("Room Name", "")).lower() == room_hint.lower()]
        if matching:
            subset = matching

    contract_hint = common.normalize_space(candidate.get("contract_hint", ""))
    if contract_hint:
        matching = [r for r in subset if common.normalize_space(r.get("Contract Length", "")).lower() == contract_hint.lower()]
        if matching:
            subset = matching

    price_hint = candidate.get("price_hint")
    if price_hint is not None:
        matching = [r for r in subset if r.get("Price") is not None and _price_key(r.get("Price")) == _price_key(price_hint)]
        if matching:
            subset = matching

    floor_hint = common.normalize_space(candidate.get("floor_hint", ""))
    if floor_hint:
        matching = [r for r in subset if common.normalize_space(r.get("Floor Level", "")).lower() == floor_hint.lower()]
        if matching:
            subset = matching

    if subset:
        without_value = [r for r in subset if r.get("Contract Value") is None]
        if without_value:
            return without_value[0]
        return subset[0]
    return rows[0]


def _parse_room_and_floor_from_option_text(option_text: str, room_hint: str, fallback_floor: str) -> Tuple[str, str]:
    text = common.normalize_space(option_text)
    head = common.normalize_space(text.split(" - ")[0])
    room_name = ""
    floor = ""

    if ":" in head:
        left, right = head.split(":", 1)
        possible_floor = _extract_unilife_floor(right)
        if possible_floor:
            floor = possible_floor
            room_name = common.clean_room_name(left)

    if not room_name:
        room_name = common.clean_room_name(head) or common.clean_room_name(room_hint)

    hint = common.clean_room_name(room_hint)
    if hint:
        if not room_name:
            room_name = hint
        else:
            room_key = common.normalize_key(room_name)
            hint_key = common.normalize_key(hint)
            if room_key.startswith(hint_key) or hint_key.startswith(room_key) or room_key in hint_key or hint_key in room_key:
                room_name = hint

    if not _is_valid_unilife_room_name(room_name):
        room_name = hint if _is_valid_unilife_room_name(hint) else ""

    full_floor = _extract_unilife_floor(text)
    if full_floor:
        floor = full_floor
    if not floor:
        floor = fallback_floor
    return room_name, floor


async def _extract_rows_from_booking_link(
    page: Page,
    src: Dict[str, str],
    candidate: Dict[str, Any],
    property_text: str,
    page_ay: str,
    room_card_incentives: Dict[str, str],
    room_card_floor: Dict[str, str],
    room_card_price: Dict[str, float],
) -> List[Dict[str, Any]]:
    href = candidate["href"]
    deep_page = await page.context.new_page()
    try:
        ok = await common.safe_goto(deep_page, href, timeout=45000)
        if not ok:
            return []

        body_text = common.normalize_currency_text(await deep_page.inner_text("body"))
        body_ay = _extract_best_academic_year(body_text, page_ay, candidate.get("ay_hint", ""))
        body_contract_value = common.parse_contract_value_numeric(body_text)
        option_payload = await deep_page.evaluate(
            r"""
            () => {
              const options = [];
              const selected = [];
              const seen = new Set();
              const selectors = [
                'label',
                '[role="radio"]',
                '[class*="availability"] label',
                '[class*="tenancy"] label',
                '[class*="contract"] label',
              ];
              const push = (raw) => {
                const text = (raw || '').replace(/\s+/g, ' ').trim();
                if (!text || text.length < 10 || text.length > 900) return;
                if (!/(pppw|ppw|pw|weeks?|days?|summer|available from|sold out|waitlist|AY\s*\d|rent:|total for the contract)/i.test(text)) return;
                if (seen.has(text)) return;
                seen.add(text);
                options.push(text);
                if (/rent:|total for the contract/i.test(text)) selected.push(text);
              };
              selectors.forEach(sel => document.querySelectorAll(sel).forEach(node => push(node.innerText || '')));
              return { options, selected };
            }
            """
        )

        option_texts: List[str] = option_payload.get("options", [])
        selected_texts: List[str] = option_payload.get("selected", [])

        rows: List[Dict[str, Any]] = []
        for option_text in option_texts:
            room_name, floor = _parse_room_and_floor_from_option_text(
                option_text,
                room_hint=candidate.get("room_hint", ""),
                fallback_floor=candidate.get("floor_hint", ""),
            )
            if not room_name:
                continue
            room_key = common.normalize_key(room_name)

            contract = _extract_contract_length_unilife(option_text) or candidate.get("contract_hint", "")
            academic_year = _extract_best_academic_year(option_text, body_ay)
            if not floor:
                floor = room_card_floor.get(room_key, "")

            price = common.parse_price_to_weekly_numeric(option_text)
            if price is None:
                if room_key in room_card_price:
                    price = room_card_price[room_key]
                elif candidate.get("price_hint") is not None:
                    price = candidate.get("price_hint")

            availability = common.infer_availability(option_text)
            if not availability or availability == "Unknown":
                availability = candidate.get("availability_hint", "")
            if (not availability or availability == "Unknown") and price is not None:
                availability = "Available"

            incentives = common.extract_and_normalise_incentives(
                option_text,
                candidate.get("incentives_hint", ""),
                room_card_incentives.get(room_key, ""),
                _room_scoped_property_incentives(room_name, property_text),
            )
            incentives = _scope_incentives(room_name, incentives)

            contract_value = common.parse_contract_value_numeric(option_text)
            rows.append(
                {
                    "Room Name": room_name,
                    "Contract Length": contract,
                    "Price": price,
                    "Contract Value": contract_value,
                    "Floor Level": floor,
                    "Academic Year": academic_year,
                    "Incentives": incentives,
                    "Availability": availability or "Unknown",
                    "Source URL": deep_page.url or href,
                    "__missing_price_reason": common.classify_missing_price_reason(candidate.get("context_text", ""), availability)
                    if price is None
                    else "",
                }
            )

        if rows and body_contract_value is not None:
            value_candidate = dict(candidate)
            if selected_texts:
                selected_text = selected_texts[0]
                selected_room, selected_floor = _parse_room_and_floor_from_option_text(
                    selected_text,
                    room_hint=candidate.get("room_hint", ""),
                    fallback_floor=candidate.get("floor_hint", ""),
                )
                value_candidate["room_hint"] = selected_room or candidate.get("room_hint", "")
                value_candidate["contract_hint"] = _extract_contract_length_unilife(selected_text) or candidate.get("contract_hint", "")
                value_candidate["price_hint"] = common.parse_price_to_weekly_numeric(selected_text) or candidate.get("price_hint")
                value_candidate["floor_hint"] = selected_floor or candidate.get("floor_hint", "")
            target = _select_contract_value_target(rows, value_candidate)
            if target and target.get("Contract Value") is None:
                target["Contract Value"] = body_contract_value

        # Fallback to generic parser only when no structured label options were found.
        if not rows:
            parsed = await common.parse_contract_rows_from_page(
                deep_page,
                source_url=href,
                room_hint=candidate.get("room_hint", ""),
                default_incentives=common.extract_and_normalise_incentives(candidate.get("incentives_hint", ""), property_text),
            )
            for parsed_row in parsed:
                room_name = common.clean_room_name(parsed_row.get("Room Name", "")) or common.clean_room_name(candidate.get("room_hint", ""))
                if not room_name:
                    continue
                room_key = common.normalize_key(room_name)
                floor = _extract_unilife_floor(parsed_row.get("Floor Level", "")) or candidate.get("floor_hint", "") or room_card_floor.get(room_key, "")
                contract = _extract_contract_length_unilife(parsed_row.get("Contract Length", "")) or candidate.get("contract_hint", "")
                price = parsed_row.get("Price")
                if price is None and room_key in room_card_price:
                    price = room_card_price[room_key]
                availability = common.normalize_space(parsed_row.get("Availability", "")) or candidate.get("availability_hint", "")
                if (not availability or availability == "Unknown") and price is not None:
                    availability = "Available"
                incentives = _scope_incentives(
                    room_name,
                    common.extract_and_normalise_incentives(
                        parsed_row.get("Incentives", ""),
                        candidate.get("incentives_hint", ""),
                        room_card_incentives.get(room_key, ""),
                    ),
                )
                rows.append(
                    {
                        "Room Name": room_name,
                        "Contract Length": contract,
                        "Price": price,
                        "Contract Value": parsed_row.get("Contract Value"),
                        "Floor Level": floor,
                        "Academic Year": _extract_best_academic_year(parsed_row.get("Academic Year", ""), body_ay),
                        "Incentives": incentives,
                        "Availability": availability or "Unknown",
                        "Source URL": common.normalize_space(parsed_row.get("Source URL", "")) or deep_page.url or href,
                        "__missing_price_reason": common.classify_missing_price_reason(candidate.get("context_text", ""), availability)
                        if price is None
                        else "",
                    }
                )

        if rows:
            return rows

        fallback_room = common.clean_room_name(candidate.get("room_hint", ""))
        if fallback_room:
            fallback_price = candidate.get("price_hint")
            fallback_availability = candidate.get("availability_hint", "") or "Unknown"
            return [
                {
                    "Room Name": fallback_room,
                    "Contract Length": candidate.get("contract_hint", ""),
                    "Price": fallback_price,
                    "Contract Value": body_contract_value,
                    "Floor Level": candidate.get("floor_hint", ""),
                    "Academic Year": candidate.get("ay_hint", "") or body_ay,
                    "Incentives": _scope_incentives(fallback_room, candidate.get("incentives_hint", "")),
                    "Availability": fallback_availability,
                    "Source URL": deep_page.url or href,
                    "__missing_price_reason": common.classify_missing_price_reason(candidate.get("context_text", ""), fallback_availability)
                    if fallback_price is None
                    else "",
                }
            ]
        return []
    finally:
        await deep_page.close()


def _is_concurrent_url(url: Any) -> bool:
    return "concurrent.co.uk" in common.normalize_space(url).lower()


def _same_unilife_row(concurrent_row: Dict[str, Any], brochure_row: Dict[str, Any]) -> bool:
    c_room = common.normalize_space(concurrent_row.get("Room Name", "")).lower()
    b_room = common.normalize_space(brochure_row.get("Room Name", "")).lower()
    if not c_room or not b_room or c_room != b_room:
        return False

    c_contract = common.normalize_space(concurrent_row.get("Contract Length", "")).lower()
    b_contract = common.normalize_space(brochure_row.get("Contract Length", "")).lower()
    if c_contract and b_contract and c_contract != b_contract:
        return False

    c_ay = common.normalize_space(concurrent_row.get("Academic Year", "")).lower()
    b_ay = common.normalize_space(brochure_row.get("Academic Year", "")).lower()
    if c_ay and b_ay and c_ay != b_ay:
        return False

    c_floor = common.normalize_space(concurrent_row.get("Floor Level", "")).lower()
    b_floor = common.normalize_space(brochure_row.get("Floor Level", "")).lower()
    if c_floor and b_floor and c_floor != b_floor:
        return False

    c_price = _price_key(concurrent_row.get("Price"))
    b_price = _price_key(brochure_row.get("Price"))
    if c_price and b_price and c_price != b_price:
        return False
    return True


def _merge_unilife_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    concurrent_rows = [dict(row) for row in rows if _is_concurrent_url(row.get("Source URL", ""))]
    brochure_rows = [dict(row) for row in rows if not _is_concurrent_url(row.get("Source URL", ""))]

    # 1) Consolidate Concurrent rows first: they are canonical when available.
    merged_concurrent: List[Dict[str, Any]] = []
    index_by_key: Dict[Tuple[str, str, str, str, str], int] = {}
    for row in concurrent_rows:
        key = (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Academic Year", "")),
            _price_key(row.get("Price")),
            common.normalize_space(row.get("Floor Level", "")),
        )
        if key not in index_by_key:
            index_by_key[key] = len(merged_concurrent)
            merged_concurrent.append(row)
            continue

        existing = merged_concurrent[index_by_key[key]]
        if existing.get("Contract Value") is None and row.get("Contract Value") is not None:
            existing["Contract Value"] = row.get("Contract Value")
        if existing.get("Price") is None and row.get("Price") is not None:
            existing["Price"] = row.get("Price")
        existing_av = common.normalize_space(existing.get("Availability", ""))
        new_av = common.normalize_space(row.get("Availability", ""))
        if (not existing_av or existing_av == "Unknown") and new_av:
            existing["Availability"] = new_av
        existing["Incentives"] = common.extract_and_normalise_incentives(
            existing.get("Incentives", ""),
            row.get("Incentives", ""),
        )

    # 2) Supplement Concurrent rows with brochure-only metadata (incentives + explicit floor when missing).
    for c_row in merged_concurrent:
        for b_row in brochure_rows:
            if not _same_unilife_row(c_row, b_row):
                continue
            c_row["Incentives"] = common.extract_and_normalise_incentives(
                c_row.get("Incentives", ""),
                b_row.get("Incentives", ""),
            )
            if not common.normalize_space(c_row.get("Floor Level", "")):
                b_floor = common.normalize_space(b_row.get("Floor Level", ""))
                if b_floor:
                    c_row["Floor Level"] = b_floor

    # 3) Keep brochure rows only where no matching Concurrent row exists.
    fallback_brochure: List[Dict[str, Any]] = []
    for b_row in brochure_rows:
        has_concurrent_match = any(_same_unilife_row(c_row, b_row) for c_row in merged_concurrent)
        if has_concurrent_match:
            continue
        fallback_brochure.append(b_row)

    merged = merged_concurrent + fallback_brochure

    # 4) Remove weaker blank-floor duplicates when explicit floor exists for same contract-level row.
    floor_specific: Dict[Tuple[str, str, str, str], bool] = {}
    for row in merged:
        base = (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Academic Year", "")),
            _price_key(row.get("Price")),
        )
        if common.normalize_space(row.get("Floor Level", "")):
            floor_specific[base] = True

    out: List[Dict[str, Any]] = []
    seen_keys = set()
    for row in merged:
        base = (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Academic Year", "")),
            _price_key(row.get("Price")),
        )
        if not common.normalize_space(row.get("Floor Level", "")) and floor_specific.get(base):
            continue

        key = (
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Academic Year", "")),
            _price_key(row.get("Price")),
            common.normalize_space(row.get("Floor Level", "")),
        )
        if key in seen_keys:
            continue
        seen_keys.add(key)
        out.append(row)
    return out


async def _parse_impl(page: Page, src: Dict[str, str], follow_booking_links: bool) -> Tuple[List[Dict[str, Any]], str]:
    await common.click_common(page)
    await page.wait_for_timeout(1200)

    page_body = common.normalize_currency_text(await page.inner_text("body"))
    page_ay = _extract_best_academic_year(page_body)
    property_text = await page.evaluate(
        r"""
        () => {
          const sels = [
            '.banner',
            '.hero',
            '[class*="promo"]',
            '[class*="offer"]',
            '[class*="announcement"]',
            '[class*="deal"]',
            '[class*="cashback"]',
            '[class*="bus-pass"]',
            '[class*="kitchen"]',
            '[class*="bedding"]'
          ];
          const out = [];
          sels.forEach(s => document.querySelectorAll(s).forEach(n => {
            const t = (n.innerText || '').replace(/\s+/g, ' ').trim();
            if (t) out.push(t);
          }));
          return out.join(' | ');
        }
        """
    )

    card_meta = await page.evaluate(
        r"""
        () => {
          const out = [];
          const cards = [...document.querySelectorAll('.room-card, [class*="room-card"], [class*="room"] article, article')];
          for (const c of cards) {
            const title = (c.querySelector('h2,h3,h4,.title,[class*="title"]')?.innerText || '').replace(/\s+/g, ' ').trim();
            const text = (c.innerText || '').replace(/\s+/g, ' ').trim();
            if (title && text && text.length < 2400) out.push({title, text});
          }
          return out;
        }
        """
    )

    room_card_price: Dict[str, float] = {}
    room_card_incentives: Dict[str, str] = {}
    room_card_floor: Dict[str, str] = {}
    room_card_availability: Dict[str, str] = {}
    known_rooms: List[str] = []
    base_rows: List[Dict[str, Any]] = []
    for card in card_meta:
        title = common.clean_room_name(card.get("title", ""))
        if not title:
            continue
        key = common.normalize_key(title)
        text = common.normalize_currency_text(card.get("text", ""))
        if title not in known_rooms:
            known_rooms.append(title)
        price = common.parse_price_to_weekly_numeric(text)
        if price is not None:
            room_card_price[key] = price
        availability = common.infer_availability(text)
        if availability == "Unknown" and price is not None:
            availability = "Available"
        room_card_availability[key] = availability
        room_card_incentives[key] = _scope_incentives(
            title,
            common.extract_and_normalise_incentives(text, _room_scoped_property_incentives(title, property_text)),
        )
        floor = _extract_unilife_floor(text)
        if floor:
            room_card_floor[key] = floor
        base_rows.append(
            {
                "Room Name": title,
                "Contract Length": _extract_contract_length_unilife(text),
                "Price": price,
                "Contract Value": common.parse_contract_value_numeric(text),
                "Floor Level": floor,
                "Academic Year": _extract_best_academic_year(text, page_ay),
                "Incentives": room_card_incentives[key],
                "Availability": availability,
                "Source URL": src["url"],
                "__missing_price_reason": common.classify_missing_price_reason(text, availability) if price is None else "",
            }
        )

    rows: List[Dict[str, Any]] = []
    if follow_booking_links:
        candidates = await _collect_booking_candidates(
            page=page,
            property_text=property_text,
            page_ay=page_ay,
            room_card_incentives=room_card_incentives,
            room_card_floor=room_card_floor,
            room_card_price=room_card_price,
            known_rooms=known_rooms,
        )
        for candidate in candidates:
            extracted_rows = await _extract_rows_from_booking_link(
                page=page,
                src=src,
                candidate=candidate,
                property_text=property_text,
                page_ay=page_ay,
                room_card_incentives=room_card_incentives,
                room_card_floor=room_card_floor,
                room_card_price=room_card_price,
            )
            rows.extend(extracted_rows)

    # Modal fallback when deep links are unavailable.
    if not rows:
        view_buttons = page.get_by_role("button", name=re.compile(r"view room|book now|book", re.IGNORECASE))
        btn_count = min(await view_buttons.count(), 40)
        for i in range(btn_count):
            btn = view_buttons.nth(i)
            try:
                if not await btn.is_visible():
                    continue
                await btn.click(timeout=1800)
                await page.wait_for_timeout(600)
                rows.extend(await _parse_open_modal(page, src, property_text, page_ay))
                close = page.get_by_role("button", name=re.compile(r"close|x", re.IGNORECASE))
                if await close.count():
                    try:
                        await close.first.click(timeout=800)
                    except Exception:
                        pass
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(180)
            except Exception:
                continue

    if not rows:
        rows = base_rows

    cleaned_rows: List[Dict[str, Any]] = []
    for row in rows:
        room_name = common.clean_room_name(row.get("Room Name", ""))
        if not room_name or not _is_valid_unilife_room_name(room_name):
            continue
        room_key = common.normalize_key(room_name)
        contract = _extract_contract_length_unilife(row.get("Contract Length", ""))
        price = row.get("Price")
        if price is None and room_key in room_card_price:
            price = room_card_price[room_key]

        floor = _extract_unilife_floor(row.get("Floor Level", "")) or room_card_floor.get(room_key, "")
        card_floor = room_card_floor.get(room_key, "")
        if card_floor and (" to " in card_floor or "|" in card_floor) and floor and (" to " not in floor and "|" not in floor):
            floor = card_floor
        ay = _extract_best_academic_year(row.get("Academic Year", ""), page_ay)
        incentives = common.extract_and_normalise_incentives(
            row.get("Incentives", ""),
            room_card_incentives.get(room_key, ""),
            _room_scoped_property_incentives(room_name, property_text),
        )
        incentives = _scope_incentives(room_name, incentives)

        availability = common.normalize_space(row.get("Availability", ""))
        if not availability:
            availability = room_card_availability.get(room_key, "Unknown")
        if availability == "Unknown" and price is not None:
            availability = "Available"

        contract_value = row.get("Contract Value")
        cleaned_rows.append(
            {
                "Room Name": room_name,
                "Contract Length": contract,
                "Price": price,
                "Contract Value": contract_value,
                "Floor Level": floor,
                "Academic Year": ay,
                "Incentives": incentives,
                "Availability": availability,
                "Source URL": common.normalize_space(row.get("Source URL", "")) or src["url"],
                "__missing_price_reason": common.classify_missing_price_reason(
                    f"{room_name} {contract}",
                    availability,
                )
                if price is None
                else "",
            }
        )

    merged_rows = _merge_unilife_rows(cleaned_rows)
    if merged_rows:
        return merged_rows, ""
    return [], "no extractable room rows"


async def parse_dom(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    # Castle Way DOM parsing can be heavy; switch to interactive fallback if this stage exceeds 60s.
    try:
        return await asyncio.wait_for(_parse_impl(page, src, follow_booking_links=False), timeout=60)
    except asyncio.TimeoutError:
        return [], "dom_timeout_switch_to_playwright"


async def parse_interactive(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    try:
        return await asyncio.wait_for(_parse_impl(page, src, follow_booking_links=True), timeout=105)
    except asyncio.TimeoutError:
        return await _parse_impl(page, src, follow_booking_links=False)


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    return await parse_interactive(page, src)
















