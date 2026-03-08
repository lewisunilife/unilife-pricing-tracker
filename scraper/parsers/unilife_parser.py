import re
from typing import Dict, List, Tuple

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


async def _parse_open_modal(page: Page, src: Dict[str, str], property_text: str, page_ay: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
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
    modal_ay = common.normalise_academic_year(modal_text) or page_ay
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
        contract = common.extract_contract_length(col_header)
        tiles = col.locator(".book")
        tile_count = await tiles.count()
        for ti in range(tile_count):
            tile = tiles.nth(ti)
            tile_text = common.normalize_space(await tile.inner_text())
            price = _unilife_tile_price(tile_text, col_header, modal_text) or base_price

            floor = common.normalise_floor_level(tile_text)
            ay = common.normalise_academic_year(tile_text) or modal_ay
            availability = common.infer_availability(tile_text) or modal_avail
            incentives = _scope_incentives(room_name, common.extract_and_normalise_incentives(tile_text, room_incentives, property_incentives))

            rows.append(
                {
                    "Room Name": room_name,
                    "Contract Length": contract or common.extract_contract_length(tile_text),
                    "Price": price,
                    "Floor Level": floor,
                    "Academic Year": ay,
                    "Incentives": incentives,
                    "Availability": availability,
                    "Source URL": src["url"],
                }
            )
            wrote = True

    if not wrote:
        rows.append(
            {
                "Room Name": room_name,
                "Contract Length": common.extract_contract_length(modal_text),
                "Price": base_price,
                "Floor Level": common.normalise_floor_level(modal_text),
                "Academic Year": modal_ay,
                "Incentives": common.extract_and_normalise_incentives(room_incentives, property_incentives),
                "Availability": modal_avail,
                "Source URL": src["url"],
            }
        )
    return rows


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, str]], str]:
    await common.click_common(page)
    await page.wait_for_timeout(1200)

    page_body = await page.inner_text("body")
    page_ay = common.normalise_academic_year(page_body)
    property_text = await page.evaluate(
        r"""
        () => {
          const sels = ['.banner', '.hero', '[class*="promo"]', '[class*="offer"]', '[class*="announcement"]'];
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
            if (title && text && text.length < 500) out.push({title, text});
          }
          return out;
        }
        """
    )
    room_card_price: Dict[str, float] = {}
    room_card_incentives: Dict[str, str] = {}
    for c in card_meta:
        title = common.clean_room_name(c.get("title", ""))
        if not title:
            continue
        key = common.normalize_key(title)
        txt = common.normalize_currency_text(c.get("text", ""))
        p = common.parse_price_to_weekly_numeric(txt)
        if p is None:
            m = re.search(r"[£Ł]\s*(\d{2,4}(?:,\d{3})*(?:\.\d{1,2})?)\s*pw", txt, flags=re.IGNORECASE)
            if m:
                try:
                    p = round(float(m.group(1).replace(",", "")), 2)
                except ValueError:
                    p = None
        if p is not None:
            room_card_price[key] = p
        room_card_incentives[key] = common.extract_and_normalise_incentives(txt)

    rows: List[Dict[str, str]] = []

    view_buttons = page.get_by_role("button", name=re.compile(r"view room|book now|book", re.IGNORECASE))
    btn_count = min(await view_buttons.count(), 40)
    for i in range(btn_count):
        btn = view_buttons.nth(i)
        try:
            if not await btn.is_visible():
                continue
            await btn.click(timeout=1800)
            await page.wait_for_timeout(600)
            modal_rows = await _parse_open_modal(page, src, property_text, page_ay)
            for mr in modal_rows:
                if mr.get("Price") is None:
                    rk = common.normalize_key(mr.get("Room Name", ""))
                    if rk in room_card_price:
                        mr["Price"] = room_card_price[rk]
                rk = common.normalize_key(mr.get("Room Name", ""))
                if rk in room_card_incentives:
                    mr["Incentives"] = common.extract_and_normalise_incentives(mr.get("Incentives", ""), room_card_incentives[rk])
            rows.extend(modal_rows)
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

    # Fallback for pages where modals are already in DOM.
    if not rows:
        blocks = await page.evaluate(
            r"""
            () => {
              const out = [];
              const modals = [...document.querySelectorAll('.modal-content__inner')];
              for (const modal of modals) {
                const title = (modal.querySelector('.title, h1, h2, h3, .top')?.innerText || '').replace(/\s+/g, ' ').trim();
                const availability = (modal.querySelector('.available')?.innerText || '').replace(/\s+/g, ' ').trim();
                const modalText = (modal.innerText || '').replace(/\s+/g, ' ').trim();
                const columns = [...modal.querySelectorAll('.contracts-column')].map(col => ({
                  header: (col.querySelector('.top, h3, h4, p, strong')?.innerText || '').replace(/\s+/g, ' ').trim(),
                  tiles: [...col.querySelectorAll('.book')].map(tile => (tile.innerText || '').replace(/\s+/g, ' ').trim())
                }));
                out.push({title, availability, modalText, columns});
              }
              return out;
            }
            """
        )

        for block in blocks:
            room_name = common.clean_room_name(block.get("title", ""))
            if not room_name:
                continue
            modal_text = common.normalize_space(block.get("modalText", ""))
            modal_avail = common.normalize_space(block.get("availability", "")) or common.infer_availability(modal_text)
            modal_ay = common.normalise_academic_year(modal_text) or page_ay
            base_price = common.parse_price_to_weekly_numeric(modal_text)
            incentives = common.extract_and_normalise_incentives(modal_text, _room_scoped_property_incentives(room_name, property_text))

            wrote = False
            for col in block.get("columns", []):
                contract = common.extract_contract_length(col.get("header", ""))
                for tile in col.get("tiles", []):
                    tile_text = common.normalize_space(tile)
                    rows.append(
                        {
                            "Room Name": room_name,
                            "Contract Length": contract or common.extract_contract_length(tile_text),
                            "Price": _unilife_tile_price(tile_text, col.get("header", ""), modal_text) or base_price,
                            "Floor Level": common.normalise_floor_level(tile_text),
                            "Academic Year": common.normalise_academic_year(tile_text) or modal_ay,
                        "Incentives": _scope_incentives(room_name, common.extract_and_normalise_incentives(tile_text, incentives)),
                            "Availability": common.infer_availability(tile_text) or modal_avail,
                            "Source URL": src["url"],
                        }
                    )
                    wrote = True

            if not wrote:
                fallback_price = base_price or room_card_price.get(common.normalize_key(room_name))
                rows.append(
                    {
                        "Room Name": room_name,
                        "Contract Length": common.extract_contract_length(modal_text),
                        "Price": fallback_price,
                        "Floor Level": common.normalise_floor_level(modal_text),
                        "Academic Year": modal_ay,
                        "Incentives": _scope_incentives(room_name, common.extract_and_normalise_incentives(incentives, room_card_incentives.get(common.normalize_key(room_name), ""))),
                        "Availability": modal_avail,
                        "Source URL": src["url"],
                    }
                )

    # dedupe inside parser
    # Fallback for pages where card prices are visible as plain text lines (e.g., Bargate/High Street).
    if rows:
        price_tokens = re.findall(r"[£Ł]\s*(\d{2,4}(?:,\d{3})*(?:\.\d{1,2})?)\s*pw", common.normalize_currency_text(page_body), flags=re.IGNORECASE)
        numeric_prices: List[float] = []
        for tok in price_tokens:
            try:
                val = round(float(tok.replace(",", "")), 2)
            except ValueError:
                continue
            if val not in numeric_prices:
                numeric_prices.append(val)

        if numeric_prices:
            ordered_rooms: List[str] = []
            for r in rows:
                room = common.normalize_space(r.get("Room Name", ""))
                if room and room not in ordered_rooms:
                    ordered_rooms.append(room)
            if len(numeric_prices) >= len(ordered_rooms) and ordered_rooms:
                map_price = {ordered_rooms[i]: numeric_prices[i] for i in range(len(ordered_rooms))}
                for r in rows:
                    if r.get("Price") is None and r.get("Room Name") in map_price:
                        r["Price"] = map_price[r["Room Name"]]

    uniq = []
    seen = set()
    for r in rows:
        key = (
            r.get("Room Name", ""),
            r.get("Contract Length", ""),
            r.get("Price", ""),
            r.get("Floor Level", ""),
            r.get("Academic Year", ""),
            r.get("Incentives", ""),
            r.get("Availability", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)

    if uniq:
        return uniq, ""
    return [], "no extractable room rows"
