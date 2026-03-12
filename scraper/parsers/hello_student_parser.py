import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import Page

from . import common

SOUTHAMPTON_LISTING_URL = "https://www.hellostudent.co.uk/properties-search?location=1072&filters=%5B%5D"
SOUTHAMPTON_LOCATION_ID = 1072


def _normalise_key(value: Any) -> str:
    return common.normalize_key(common.normalize_space(value))


def _normalise_academic_year(value: Any) -> str:
    text = common.normalize_space(value)
    if not text:
        return ""
    normalised = common.normalise_academic_year(text)
    if normalised:
        return normalised
    return common.normalise_academic_year(text.replace("-", "/"))


def _parse_iso_date(value: Any) -> str:
    text = common.normalize_space(value)
    if not text:
        return ""
    try:
        return dt.date.fromisoformat(text).strftime("%d/%m/%Y")
    except ValueError:
        return ""


def _contract_length_from_term(term: Dict[str, Any]) -> str:
    duration = term.get("duration")
    if isinstance(duration, int) and duration > 0:
        return f"{duration} WEEK" if duration == 1 else f"{duration} WEEKS"
    return common.extract_contract_length(term.get("title", ""))


def _weekly_price_from_pence(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value) / 100.0, 2)
    except Exception:
        return None


def _calculate_contract_value(weekly_price: Optional[float], contract_length: str) -> Optional[float]:
    if weekly_price is None:
        return None
    length_text = common.extract_contract_length(contract_length)
    if not length_text:
        return None
    try:
        weeks = int(length_text.split()[0])
    except Exception:
        return None
    return round(weekly_price * weeks, 2)


def _availability_from_total_available(value: Any) -> str:
    try:
        count = int(value)
    except Exception:
        return "Unknown"
    return "Available" if count > 0 else "Sold Out"


def _property_from_listing(entry: Dict[str, Any]) -> Dict[str, Any]:
    details = entry.get("acf", {}).get("details", {}) or {}
    return {
        "property_id": entry.get("ID"),
        "property_name": common.normalize_space(entry.get("post_title")),
        "property_slug": common.normalize_space(entry.get("post_name")),
        "property_url": f"https://www.hellostudent.co.uk/student-accommodation/southampton/{common.normalize_space(entry.get('post_name'))}",
        "booking_url": f"https://www.hellostudent.co.uk/book-a-room?property={entry.get('ID')}",
        "description": common.normalize_space(details.get("description")),
    }


async def _fetch_listing_properties(page: Page) -> List[Dict[str, Any]]:
    payload = await page.evaluate(
        """async (locationId) => {
            try {
                const res = await fetch(`/wp-json/wp/v2/search-location?location=${locationId}&filters=%5B%5D`, {
                    credentials: 'same-origin',
                    headers: { accept: 'application/json' },
                });
                const data = await res.json();
                const properties = (((data || {}).acf || {}).properties || []).map((entry) => ({
                    ID: entry.ID,
                    post_title: entry.post_title,
                    post_name: entry.post_name,
                    acf: {
                        details: ((entry.acf || {}).details || {}),
                    },
                }));
                return { ok: res.ok, properties };
            } catch (error) {
                return { ok: false, error: String(error), properties: [] };
            }
        }""",
        SOUTHAMPTON_LOCATION_ID,
    )
    if not payload or not payload.get("ok"):
        return []
    return [_property_from_listing(entry) for entry in payload.get("properties", []) if entry.get("ID")]


def _match_property(target_property: str, properties: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    target_key = _normalise_key(target_property)
    for prop in properties:
        if _normalise_key(prop.get("property_name", "")) == target_key:
            return prop
    for prop in properties:
        if target_key and target_key in _normalise_key(prop.get("property_name", "")):
            return prop
    return None


async def _fetch_setup(page: Page, property_id: int) -> Dict[str, Any]:
    return await page.evaluate(
        """async (propertyId) => {
            try {
                const res = await fetch(`/wp-json/roomAvailability/v1/setup?property=${propertyId}`, {
                    credentials: 'same-origin',
                    headers: { accept: 'application/json' },
                });
                const data = await res.json();
                return { ok: res.ok, status: res.status, data };
            } catch (error) {
                return { ok: false, error: String(error), data: null };
            }
        }""",
        property_id,
    )


async def _fetch_term_rooms(page: Page, property_id: int, academic_year_id: int, term_id: int) -> Dict[str, Any]:
    return await page.evaluate(
        """async ({ propertyId, academicYearId, termId }) => {
            try {
                const res = await fetch(
                    `/wp-json/roomAvailability/v1/filter?academicYear=${academicYearId}&property=${propertyId}&sortOrder=relevance&academicTerm=${termId}`,
                    {
                        credentials: 'same-origin',
                        headers: { accept: 'application/json' },
                    }
                );
                const json = await res.json();
                return { ok: res.ok, status: res.status, data: json };
            } catch (error) {
                return { ok: false, error: String(error), data: null };
            }
        }""",
        {"propertyId": property_id, "academicYearId": academic_year_id, "termId": term_id},
    )


def _room_label(room: Dict[str, Any]) -> str:
    for candidate in [
        room.get("fields", {}).get("name", ""),
        room.get("roomTitle", ""),
        room.get("roomCode", ""),
    ]:
        clean = common.clean_room_name(candidate)
        if clean:
            return clean
    return ""


def _dedupe_term_rooms(matches: List[Dict[str, Any]], property_id: int) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for match in matches:
        info = match.get("info", {}) or {}
        candidate_rooms = match.get("rooms") or []
        if not candidate_rooms and info:
            candidate_rooms = [info]
        for room in candidate_rooms:
            if room.get("propertyPostId") != property_id:
                continue
            key = (
                room.get("propertyPostId"),
                room.get("roomGradeId"),
                room.get("roomPostId"),
                room.get("academicTermId"),
                common.normalize_space(room.get("applyOnlineUrl", "")),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(room)
    return deduped


def _disambiguate_room_names(rows: List[Dict[str, Any]]) -> None:
    grouped: Dict[Tuple[str, str], List[int]] = {}
    for idx, row in enumerate(rows):
        key = (
            _normalise_key(row.get("Property", "")),
            _normalise_key(row.get("__base_room_name", row.get("Room Name", ""))),
        )
        grouped.setdefault(key, []).append(idx)

    for indices in grouped.values():
        if len(indices) <= 1:
            continue
        for idx in indices:
            row = rows[idx]
            suffix_parts = [
                common.normalize_space(row.get("Academic Year", "")),
                common.extract_contract_length(row.get("Contract Length", "")),
                common.normalize_space(row.get("Start Date", "")),
            ]
            suffix = " | ".join(part for part in suffix_parts if part)
            if suffix:
                row["Room Name"] = f"{row['__base_room_name']} ({suffix})"


def _dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            common.normalize_space(row.get("Property", "")),
            common.normalize_space(row.get("Room Name", "")),
            common.normalize_space(row.get("Academic Year", "")),
            common.normalize_space(row.get("Contract Length", "")),
            common.normalize_space(row.get("Start Date", "")),
            common.normalize_space(row.get("__option_identity", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _build_row(
    property_entry: Dict[str, Any],
    academic_year: str,
    term: Dict[str, Any],
    room: Dict[str, Any],
    booking_url: str,
) -> Optional[Dict[str, Any]]:
    room_name = _room_label(room)
    if not room_name:
        return None

    contract_length = _contract_length_from_term(term) or common.extract_contract_length(room.get("academicTermName", ""))
    weekly_price = _weekly_price_from_pence(room.get("pricePerWeek"))
    contract_value = _calculate_contract_value(weekly_price, contract_length)
    start_date = _parse_iso_date(term.get("start")) or _parse_iso_date(room.get("academicTermStartDate"))
    source_url = booking_url

    row = {
        "Property": property_entry.get("property_name", ""),
        "Room Name": room_name,
        "Floor Level": "",
        "Contract Length": contract_length,
        "Academic Year": academic_year,
        "Price": weekly_price,
        "Contract Value": contract_value,
        "Availability": _availability_from_total_available(room.get("totalAvailable")),
        "Source URL": source_url,
        "Start Date": start_date,
        "__base_room_name": room_name,
        "__apply_online_url": common.normalize_space(room.get("applyOnlineUrl", "")),
        "__option_identity": "|".join(
            [
                common.normalize_space(str(room.get("roomGradeId", ""))),
                common.normalize_space(str(room.get("academicTermId", ""))),
                common.normalize_space(str(room.get("roomPostId", ""))),
            ]
        ),
        "__room_grade_id": common.normalize_space(str(room.get("roomGradeId", ""))),
        "__room_post_id": common.normalize_space(str(room.get("roomPostId", ""))),
        "__term_id": common.normalize_space(str(room.get("academicTermId", ""))),
        "__contract_start_iso": common.normalize_space(room.get("academicTermStartDate", "")),
    }
    return row


async def parse(page: Page, src: Dict[str, str]) -> Tuple[List[Dict[str, Any]], str]:
    property_name = common.normalize_space(src.get("property", ""))
    if not await common.safe_goto(page, SOUTHAMPTON_LISTING_URL, timeout=120000):
        return [], "hello_student_listing_unavailable"

    listing_properties = await _fetch_listing_properties(page)
    if not listing_properties:
        return [], "hello_student_listing_properties_missing"

    property_entry = _match_property(property_name, listing_properties)
    if not property_entry:
        return [], "hello_student_property_not_found"

    property_id = property_entry.get("property_id")
    if not property_id:
        return [], "hello_student_property_id_missing"

    booking_url = property_entry.get("booking_url", "")
    if not booking_url or not await common.safe_goto(page, booking_url, timeout=120000):
        return [], "hello_student_booking_flow_unavailable"

    setup = await _fetch_setup(page, property_id)
    setup_data = (setup or {}).get("data", {}) or {}
    if not setup.get("ok") or not setup_data.get("success"):
        return [], "hello_student_setup_missing"

    academic_years = ((setup_data.get("data") or {}).get("academicYears")) or []
    rows: List[Dict[str, Any]] = []

    for year in academic_years:
        year_id = year.get("value")
        academic_year = _normalise_academic_year(year.get("title", ""))
        if not year_id or not academic_year:
            continue

        for term in year.get("academicTerms") or []:
            term_id = term.get("value")
            if not term_id:
                continue

            filter_payload = await _fetch_term_rooms(page, property_id, year_id, term_id)
            filter_data = (filter_payload or {}).get("data", {}) or {}
            if not filter_payload.get("ok") or not filter_data.get("success"):
                continue

            match_data = (filter_data.get("data") or {})
            all_matches = list(match_data.get("completeMatches") or [])
            property_rooms = _dedupe_term_rooms(all_matches, property_id)
            if not property_rooms:
                continue

            for room in property_rooms:
                row = _build_row(property_entry, academic_year, term, room, booking_url)
                if row:
                    rows.append(row)

    if not rows:
        return [], "hello_student_no_room_rows"

    _disambiguate_room_names(rows)
    rows = _dedupe_rows(rows)
    for row in rows:
        row.pop("__base_room_name", None)
    return rows, ""
