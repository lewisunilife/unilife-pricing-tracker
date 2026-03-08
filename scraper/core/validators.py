from typing import Any, Dict, List, Tuple

from .normalisers import (
    clean_property_name,
    clean_room_name,
    extract_and_assign_incentives,
    extract_contract_length,
    normalise_academic_year,
    normalise_availability,
    normalise_floor_level,
    normalize_space,
    parse_price_to_weekly_numeric,
)


def validate_row(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    issues: List[str] = []
    row = dict(raw)

    row["Operator"] = normalize_space(row.get("Operator", ""))
    row["Property"] = clean_property_name(row.get("Property", ""))
    room_before = normalize_space(row.get("Room Name", ""))
    row["Room Name"] = clean_room_name(room_before)
    if room_before and not row["Room Name"]:
        issues.append("room_name_cleaned_to_blank")

    if row["Property"] and row["Room Name"] and row["Property"].lower() == row["Room Name"].lower():
        row["Room Name"] = ""
        issues.append("room_name_equals_property")

    row["Contract Length"] = extract_contract_length(row.get("Contract Length", "")) or normalize_space(row.get("Contract Length", ""))
    row["Floor Level"] = normalise_floor_level(row.get("Floor Level", ""))

    ay_before = normalize_space(row.get("Academic Year", ""))
    row["Academic Year"] = normalise_academic_year(ay_before)
    if ay_before and not row["Academic Year"]:
        issues.append("invalid_academic_year")

    price_before = row.get("Price", "")
    row["Price"] = parse_price_to_weekly_numeric(price_before)
    if normalize_space(price_before) and row["Price"] is None:
        issues.append("invalid_or_ambiguous_price")

    row["Incentives"] = extract_and_assign_incentives(
        row.get("Room Name", ""),
        row.get("Incentives", ""),
        row.get("Incentives", ""),
        row.get("Incentives", ""),
    )
    row["Availability"] = normalise_availability(row.get("Availability", ""))
    row["Source URL"] = normalize_space(row.get("Source URL", ""))
    row["City"] = normalize_space(row.get("City", ""))
    row["Scrape Source"] = normalize_space(row.get("Scrape Source", ""))
    row["Snapshot ID"] = normalize_space(row.get("Snapshot ID", ""))
    row["Snapshot Date"] = normalize_space(row.get("Snapshot Date", ""))
    row["Run Timestamp"] = normalize_space(row.get("Run Timestamp", ""))
    row["HALL ID"] = normalize_space(row.get("HALL ID", ""))
    row["ROOM ID"] = normalize_space(row.get("ROOM ID", ""))

    return row, issues


def is_publishable_row(row: Dict[str, Any]) -> bool:
    # Accuracy over volume: require at least a clean room name.
    return bool(normalize_space(row.get("Room Name", "")))
