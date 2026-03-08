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
    parse_contract_value_numeric,
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

    contract_value_before = row.get("Contract Value", "")
    row["Contract Value"] = parse_contract_value_numeric(contract_value_before)
    if normalize_space(contract_value_before) and row["Contract Value"] is None:
        issues.append("invalid_contract_value")

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
    row["HALL ID"] = normalize_space(row.get("HALL ID", ""))
    row["ROOM ID"] = normalize_space(row.get("ROOM ID", ""))

    return row, issues


def infer_missing_price_reason(raw: Dict[str, Any], cleaned: Dict[str, Any], issues: List[str]) -> str:
    explicit = normalize_space(raw.get("__missing_price_reason", "")).lower()
    allowed = {
        "sold_out",
        "unavailable_no_contract_options",
        "hidden_deeper_in_flow",
        "parser_selector_failure",
        "blocked",
        "ambiguous_period",
        "not_shown_publicly",
    }
    if explicit in allowed:
        return explicit

    if cleaned.get("Price") is not None:
        return ""

    availability = normalize_space(cleaned.get("Availability", "")).lower()
    if availability == "sold out":
        return "sold_out"
    if availability == "unavailable":
        return "unavailable_no_contract_options"
    if any(issue == "invalid_or_ambiguous_price" for issue in issues):
        return "ambiguous_period"
    if normalize_space(raw.get("Contract Length", "")):
        return "not_shown_publicly"
    return "parser_selector_failure"


def is_publishable_row(row: Dict[str, Any]) -> bool:
    # Accuracy over volume: require clean room names and enforce strict blank-price rules.
    if not normalize_space(row.get("Room Name", "")):
        return False
    if row.get("Price") is not None:
        return True
    availability = normalize_space(row.get("Availability", ""))
    return availability in {"Sold Out", "Unavailable"}
