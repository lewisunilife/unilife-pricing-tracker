from __future__ import annotations

import os
import re
import json
import smtplib
from dataclasses import dataclass
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request

import pandas as pd


SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_TO = ["[lewis@unilife.co.uk](mailto:lewis@unilife.co.uk)"]
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
OPENAI_SUMMARY_MODEL = "gpt-4.1"
SIGNATURE_LINES = [
    "Kind Regards",
    "",
    "Artificial Intelligence",
    "Unilife PBSA Market Intelligence Monitoring",
    "",
    "(Note: This message was sent via AI)",
]

WORKBOOK_PATH = Path(__file__).resolve().parents[1] / "data" / "Unilife_Pricing_Snapshot.xlsx"
SHEET_NAME = "All Pricing"

KEY_COLS = [
    "Operator",
    "Property",
    "Room Name",
    "Contract Length",
    "Academic Year",
    "Floor Level",
]

CHANGE_COLS = ["Price", "Contract Value", "Incentives", "Availability"]


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _norm_num(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        return round(float(value), 2)
    except Exception:
        text = _norm_text(value).replace(",", "")
        if not text:
            return None
        try:
            return round(float(text), 2)
        except Exception:
            return None


def _extract_email(raw: str) -> str:
    token = _norm_text(raw)
    match = re.search(r"mailto:([^)]+)", token, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})", token, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return token


def _snapshot_order(values: Iterable[Any]) -> List[str]:
    ordered: List[str] = []
    seen = set()
    for item in values:
        key = _norm_text(item)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered


def _collapse_snapshot(df: pd.DataFrame) -> Dict[Tuple[str, ...], Dict[str, Any]]:
    rows: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    for _, row in df.iterrows():
        key = tuple(_norm_text(row.get(col, "")) for col in KEY_COLS)
        if not any(key):
            continue
        current = rows.get(key, {})

        price_val = _norm_num(row.get("Price"))
        contract_val = _norm_num(row.get("Contract Value"))
        incentives_val = _norm_text(row.get("Incentives"))
        availability_val = _norm_text(row.get("Availability"))

        # Prefer explicit values when multiple rows collide to same key.
        if price_val is not None:
            current["Price"] = price_val
        elif "Price" not in current:
            current["Price"] = None

        if contract_val is not None:
            current["Contract Value"] = contract_val
        elif "Contract Value" not in current:
            current["Contract Value"] = None

        if incentives_val:
            current["Incentives"] = incentives_val
        elif "Incentives" not in current:
            current["Incentives"] = ""

        if availability_val:
            current["Availability"] = availability_val
        elif "Availability" not in current:
            current["Availability"] = "Unknown"

        source_url_val = _norm_text(row.get("Source URL", ""))
        if source_url_val:
            current["Source URL"] = source_url_val
        elif "Source URL" not in current:
            current["Source URL"] = ""

        rows[key] = current
    return rows


def _fmt_key(key: Tuple[str, ...]) -> str:
    op, prop, room, contract, ay, floor = key
    details = " | ".join(part for part in [room, contract, ay, floor] if part)
    return f"{op} | {prop}\n{details}".strip()


def _fmt_money(value: float | None) -> str:
    if value is None:
        return "blank"
    return f"\u00a3{value:.2f}"


def _split_room_identity(room: str) -> Tuple[str, str]:
    value = _norm_text(room)
    match = re.match(r"(.+?)\s+\(([A-Z]{1,4}-\d{2,4})\)$", value)
    if match:
        return _norm_text(match.group(1)), _norm_text(match.group(2))
    return "", value


@dataclass
class Delta:
    key: Tuple[str, ...]
    old: Dict[str, Any]
    new: Dict[str, Any]


@dataclass
class SummaryStats:
    total_changes: int
    affected_properties: int
    biggest_increase: str
    biggest_decrease: str
    busiest_property: str
    scope_label: str
    detail_lines: List[str]


def _build_deltas(previous: Dict[Tuple[str, ...], Dict[str, Any]], latest: Dict[Tuple[str, ...], Dict[str, Any]]) -> Dict[str, Any]:
    prev_keys = set(previous.keys())
    latest_keys = set(latest.keys())
    shared = sorted(prev_keys & latest_keys)

    new_options = sorted(latest_keys - prev_keys)
    removed_options = sorted(prev_keys - latest_keys)
    deltas: List[Delta] = [Delta(key=k, old=previous[k], new=latest[k]) for k in shared]

    price_changes: List[Delta] = []
    incentive_changes: List[Delta] = []
    availability_changes: List[Delta] = []
    contract_value_changes: List[Delta] = []

    for delta in deltas:
        old_price = delta.old.get("Price")
        new_price = delta.new.get("Price")
        if old_price != new_price:
            price_changes.append(delta)

        old_cv = delta.old.get("Contract Value")
        new_cv = delta.new.get("Contract Value")
        if old_cv != new_cv:
            contract_value_changes.append(delta)

        old_incentives = _norm_text(delta.old.get("Incentives", ""))
        new_incentives = _norm_text(delta.new.get("Incentives", ""))
        if old_incentives != new_incentives:
            incentive_changes.append(delta)

        old_availability = _norm_text(delta.old.get("Availability", ""))
        new_availability = _norm_text(delta.new.get("Availability", ""))
        if old_availability != new_availability:
            availability_changes.append(delta)

    return {
        "new_options": new_options,
        "removed_options": removed_options,
        "price_changes": price_changes,
        "contract_value_changes": contract_value_changes,
        "incentive_changes": incentive_changes,
        "availability_changes": availability_changes,
    }


def _analytics_lines(latest_df: pd.DataFrame) -> List[str]:
    lines: List[str] = []
    work = latest_df.copy()
    work["Price"] = pd.to_numeric(work["Price"], errors="coerce")

    priced = work.dropna(subset=["Price"])
    if priced.empty:
        return ["No numeric weekly prices available for analytics."]

    op_stats = priced.groupby("Operator")["Price"].agg(["min", "max", "mean"]).reset_index().sort_values("Operator")
    prop_stats = (
        priced.groupby(["Operator", "Property"])["Price"].agg(["min", "max", "mean"]).reset_index().sort_values(["Operator", "Property"])
    )

    lines.append("Minimum / Maximum / Average weekly rent by operator:")
    for _, row in op_stats.iterrows():
        lines.append(
            f"- {row['Operator']}: min \u00a3{row['min']:.2f}, max \u00a3{row['max']:.2f}, avg \u00a3{row['mean']:.2f}"
        )

    lines.append("")
    lines.append("Minimum / Maximum / Average weekly rent by property:")
    for _, row in prop_stats.iterrows():
        lines.append(
            f"- {row['Operator']} | {row['Property']}: min \u00a3{row['min']:.2f}, max \u00a3{row['max']:.2f}, avg \u00a3{row['mean']:.2f}"
        )
    return lines


def _contract_trend_lines(latest_df: pd.DataFrame) -> List[str]:
    text = latest_df["Contract Length"].apply(_norm_text)
    c51 = int(text.str.contains(r"\b51\s*weeks?\b", case=False, regex=True).sum())
    c45 = int(text.str.contains(r"\b45\s*weeks?\b", case=False, regex=True).sum())
    cflex = int(text.str.contains(r"\bflexible\s*stay\b", case=False, regex=True).sum())
    return [
        f"- 51 week contracts: {c51}",
        f"- 45 week contracts: {c45}",
        f"- Flexible stay contracts: {cflex}",
    ]


def _incentive_activity_lines(latest_df: pd.DataFrame) -> List[str]:
    work = latest_df.copy()
    work["Incentives"] = work["Incentives"].apply(_norm_text)
    active = work[work["Incentives"] != ""]
    if active.empty:
        return ["No active incentives detected in latest snapshot."]
    counts = active.groupby("Operator").size().sort_values(ascending=False)
    return [f"- {op}: {int(count)}" for op, count in counts.items()]


def _top_movement_lines(price_changes: List[Delta]) -> List[str]:
    movement: List[Tuple[float, Delta]] = []
    for delta in price_changes:
        old_price = delta.old.get("Price")
        new_price = delta.new.get("Price")
        if old_price is None or new_price is None:
            continue
        movement.append((round(new_price - old_price, 2), delta))

    if not movement:
        return ["No numeric-on-numeric price movement available."]

    increases = sorted([x for x in movement if x[0] > 0], key=lambda x: x[0], reverse=True)[:5]
    decreases = sorted([x for x in movement if x[0] < 0], key=lambda x: x[0])[:5]

    lines: List[str] = []
    lines.append("Top 5 price increases:")
    if increases:
        for diff, delta in increases:
            lines.append(
                f"- {_fmt_key(delta.key)}: {_fmt_money(delta.old.get('Price'))} -> {_fmt_money(delta.new.get('Price'))} (+\u00a3{abs(diff):.2f})"
            )
    else:
        lines.append("- None")

    lines.append("")
    lines.append("Top 5 price decreases:")
    if decreases:
        for diff, delta in decreases:
            lines.append(
                f"- {_fmt_key(delta.key)}: {_fmt_money(delta.old.get('Price'))} -> {_fmt_money(delta.new.get('Price'))} (-\u00a3{abs(diff):.2f})"
            )
    else:
        lines.append("- None")
    return lines


def _describe_delta(delta: Delta, label: str) -> str:
    op, prop, room, contract, ay, floor = delta.key
    room_type, room_name = _split_room_identity(room)
    parts = [f"{label}: {op} | {prop}"]
    if room_type:
        parts.append(f"room type {room_type}")
    parts.append(f"room {room_name or room}")
    if contract:
        parts.append(f"contract {contract}")
    if ay:
        parts.append(f"year {ay}")
    if floor:
        parts.append(f"floor {floor}")
    return " | ".join(parts)


def _describe_price_change(delta: Delta) -> str:
    line = _describe_delta(delta, "Price change")
    old_price = _fmt_money(delta.old.get("Price"))
    new_price = _fmt_money(delta.new.get("Price"))
    old_num = delta.old.get("Price")
    new_num = delta.new.get("Price")
    if old_num is not None and new_num is not None:
        diff = round(new_num - old_num, 2)
        sign = "+" if diff > 0 else ""
        return f"{line} | {old_price} -> {new_price} ({sign}\u00a3{diff:.2f})"
    return f"{line} | {old_price} -> {new_price}"


def _describe_key_change(key: Tuple[str, ...], label: str) -> str:
    op, prop, room, contract, ay, floor = key
    room_type, room_name = _split_room_identity(room)
    parts = [f"{label}: {op} | {prop}"]
    if room_type:
        parts.append(f"room type {room_type}")
    parts.append(f"room {room_name or room}")
    if contract:
        parts.append(f"contract {contract}")
    if ay:
        parts.append(f"year {ay}")
    if floor:
        parts.append(f"floor {floor}")
    return " | ".join(parts)


def _price_change_extremes(price_changes: List[Delta]) -> Tuple[str, str]:
    numeric_moves: List[Tuple[float, Delta]] = []
    for delta in price_changes:
        old_num = delta.old.get("Price")
        new_num = delta.new.get("Price")
        if old_num is None or new_num is None:
            continue
        numeric_moves.append((round(new_num - old_num, 2), delta))

    increases = [item for item in numeric_moves if item[0] > 0]
    decreases = [item for item in numeric_moves if item[0] < 0]

    biggest_increase = "None"
    if increases:
        diff, delta = sorted(increases, key=lambda item: item[0], reverse=True)[0]
        biggest_increase = f"{_describe_price_change(delta)}"

    biggest_decrease = "None"
    if decreases:
        diff, delta = sorted(decreases, key=lambda item: item[0])[0]
        biggest_decrease = f"{_describe_price_change(delta)}"

    return biggest_increase, biggest_decrease


def _build_summary_stats(deltas: Dict[str, Any]) -> SummaryStats:
    affected_counts: Dict[Tuple[str, str], int] = {}
    affected_pairs = set()

    def mark_pair(key: Tuple[str, ...]) -> None:
        pair = (key[0], key[1])
        affected_pairs.add(pair)
        affected_counts[pair] = affected_counts.get(pair, 0) + 1

    detail_lines: List[str] = []
    total_changes = 0

    for delta in deltas["price_changes"]:
        total_changes += 1
        mark_pair(delta.key)
        detail_lines.append(_describe_price_change(delta))

    for delta in deltas["contract_value_changes"]:
        total_changes += 1
        mark_pair(delta.key)
        detail_lines.append(
            f"{_describe_delta(delta, 'Contract value change')} | "
            f"{_fmt_money(delta.old.get('Contract Value'))} -> {_fmt_money(delta.new.get('Contract Value'))}"
        )

    for delta in deltas["incentive_changes"]:
        total_changes += 1
        mark_pair(delta.key)
        detail_lines.append(
            f"{_describe_delta(delta, 'Incentive change')} | "
            f"{_norm_text(delta.old.get('Incentives')) or 'blank'} -> {_norm_text(delta.new.get('Incentives')) or 'blank'}"
        )

    for delta in deltas["availability_changes"]:
        total_changes += 1
        mark_pair(delta.key)
        detail_lines.append(
            f"{_describe_delta(delta, 'Availability change')} | "
            f"{_norm_text(delta.old.get('Availability')) or 'Unknown'} -> {_norm_text(delta.new.get('Availability')) or 'Unknown'}"
        )

    for key in deltas["new_options"]:
        total_changes += 1
        mark_pair(key)
        detail_lines.append(_describe_key_change(key, "New contract option"))

    for key in deltas["removed_options"]:
        total_changes += 1
        mark_pair(key)
        detail_lines.append(_describe_key_change(key, "Removed contract option"))

    biggest_increase, biggest_decrease = _price_change_extremes(deltas["price_changes"])

    busiest_property = "None"
    if affected_counts:
        pair, count = sorted(affected_counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))[0]
        busiest_property = f"{pair[0]} | {pair[1]} ({count} changes)"

    scope_label = "isolated"
    if len(affected_pairs) >= 4:
        scope_label = "broad-based"
    elif len(affected_pairs) >= 2:
        scope_label = "moderately broad"

    return SummaryStats(
        total_changes=total_changes,
        affected_properties=len(affected_pairs),
        biggest_increase=biggest_increase,
        biggest_decrease=biggest_decrease,
        busiest_property=busiest_property,
        scope_label=scope_label,
        detail_lines=detail_lines[:40],
    )


def _extract_response_text(payload: Dict[str, Any]) -> str:
    text = _norm_text(payload.get("output_text"))
    if text:
        return text

    for item in payload.get("output", []) or []:
        for part in item.get("content", []) or []:
            if part.get("type") == "output_text":
                text = _norm_text(part.get("text"))
                if text:
                    return text
    return ""


def _generate_ai_summary(deltas: Dict[str, Any]) -> str:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return ""

    stats = _build_summary_stats(deltas)
    if stats.total_changes <= 0:
        return ""

    prompt = "\n".join(
        [
            "Write a concise internal monitoring summary for a PBSA pricing change email.",
            "Use only the supplied facts. Do not invent causes or missing details.",
            "Keep it professional, clear, and 4 to 7 sentences maximum.",
            "State the total number of changes, the biggest price increase, the biggest price decrease, the operator/property with the most changes, and whether the run looks isolated or broad-based.",
            "If there were no increases or no decreases, say that plainly.",
            "",
            f"Total detected changes: {stats.total_changes}",
            f"Affected operator/property pairs: {stats.affected_properties}",
            f"Operator/property with most changes: {stats.busiest_property}",
            f"Run scope: {stats.scope_label}",
            f"Biggest price increase: {stats.biggest_increase}",
            f"Biggest price decrease: {stats.biggest_decrease}",
            "Detected changes:",
            *stats.detail_lines,
        ]
    )

    payload = {
        "model": OPENAI_SUMMARY_MODEL,
        "input": prompt,
        "temperature": 0.2,
        "max_output_tokens": 220,
        "store": False,
    }

    request = urllib_request.Request(
        OPENAI_RESPONSES_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with urllib_request.urlopen(request, timeout=30) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except (urllib_error.URLError, urllib_error.HTTPError, TimeoutError, ValueError, OSError):
        return ""

    return _extract_response_text(response_payload)


def _build_email_body(
    latest_snapshot_id: str,
    previous_snapshot_id: str,
    deltas: Dict[str, Any],
    latest_df: pd.DataFrame,
    ai_summary: str = "",
) -> str:
    lines: List[str] = []
    lines.append("PBSA Pricing Changes Detected \u2014 Southampton")
    lines.append("")
    lines.append(f"Previous Snapshot ID: {previous_snapshot_id}")
    lines.append(f"Latest Snapshot ID: {latest_snapshot_id}")
    lines.append("")

    if _norm_text(ai_summary):
        lines.append("AI Summary")
        lines.append(_norm_text(ai_summary))
        lines.append("")

    lines.append("Section 1 \u2014 Price Changes")
    if deltas["price_changes"] or deltas["contract_value_changes"] or deltas["new_options"] or deltas["removed_options"]:
        for delta in deltas["price_changes"]:
            op, prop, room, contract, ay, floor = delta.key
            room_type, room_name = _split_room_identity(room)
            old_price = _fmt_money(delta.old.get("Price"))
            new_price = _fmt_money(delta.new.get("Price"))
            old_num = delta.old.get("Price")
            new_num = delta.new.get("Price")
            diff = "blank"
            if old_num is not None and new_num is not None:
                change = round(new_num - old_num, 2)
                sign = "+" if change > 0 else ""
                diff = f"{sign}\u00a3{change:.2f}"
            source_url = _norm_text(delta.new.get("Source URL")) or _norm_text(delta.old.get("Source URL"))
            lines.append(f"Operator: {op}")
            lines.append(f"Property: {prop}")
            if room_type:
                lines.append(f"Room Type: {room_type}")
            lines.append(f"Room Name: {room_name or room}")
            lines.append(f"Contract Length: {contract or 'blank'}")
            lines.append(f"Academic Year: {ay or 'blank'}")
            if floor:
                lines.append(f"Floor Level: {floor}")
            lines.append(f"Old Price: {old_price}")
            lines.append(f"New Price: {new_price}")
            lines.append(f"Difference: {diff}")
            if source_url:
                lines.append(f"Source URL: {source_url}")
            lines.append("")

        for key in deltas["new_options"]:
            lines.append(f"New contract option: {_fmt_key(key)}")
        for key in deltas["removed_options"]:
            lines.append(f"Removed contract option: {_fmt_key(key)}")

        for delta in deltas["contract_value_changes"]:
            lines.append(
                f"Contract Value change: {_fmt_key(delta.key)} | "
                f"{_fmt_money(delta.old.get('Contract Value'))} -> {_fmt_money(delta.new.get('Contract Value'))}"
            )
    else:
        lines.append("No price or contract-option changes detected.")
    lines.append("")

    lines.append("Section 2 \u2014 Incentive Changes")
    if deltas["incentive_changes"]:
        for delta in deltas["incentive_changes"]:
            lines.append(_fmt_key(delta.key))
            lines.append(f"{_norm_text(delta.old.get('Incentives')) or 'blank'} -> {_norm_text(delta.new.get('Incentives')) or 'blank'}")
            lines.append("")
    else:
        lines.append("No incentive changes detected.")
    lines.append("")

    lines.append("Section 3 \u2014 Availability Changes")
    if deltas["availability_changes"]:
        for delta in deltas["availability_changes"]:
            lines.append(_fmt_key(delta.key))
            lines.append(
                f"{_norm_text(delta.old.get('Availability')) or 'Unknown'} -> {_norm_text(delta.new.get('Availability')) or 'Unknown'}"
            )
            lines.append("")
    else:
        lines.append("No availability changes detected.")
    lines.append("")

    lines.append("Section 4 \u2014 Market Summary Analytics")
    lines.extend(_analytics_lines(latest_df))
    lines.append("")

    lines.append("Section 5 \u2014 Contract Trends")
    lines.extend(_contract_trend_lines(latest_df))
    lines.append("")

    lines.append("Section 6 \u2014 Incentive Activity")
    lines.extend(_incentive_activity_lines(latest_df))
    lines.append("")

    lines.append("Section 7 \u2014 Price Movement Summary")
    lines.extend(_top_movement_lines(deltas["price_changes"]))
    lines.append("")
    lines.extend(SIGNATURE_LINES)
    lines.append("")

    return "\n".join(lines).strip() + "\n"


def _send_email(subject: str, body: str) -> None:
    username = os.getenv("SMTP_USERNAME", "").strip()
    password = os.getenv("SMTP_PASSWORD", "").strip()
    if not username or not password:
        raise RuntimeError("SMTP_USERNAME and SMTP_PASSWORD secrets are required to send report email.")

    from_addr = _extract_email(username)
    to_addrs = [_extract_email(addr) for addr in EMAIL_TO]

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(from_addr, to_addrs, msg.as_string())


def main() -> int:
    if not WORKBOOK_PATH.exists():
        print(f"Workbook not found: {WORKBOOK_PATH}")
        return 1

    df = pd.read_excel(WORKBOOK_PATH, sheet_name=SHEET_NAME, engine="openpyxl")
    if df.empty:
        print("No pricing changes detected")
        return 0

    for col in ["Snapshot ID", "Snapshot Date", *KEY_COLS, *CHANGE_COLS]:
        if col not in df.columns:
            df[col] = ""

    snapshots = _snapshot_order(df["Snapshot ID"].tolist())
    if len(snapshots) < 2:
        print("No pricing changes detected")
        return 0

    latest_snapshot = snapshots[-1]
    previous_snapshot = snapshots[-2]

    latest_df = df[df["Snapshot ID"].apply(_norm_text) == latest_snapshot].copy()
    previous_df = df[df["Snapshot ID"].apply(_norm_text) == previous_snapshot].copy()

    latest_map = _collapse_snapshot(latest_df)
    previous_map = _collapse_snapshot(previous_df)
    deltas = _build_deltas(previous_map, latest_map)

    has_changes = any(
        [
            deltas["new_options"],
            deltas["removed_options"],
            deltas["price_changes"],
            deltas["contract_value_changes"],
            deltas["incentive_changes"],
            deltas["availability_changes"],
        ]
    )

    if not has_changes:
        print("No pricing changes detected")
        return 0

    subject = "PBSA Pricing Changes Detected \u2014 Southampton"
    ai_summary = _generate_ai_summary(deltas)
    body = _build_email_body(
        latest_snapshot_id=latest_snapshot,
        previous_snapshot_id=previous_snapshot,
        deltas=deltas,
        latest_df=latest_df,
        ai_summary=ai_summary,
    )

    _send_email(subject, body)
    print("Pricing change report email sent")
    print(f"Latest snapshot: {latest_snapshot}")
    print(f"Previous snapshot: {previous_snapshot}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
