from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from html import escape
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

import detect_changes_and_report as base

CITY = "Southampton"
LOOKBACK_DAYS = 7


@dataclass
class ActivityRecord:
    events: int = 0
    intervals: set[str] = field(default_factory=set)
    categories: set[str] = field(default_factory=set)


def _ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df


def _load_city_snapshot_data(city: str) -> tuple[pd.DataFrame, list[str], pd.Timestamp, pd.Timestamp]:
    if not base.WORKBOOK_PATH.exists():
        raise FileNotFoundError(f"Workbook not found: {base.WORKBOOK_PATH}")

    df = pd.read_excel(base.WORKBOOK_PATH, sheet_name=base.SHEET_NAME, engine="openpyxl")
    if df.empty:
        return df, [], pd.NaT, pd.NaT

    df = _ensure_columns(
        df,
        [
            "Snapshot ID",
            "Snapshot Date",
            "City",
            *base.KEY_COLS,
            *base.CHANGE_COLS,
            "Source URL",
        ],
    )

    city_norm = city.strip().casefold()
    df = df[df["City"].apply(base._norm_text).str.casefold() == city_norm].copy()
    if df.empty:
        return df, [], pd.NaT, pd.NaT

    df["Snapshot Timestamp"] = pd.to_datetime(df["Snapshot ID"], errors="coerce")
    fallback_dates = pd.to_datetime(df["Snapshot Date"], errors="coerce")
    missing_timestamp = df["Snapshot Timestamp"].isna()
    df.loc[missing_timestamp, "Snapshot Timestamp"] = fallback_dates[missing_timestamp]
    df = df[df["Snapshot Timestamp"].notna()].copy()
    if df.empty:
        return df, [], pd.NaT, pd.NaT

    latest_day = df["Snapshot Timestamp"].max().normalize()
    window_start = latest_day - pd.Timedelta(days=LOOKBACK_DAYS - 1)
    df = df[df["Snapshot Timestamp"].dt.normalize() >= window_start].copy()
    df.sort_values(["Snapshot Timestamp", "Snapshot ID"], inplace=True)
    snapshots = base._snapshot_order(df["Snapshot ID"].tolist())
    return df, snapshots, window_start, latest_day


def _empty_activity_map() -> dict[str, Any]:
    return {
        "pair_records": defaultdict(ActivityRecord),
        "operator_records": defaultdict(ActivityRecord),
        "category_pair_intervals": defaultdict(set),
        "category_operator_intervals": defaultdict(set),
        "price_events": [],
        "contract_events": [],
        "incentive_events": [],
        "availability_events": [],
        "price_key_counts": Counter(),
        "contract_key_counts": Counter(),
        "incentive_key_counts": Counter(),
        "availability_key_counts": Counter(),
        "snapshot_pairs": [],
    }


def _pair_from_key(key: Tuple[str, ...]) -> tuple[str, str]:
    return base._norm_text(key[0]), base._norm_text(key[1])


def _room_details_from_key(key: Tuple[str, ...]) -> dict[str, str]:
    op, prop, room, contract, ay, floor = key
    return {
        "operator": base._norm_text(op),
        "property": base._norm_text(prop),
        "room_name": base._norm_text(room),
        "contract_length": base._norm_text(contract),
        "academic_year": base._norm_text(ay),
        "floor_level": base._norm_text(floor),
    }


def _format_pair(pair: tuple[str, str]) -> str:
    operator, property_name = pair
    if operator and property_name:
        return f"{operator} | {property_name}"
    return operator or property_name or "Unknown"


def _record_activity(
    store: dict[str, Any],
    key: Tuple[str, ...],
    interval_label: str,
    category: str,
) -> None:
    pair = _pair_from_key(key)
    operator = pair[0] or "Unknown"

    pair_record = store["pair_records"][pair]
    pair_record.events += 1
    pair_record.intervals.add(interval_label)
    pair_record.categories.add(category)

    operator_record = store["operator_records"][operator]
    operator_record.events += 1
    operator_record.intervals.add(interval_label)
    operator_record.categories.add(category)

    store["category_pair_intervals"][(category, pair)].add(interval_label)
    store["category_operator_intervals"][(category, operator)].add(interval_label)


def _source_url_for_key(
    key: Tuple[str, ...],
    previous_map: Dict[Tuple[str, ...], Dict[str, Any]],
    latest_map: Dict[Tuple[str, ...], Dict[str, Any]],
) -> str:
    latest_url = base._norm_text(latest_map.get(key, {}).get("Source URL"))
    previous_url = base._norm_text(previous_map.get(key, {}).get("Source URL"))
    return latest_url or previous_url


def _collect_weekly_activity(df: pd.DataFrame, snapshot_ids: list[str]) -> dict[str, Any]:
    store = _empty_activity_map()
    if not snapshot_ids:
        return store

    snapshot_frames = {
        snapshot_id: df[df["Snapshot ID"].apply(base._norm_text) == snapshot_id].copy()
        for snapshot_id in snapshot_ids
    }

    for previous_snapshot, latest_snapshot in zip(snapshot_ids, snapshot_ids[1:]):
        interval_label = f"{previous_snapshot} -> {latest_snapshot}"
        store["snapshot_pairs"].append(interval_label)

        previous_map = base._collapse_snapshot(snapshot_frames[previous_snapshot])
        latest_map = base._collapse_snapshot(snapshot_frames[latest_snapshot])
        deltas = base._build_deltas(previous_map, latest_map)

        for delta in deltas["price_changes"]:
            details = _room_details_from_key(delta.key)
            old_price = delta.old.get("Price")
            new_price = delta.new.get("Price")
            change_amount = None
            direction = "updated"
            if old_price is not None and new_price is not None:
                change_amount = round(new_price - old_price, 2)
                if change_amount > 0:
                    direction = "increase"
                elif change_amount < 0:
                    direction = "decrease"
            store["price_events"].append(
                {
                    **details,
                    "previous_snapshot": previous_snapshot,
                    "latest_snapshot": latest_snapshot,
                    "previous_price": old_price,
                    "latest_price": new_price,
                    "change_amount": change_amount,
                    "direction": direction,
                    "source_url": _source_url_for_key(delta.key, previous_map, latest_map),
                }
            )
            store["price_key_counts"][delta.key] += 1
            _record_activity(store, delta.key, interval_label, "price")

        for key in deltas["new_options"]:
            details = _room_details_from_key(key)
            store["contract_events"].append(
                {
                    **details,
                    "previous_snapshot": previous_snapshot,
                    "latest_snapshot": latest_snapshot,
                    "action": "added",
                    "source_url": _source_url_for_key(key, previous_map, latest_map),
                }
            )
            store["contract_key_counts"][key] += 1
            _record_activity(store, key, interval_label, "contract")

        for key in deltas["removed_options"]:
            details = _room_details_from_key(key)
            store["contract_events"].append(
                {
                    **details,
                    "previous_snapshot": previous_snapshot,
                    "latest_snapshot": latest_snapshot,
                    "action": "removed",
                    "source_url": _source_url_for_key(key, previous_map, latest_map),
                }
            )
            store["contract_key_counts"][key] += 1
            _record_activity(store, key, interval_label, "contract")

        for delta in deltas["incentive_changes"]:
            details = _room_details_from_key(delta.key)
            store["incentive_events"].append(
                {
                    **details,
                    "previous_snapshot": previous_snapshot,
                    "latest_snapshot": latest_snapshot,
                    "old_incentive": base._norm_text(delta.old.get("Incentives")) or "blank",
                    "new_incentive": base._norm_text(delta.new.get("Incentives")) or "blank",
                    "source_url": _source_url_for_key(delta.key, previous_map, latest_map),
                }
            )
            store["incentive_key_counts"][delta.key] += 1
            _record_activity(store, delta.key, interval_label, "incentive")

        for delta in deltas["availability_changes"]:
            details = _room_details_from_key(delta.key)
            store["availability_events"].append(
                {
                    **details,
                    "previous_snapshot": previous_snapshot,
                    "latest_snapshot": latest_snapshot,
                    "old_availability": base._norm_text(delta.old.get("Availability")) or "Unknown",
                    "new_availability": base._norm_text(delta.new.get("Availability")) or "Unknown",
                    "source_url": _source_url_for_key(delta.key, previous_map, latest_map),
                }
            )
            store["availability_key_counts"][delta.key] += 1
            _record_activity(store, delta.key, interval_label, "availability")

    return store


def _category_repeats(store: dict[str, Any], category: str) -> list[tuple[tuple[str, str], ActivityRecord]]:
    repeats: list[tuple[tuple[str, str], ActivityRecord]] = []
    for pair, record in store["pair_records"].items():
        if category in record.categories and len(store["category_pair_intervals"][(category, pair)]) >= 2:
            repeat_record = ActivityRecord(
                events=record.events,
                intervals=set(store["category_pair_intervals"][(category, pair)]),
                categories={category},
            )
            repeats.append((pair, repeat_record))
    repeats.sort(key=lambda item: (-len(item[1].intervals), -item[1].events, item[0][0], item[0][1]))
    return repeats


def _classify_market_sentiment(store: dict[str, Any]) -> str:
    total_events = (
        len(store["price_events"])
        + len(store["contract_events"])
        + len(store["incentive_events"])
        + len(store["availability_events"])
    )
    repeated_pairs = sum(1 for _, record in store["pair_records"].items() if len(record.intervals) >= 2)
    multi_category_pairs = sum(1 for _, record in store["pair_records"].items() if len(record.categories) >= 2)
    increases = sum(1 for event in store["price_events"] if event["direction"] == "increase")
    decreases = sum(1 for event in store["price_events"] if event["direction"] == "decrease")

    if total_events == 0:
        return "Stable"
    if repeated_pairs >= 3 or multi_category_pairs >= 3 or total_events >= 24:
        return "Structurally Active"
    if increases >= max(4, decreases * 2) and increases > decreases:
        return "Firming"
    if decreases >= max(4, increases * 2) and decreases > increases:
        return "Softening"
    return "Stable"


def _biggest_price_move(events: list[dict[str, Any]], direction: str) -> str:
    filtered = [
        event
        for event in events
        if event["change_amount"] is not None
        and ((direction == "increase" and event["change_amount"] > 0) or (direction == "decrease" and event["change_amount"] < 0))
    ]
    if not filtered:
        return "None detected."

    key_func = max if direction == "increase" else min
    event = key_func(filtered, key=lambda item: item["change_amount"])
    move = event["change_amount"] or 0.0
    return (
        f"{event['operator']} | {event['property']} | {event['room_name']} | "
        f"{event['contract_length'] or 'blank'} | {base._fmt_money(event['previous_price'])} -> "
        f"{base._fmt_money(event['latest_price'])} ({move:+.2f})"
    )


def _format_repeat_lines(repeats: list[tuple[tuple[str, str], ActivityRecord]], empty_text: str) -> list[str]:
    if not repeats:
        return [empty_text]
    lines: list[str] = []
    for pair, record in repeats[:5]:
        lines.append(f"{_format_pair(pair)} changed in {len(record.intervals)} snapshot comparisons.")
    return lines


def _format_operator_activity_lines(store: dict[str, Any]) -> list[str]:
    ranked = sorted(
        store["operator_records"].items(),
        key=lambda item: (-len(item[1].intervals), -item[1].events, item[0]),
    )
    if not ranked:
        return ["No operator-level activity was detected."]
    lines: list[str] = []
    for operator, record in ranked[:5]:
        category_text = ", ".join(sorted(record.categories))
        lines.append(
            f"{operator}: {record.events} tracked changes across {len(record.intervals)} snapshot comparisons "
            f"({category_text})."
        )
    return lines


def _format_property_activity_lines(store: dict[str, Any]) -> list[str]:
    ranked = sorted(
        store["pair_records"].items(),
        key=lambda item: (-len(item[1].intervals), -item[1].events, item[0][0], item[0][1]),
    )
    if not ranked:
        return ["No property-level activity was detected."]
    lines: list[str] = []
    for pair, record in ranked[:6]:
        category_text = ", ".join(sorted(record.categories))
        lines.append(
            f"{_format_pair(pair)}: {record.events} tracked changes across {len(record.intervals)} snapshot comparisons "
            f"({category_text})."
        )
    return lines


def _format_pricing_lines(store: dict[str, Any]) -> list[str]:
    increases = sum(1 for event in store["price_events"] if event["direction"] == "increase")
    decreases = sum(1 for event in store["price_events"] if event["direction"] == "decrease")
    repeated = _category_repeats(store, "price")
    lines = [
        f"Price increases detected: {increases}.",
        f"Price decreases detected: {decreases}.",
        f"Largest increase: {_biggest_price_move(store['price_events'], 'increase')}",
        f"Largest decrease: {_biggest_price_move(store['price_events'], 'decrease')}",
    ]
    lines.extend(_format_repeat_lines(repeated, "No repeated price changes were detected across multiple snapshot comparisons."))
    return lines


def _format_incentive_lines(store: dict[str, Any]) -> list[str]:
    repeated = _category_repeats(store, "incentive")
    lines = [
        f"Incentive changes detected: {len(store['incentive_events'])}.",
    ]
    lines.extend(_format_repeat_lines(repeated, "No repeated incentive changes were detected across multiple snapshot comparisons."))

    for event in store["incentive_events"][:5]:
        lines.append(
            f"{event['operator']} | {event['property']} | {event['room_name']} | "
            f"{event['contract_length'] or 'blank'} | {event['old_incentive']} -> {event['new_incentive']}."
        )
    return lines


def _format_availability_lines(store: dict[str, Any]) -> list[str]:
    repeated = _category_repeats(store, "availability")
    lines = [
        f"Availability changes detected: {len(store['availability_events'])}.",
    ]
    lines.extend(_format_repeat_lines(repeated, "No repeated availability changes were detected across multiple snapshot comparisons."))

    for event in store["availability_events"][:5]:
        lines.append(
            f"{event['operator']} | {event['property']} | {event['room_name']} | "
            f"{event['contract_length'] or 'blank'} | {event['old_availability']} -> {event['new_availability']}."
        )
    return lines


def _format_contract_lines(store: dict[str, Any]) -> list[str]:
    added = sum(1 for event in store["contract_events"] if event["action"] == "added")
    removed = sum(1 for event in store["contract_events"] if event["action"] == "removed")
    repeated = _category_repeats(store, "contract")
    lines = [
        f"Contract options added: {added}.",
        f"Contract options removed: {removed}.",
    ]
    lines.extend(_format_repeat_lines(repeated, "No repeated contract-structure changes were detected across multiple snapshot comparisons."))

    for event in store["contract_events"][:6]:
        lines.append(
            f"{event['operator']} | {event['property']} | {event['room_name']} | "
            f"{event['contract_length'] or 'blank'} | {event['academic_year'] or 'blank'} | "
            f"Contract option {event['action']}."
        )
    return lines


def _format_weekly_signal_lines(store: dict[str, Any], snapshot_count: int) -> list[str]:
    pair_count = len(store["pair_records"])
    repeated_pairs = sum(1 for _, record in store["pair_records"].items() if len(record.intervals) >= 2)
    multi_category_pairs = sum(1 for _, record in store["pair_records"].items() if len(record.categories) >= 2)
    total_events = (
        len(store["price_events"])
        + len(store["contract_events"])
        + len(store["incentive_events"])
        + len(store["availability_events"])
    )
    return [
        f"Snapshots analysed: {snapshot_count}, covering {max(snapshot_count - 1, 0)} consecutive snapshot comparisons.",
        f"Tracked movements detected: {total_events} across {pair_count} operator/property pairs.",
        f"Repeatedly active operator/property pairs: {repeated_pairs}.",
        f"Operator/property pairs with multi-category changes: {multi_category_pairs}.",
        f"Repeated price-change pairs: {len(_category_repeats(store, 'price'))}.",
        f"Repeated contract-change pairs: {len(_category_repeats(store, 'contract'))}.",
        f"Repeated incentive-change pairs: {len(_category_repeats(store, 'incentive'))}.",
        f"Repeated availability-change pairs: {len(_category_repeats(store, 'availability'))}.",
    ]


def _watchlist_rows(store: dict[str, Any]) -> list[dict[str, str]]:
    ranked = sorted(
        store["pair_records"].items(),
        key=lambda item: (-len(item[1].intervals), -item[1].events, -len(item[1].categories), item[0][0], item[0][1]),
    )
    rows: list[dict[str, str]] = []
    for pair, record in ranked[:8]:
        note_parts = []
        if "price" in record.categories:
            note_parts.append("price movement")
        if "contract" in record.categories:
            note_parts.append("contract resets")
        if "incentive" in record.categories:
            note_parts.append("incentive changes")
        if "availability" in record.categories:
            note_parts.append("availability changes")
        rows.append(
            {
                "operator": pair[0] or "Unknown",
                "property": pair[1] or "Unknown",
                "events": str(record.events),
                "intervals": str(len(record.intervals)),
                "categories": ", ".join(sorted(record.categories)) or "None",
                "note": ", ".join(note_parts) or "Monitor for fresh movement.",
            }
        )
    return rows


def _build_watchlist_table(rows: list[dict[str, str]]) -> str:
    if not rows:
        return '<p style="margin:0; font-size:14px; color:#52606d;">No watchlist properties were identified this week.</p>'

    header_style = (
        "padding:12px 14px; border:1px solid #d9e2ec; background-color:#eef2f7; "
        "font-size:13px; font-weight:700; text-align:left; color:#243b53;"
    )
    cell_style = "padding:12px 14px; border:1px solid #d9e2ec; font-size:13px; color:#243b53; vertical-align:top;"

    parts = [
        '<table style="width:100%; border-collapse:collapse; margin:0;">',
        "<thead><tr>",
    ]
    for heading in ["Operator", "Property", "Events", "Intervals", "Categories", "Watchlist Note"]:
        parts.append(f'<th style="{header_style}">{escape(heading)}</th>')
    parts.append("</tr></thead><tbody>")

    for row in rows:
        parts.append("<tr>")
        parts.append(f'<td style="{cell_style}">{escape(row["operator"])}</td>')
        parts.append(f'<td style="{cell_style}">{escape(row["property"])}</td>')
        parts.append(f'<td style="{cell_style}">{escape(row["events"])}</td>')
        parts.append(f'<td style="{cell_style}">{escape(row["intervals"])}</td>')
        parts.append(f'<td style="{cell_style}">{escape(row["categories"])}</td>')
        parts.append(f'<td style="{cell_style}">{escape(row["note"])}</td>')
        parts.append("</tr>")
    parts.append("</tbody></table>")
    return "".join(parts)


def _build_html_list(items: list[str]) -> str:
    if not items:
        return '<ul style="margin:0 0 0 18px; padding:0; color:#243b53;"><li style="margin:0 0 8px 0;">None noted.</li></ul>'
    parts = ['<ul style="margin:0 0 0 18px; padding:0; color:#243b53;">']
    for item in items:
        parts.append(f'<li style="margin:0 0 8px 0;">{escape(base._norm_text(item))}</li>')
    parts.append("</ul>")
    return "".join(parts)


def _section_html(title: str, body_html: str) -> str:
    return (
        f'<div style="margin:26px 0 0 0; font-size:18px; font-weight:700; color:#102a43;">{escape(title)}</div>'
        f'<div style="margin:12px 0 0 0; font-size:14px; color:#243b53; line-height:1.7;">{body_html}</div>'
    )


def _activity_highlights(store: dict[str, Any]) -> dict[str, str]:
    operator_ranked = sorted(
        store["operator_records"].items(),
        key=lambda item: (-len(item[1].intervals), -item[1].events, item[0]),
    )
    property_ranked = sorted(
        store["pair_records"].items(),
        key=lambda item: (-len(item[1].intervals), -item[1].events, item[0][0], item[0][1]),
    )
    busiest_operator = operator_ranked[0][0] if operator_ranked else "None detected"
    busiest_property = _format_pair(property_ranked[0][0]) if property_ranked else "None detected"
    return {
        "busiest_operator": busiest_operator,
        "busiest_property": busiest_property,
        "largest_increase": _biggest_price_move(store["price_events"], "increase"),
        "largest_decrease": _biggest_price_move(store["price_events"], "decrease"),
    }


def _fallback_overview(
    sentiment_label: str,
    store: dict[str, Any],
    snapshot_count: int,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> str:
    highlights = _activity_highlights(store)
    increases = sum(1 for event in store["price_events"] if event["direction"] == "increase")
    decreases = sum(1 for event in store["price_events"] if event["direction"] == "decrease")
    total_events = (
        len(store["price_events"])
        + len(store["contract_events"])
        + len(store["incentive_events"])
        + len(store["availability_events"])
    )
    repeated_pairs = sum(1 for _, record in store["pair_records"].items() if len(record.intervals) >= 2)
    return (
        f"Across {snapshot_count} Southampton snapshots between {window_start:%d %b %Y} and {window_end:%d %b %Y}, "
        f"the market looked {sentiment_label.lower()}. We recorded {total_events} tracked movements across "
        f"{len(store['pair_records'])} operator/property pairs, with {increases} price increases and {decreases} price decreases. "
        f"The most active operator was {highlights['busiest_operator']}, while {highlights['busiest_property']} "
        f"showed the heaviest property-level activity. Repeated movements were concentrated at {repeated_pairs} "
        f"operator/property pairs over the week."
    )


def _generate_ai_weekly_summary(
    sentiment_label: str,
    store: dict[str, Any],
    snapshot_count: int,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> str:
    total_events = (
        len(store["price_events"])
        + len(store["contract_events"])
        + len(store["incentive_events"])
        + len(store["availability_events"])
    )
    highlights = _activity_highlights(store)
    prompt_lines = [
        "Write a concise internal PBSA weekly market sentiment summary for Southampton.",
        "Use only the supplied facts. Do not invent causes, competitor intent, or unsupported reasons.",
        "Keep the tone professional, clear, and useful for internal market intelligence.",
        "Return one short paragraph in plain text only, with 4 to 6 sentences and no bullets, headings, markdown, or signature.",
        f"Weekly window: {window_start:%Y-%m-%d} to {window_end:%Y-%m-%d}",
        f"Market sentiment label from heuristics: {sentiment_label}",
        f"Snapshot count analysed: {snapshot_count}",
        f"Snapshot comparisons analysed: {max(snapshot_count - 1, 0)}",
        f"Total tracked movements: {total_events}",
        f"Price increases: {sum(1 for event in store['price_events'] if event['direction'] == 'increase')}",
        f"Price decreases: {sum(1 for event in store['price_events'] if event['direction'] == 'decrease')}",
        f"Contract option changes: {len(store['contract_events'])}",
        f"Incentive changes: {len(store['incentive_events'])}",
        f"Availability changes: {len(store['availability_events'])}",
        f"Repeated price-change pairs: {len(_category_repeats(store, 'price'))}",
        f"Repeated contract-change pairs: {len(_category_repeats(store, 'contract'))}",
        f"Repeated incentive-change pairs: {len(_category_repeats(store, 'incentive'))}",
        f"Repeated availability-change pairs: {len(_category_repeats(store, 'availability'))}",
        f"Most active operator: {highlights['busiest_operator']}",
        f"Most active property: {highlights['busiest_property']}",
        f"Largest price increase: {highlights['largest_increase']}",
        f"Largest price decrease: {highlights['largest_decrease']}",
        "Top operator activity lines:",
        *_format_operator_activity_lines(store)[:5],
        "Top property activity lines:",
        *_format_property_activity_lines(store)[:6],
        "Pricing summary lines:",
        *_format_pricing_lines(store)[:6],
        "Contract summary lines:",
        *_format_contract_lines(store)[:6],
        "Incentive summary lines:",
        *_format_incentive_lines(store)[:6],
        "Availability summary lines:",
        *_format_availability_lines(store)[:6],
    ]
    return base._call_openai("\n".join(prompt_lines), max_output_tokens=320)


def _build_email_body(
    sentiment_label: str,
    overview: str,
    store: dict[str, Any],
    snapshot_count: int,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> str:
    window_text = f"{window_start:%d %b %Y} to {window_end:%d %b %Y}"
    watchlist_html = _build_watchlist_table(_watchlist_rows(store))
    html_parts = [
        "<html><body style=\"margin:0; padding:0; background-color:#f5f7fa;\">",
        '<div style="padding:24px 12px; background-color:#f5f7fa;">',
        '<div style="max-width:1080px; margin:0 auto; background-color:#ffffff; border:1px solid #d9e2ec; border-radius:10px; padding:32px; font-family:Arial, Helvetica, sans-serif; color:#243b53; line-height:1.6;">',
        '<p style="margin:0 0 12px 0; font-size:16px;">Hi team,</p>',
        '<p style="margin:0 0 18px 0; font-size:16px;">Please see below your PBSA Market Intelligence summary for Southampton.</p>',
        (
            '<p style="margin:0 0 20px 0; font-size:12px; color:#7b8794;">'
            f"Weekly window: {escape(window_text)}<br>"
            f"Southampton snapshots analysed: {snapshot_count}"
            "</p>"
        ),
        _section_html(
            "Market sentiment",
            (
                f'<p style="margin:0 0 12px 0;"><strong>Weekly view:</strong> {escape(sentiment_label)}</p>'
                f'<p style="margin:0;">{escape(base._norm_text(overview))}</p>'
            ),
        ),
        _section_html("Weekly trend signals", _build_html_list(_format_weekly_signal_lines(store, snapshot_count))),
        _section_html(
            "Most repeatedly active operators/properties",
            (
                '<div style="font-weight:700; margin:0 0 8px 0;">Operators</div>'
                f"{_build_html_list(_format_operator_activity_lines(store))}"
                '<div style="font-weight:700; margin:18px 0 8px 0;">Properties</div>'
                f"{_build_html_list(_format_property_activity_lines(store))}"
            ),
        ),
        _section_html("Pricing direction summary", _build_html_list(_format_pricing_lines(store))),
        _section_html("Incentive summary", _build_html_list(_format_incentive_lines(store))),
        _section_html("Availability summary", _build_html_list(_format_availability_lines(store))),
        _section_html("Contract structure summary", _build_html_list(_format_contract_lines(store))),
        _section_html("Watchlist / properties to monitor", watchlist_html),
    ]
    html = "".join(html_parts)
    return base._append_html_signature(html) + "</div></div></body></html>"


def main() -> int:
    try:
        df, snapshot_ids, window_start, window_end = _load_city_snapshot_data(CITY)
    except FileNotFoundError as exc:
        print(str(exc))
        return 1

    if df.empty or not snapshot_ids:
        print(f"No {CITY} snapshots available for weekly summary.")
        return 0

    store = _collect_weekly_activity(df, snapshot_ids)
    sentiment_label = _classify_market_sentiment(store)
    overview = _generate_ai_weekly_summary(
        sentiment_label=sentiment_label,
        store=store,
        snapshot_count=len(snapshot_ids),
        window_start=window_start,
        window_end=window_end,
    )
    if not base._norm_text(overview):
        overview = _fallback_overview(
            sentiment_label=sentiment_label,
            store=store,
            snapshot_count=len(snapshot_ids),
            window_start=window_start,
            window_end=window_end,
        )

    body = _build_email_body(
        sentiment_label=sentiment_label,
        overview=overview,
        store=store,
        snapshot_count=len(snapshot_ids),
        window_start=window_start,
        window_end=window_end,
    )
    subject = f"Weekly PBSA Market Summary - Southampton ({window_end:%d %b %Y})"
    base._send_email(subject, body, subtype="html")

    print("Weekly Southampton market summary email sent")
    print(f"Window start: {window_start:%Y-%m-%d}")
    print(f"Window end: {window_end:%Y-%m-%d}")
    print(f"Snapshots analysed: {len(snapshot_ids)}")
    print(f"Weekly sentiment: {sentiment_label}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
