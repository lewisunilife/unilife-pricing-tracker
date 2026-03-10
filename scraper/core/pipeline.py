import asyncio
from contextlib import suppress
import datetime as dt
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import yaml
from playwright.async_api import async_playwright

from scraper.core.api_detector import extract_api_rows
from scraper.core.coverage import CoverageTracker
from scraper.core.ids import hall_id, room_id
from scraper.core.models import SnapshotContext, SourceRecord
from scraper.core.normalisers import normalize_space
from scraper.core.playwright_helpers import safe_goto
from scraper.core.validators import infer_missing_price_reason, is_publishable_row, validate_row
from scraper.core.workbook import OUTPUT_COLUMNS, append_rows, dedupe_within_run, migrate_workbook
from scraper.parsers import get_adapter

LONDON_TZ = ZoneInfo("Europe/London")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


async def _new_pipeline_page(context_browser, default_timeout_ms: int, default_navigation_timeout_ms: int):
    page = await context_browser.new_page()
    page.set_default_timeout(default_timeout_ms)
    page.set_default_navigation_timeout(default_navigation_timeout_ms)
    return page


async def _replace_pipeline_page(context_browser, page, default_timeout_ms: int, default_navigation_timeout_ms: int):
    try:
        await page.close()
    except Exception:
        pass
    return await _new_pipeline_page(context_browser, default_timeout_ms, default_navigation_timeout_ms)


async def _await_with_timeout(coro, timeout_seconds: float):
    task = asyncio.create_task(coro)
    try:
        return await asyncio.wait_for(task, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await task
        raise


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def workbook_path() -> Path:
    return repo_root() / "data" / "Unilife_Pricing_Snapshot.xlsx"


def scrape_source_label() -> str:
    return "GitHub Actions" if os.getenv("GITHUB_ACTIONS", "").lower() == "true" else "Local"


def should_run_for_london_9am() -> Tuple[bool, str]:
    event_name = os.getenv("GITHUB_EVENT_NAME", "").strip().lower()
    enforce = os.getenv("ENFORCE_LONDON_9AM", "").strip().lower() in {"1", "true", "yes"}
    now = dt.datetime.now(LONDON_TZ)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S %Z")
    if event_name == "schedule":
        reason = f"scheduled event at {timestamp}; London time gate is informational only (no skip enforced)"
        print(f"[INFO] {reason}")
        return True, reason
    if enforce:
        reason = f"ENFORCE_LONDON_9AM=true at {timestamp}; strict gate disabled, run continues"
        print(f"[INFO] {reason}")
        return True, reason
    return True, f"manual/non-schedule event '{event_name or 'unknown'}' at {timestamp}; no gate enforcement"


def load_sources(config_dir: Path, city_filter: Optional[str] = None) -> List[SourceRecord]:
    cities_file = config_dir / "cities.yaml"
    with cities_file.open("r", encoding="utf-8") as fh:
        city_config = yaml.safe_load(fh) or {}

    sources: List[SourceRecord] = []
    for city_entry in city_config.get("cities", []):
        city_name = city_entry["name"]
        if city_filter and city_filter.lower() != city_name.lower():
            continue
        city_path = config_dir / city_entry["config"]
        with city_path.open("r", encoding="utf-8") as fh:
            payload = yaml.safe_load(fh) or {}
        for src in payload.get("sources", []):
            sources.append(
                SourceRecord(
                    city=payload.get("city", city_name),
                    operator=src["operator"],
                    property=src["property"],
                    parser=src["parser"],
                    primary_url=src["primary_url"],
                    secondary_urls=src.get("secondary_urls", []) or [],
                    notes=src.get("notes", ""),
                )
            )
    return sources


def build_candidate_row(
    source: SourceRecord,
    url: str,
    raw: Dict[str, Any],
    context: SnapshotContext,
) -> Tuple[Dict[str, Any], str]:
    room_name = raw.get("Room Name", "")
    property_name = normalize_space(raw.get("Property", "")) or source.property
    row: Dict[str, Any] = {
        "Snapshot ID": context.snapshot_id,
        "Snapshot Date": context.snapshot_date,
        "City": source.city,
        "Operator": source.operator,
        "HALL ID": "",
        "Property": property_name,
        "ROOM ID": "",
        "Room Name": room_name,
        "Floor Level": raw.get("Floor Level", ""),
        "Contract Length": raw.get("Contract Length", ""),
        "Academic Year": raw.get("Academic Year", ""),
        "Price": raw.get("Price", ""),
        "Contract Value": raw.get("Contract Value", ""),
        "Incentives": raw.get("Incentives", ""),
        "Availability": raw.get("Availability", ""),
        "Source URL": normalize_space(raw.get("Source URL", "")) or url,
        "Scrape Source": context.scrape_source,
    }
    cleaned, issues = validate_row(row)
    cleaned["HALL ID"] = hall_id(cleaned["Operator"], cleaned["Property"]) if normalize_space(cleaned["Property"]) else ""
    cleaned["ROOM ID"] = (
        room_id(cleaned["Operator"], cleaned["Property"], cleaned["Room Name"])
        if normalize_space(cleaned["Property"]) and normalize_space(cleaned["Room Name"])
        else ""
    )
    cleaned["__missing_price_reason"] = infer_missing_price_reason(raw, cleaned, issues)
    if issues:
        cleaned["__validation_issues"] = "|".join(issues)
    return cleaned, cleaned["__missing_price_reason"]


async def _classify_block_reason(page) -> str:
    try:
        text = (await page.inner_text("body")).lower()
    except Exception:
        return "unable to read body"
    if "captcha" in text or "cloudflare" in text or "access denied" in text:
        return "anti-bot/challenge page"
    if "forbidden" in text:
        return "forbidden response text"
    return "no extractable room rows"


async def run_pipeline(city: Optional[str] = None, force_9am_gate: bool = True) -> Dict[str, Any]:
    event_name = os.getenv("GITHUB_EVENT_NAME", "").strip().lower() or "unknown"
    gate_enforced = False
    _gate_ok, gate_reason = should_run_for_london_9am()

    mig = migrate_workbook(workbook_path())
    print(f"[INFO] Workbook migration complete: {mig['before']} -> {mig['after']} rows")

    now = dt.datetime.now(LONDON_TZ)
    context = SnapshotContext(
        snapshot_id=now.strftime("%Y-%m-%dT%H:%M:%S"),
        snapshot_date=now.date().isoformat(),
        scrape_source=scrape_source_label(),
    )

    sources = load_sources(repo_root() / "scraper" / "config", city_filter=city)
    tracker = CoverageTracker()
    raw_records: List[Dict[str, Any]] = []
    validated_records: List[Dict[str, Any]] = []
    source_outcomes: List[Dict[str, Any]] = []
    source_timeout_seconds = _env_int("SOURCE_TIMEOUT_SECONDS", 300)
    url_stage_timeout_seconds = _env_int("URL_STAGE_TIMEOUT_SECONDS", 120)
    goto_timeout_ms = _env_int("GOTO_TIMEOUT_MS", 90000)
    default_timeout_ms = _env_int("PLAYWRIGHT_TIMEOUT_MS", 45000)
    default_navigation_timeout_ms = _env_int("PLAYWRIGHT_NAV_TIMEOUT_MS", 90000)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context_browser = await browser.new_context()
        context_browser.set_default_timeout(default_timeout_ms)
        context_browser.set_default_navigation_timeout(default_navigation_timeout_ms)
        page = await _new_pipeline_page(context_browser, default_timeout_ms, default_navigation_timeout_ms)
        try:
            for source in sources:
                source_start = time.monotonic()
                source_deadline = source_start + source_timeout_seconds
                source_timed_out = False
                source_failed = False
                source_reason = ""

                adapter = get_adapter(source.parser)
                if not adapter:
                    for url in source.urls:
                        tracker.add(source, url, "adapter", "none", "failed", "parser adapter missing", 0)
                    elapsed = round(time.monotonic() - source_start, 2)
                    print(
                        f"[OPERATOR_END] {source.operator} | {source.property} | status=failed | "
                        f"elapsed_s={elapsed} | rows_extracted=0 | rows_appended=0 | reason=parser adapter missing"
                    )
                    source_outcomes.append(
                        {
                            "operator": source.operator,
                            "property": source.property,
                            "status": "failed",
                            "elapsed_seconds": elapsed,
                            "rows_extracted": 0,
                            "rows_appended": 0,
                            "reason": "parser adapter missing",
                        }
                    )
                    continue

                print(
                    f"[OPERATOR_START] {source.operator} | {source.property} | source_timeout_s={source_timeout_seconds}"
                )
                property_rows: List[Dict[str, Any]] = []
                source_raw_count = 0

                for url in source.urls:
                    remaining = source_deadline - time.monotonic()
                    if remaining <= 0:
                        source_timed_out = True
                        source_reason = "source timeout reached"
                        tracker.add(source, url, "source", "timeout", "timed_out", source_reason, 0)
                        print(f"[TIMEOUT] {source.operator} | {source.property} | source timeout reached before URL: {url}")
                        break

                    step_timeout = max(1.0, min(float(url_stage_timeout_seconds), remaining))
                    url_low = url.lower()
                    url_kind = "booking_flow" if any(token in url_low for token in ["booking", "signing", "portal", "book-a-room", "/rooms"]) else "brochure_page"
                    print(f"[URL_START] {source.operator} | {source.property} | kind={url_kind} | {url}")

                    # Stage 1: load page (brochure/booking URL).
                    try:
                        ok, load_reason = await _await_with_timeout(
                            safe_goto(page, url, timeout=goto_timeout_ms),
                            step_timeout,
                        )
                    except asyncio.TimeoutError:
                        source_timed_out = True
                        source_reason = f"load timeout after {step_timeout:.1f}s"
                        tracker.add(source, url, "load", "none", "timed_out", source_reason, 0)
                        print(f"[TIMEOUT] {source.operator} | {source.property} | stage=load | url={url} | {source_reason}")
                        page = await _replace_pipeline_page(
                            context_browser,
                            page,
                            default_timeout_ms,
                            default_navigation_timeout_ms,
                        )
                        continue

                    if not ok:
                        tracker.add(source, url, "load", "none", "failed", f"page unavailable: {load_reason}", 0)
                        print(f"[URL_FAIL] {source.operator} | {source.property} | stage=load | reason={load_reason}")
                        continue

                    # Stage 2: API detection.
                    print(f"[STAGE] {source.operator} | {source.property} | url={url} | stage=api")
                    try:
                        api_rows, api_reason = await _await_with_timeout(extract_api_rows(page), step_timeout)
                    except asyncio.TimeoutError:
                        source_timed_out = True
                        source_reason = f"api stage timeout after {step_timeout:.1f}s"
                        tracker.add(source, url, "api", "API", "timed_out", source_reason, 0)
                        print(f"[TIMEOUT] {source.operator} | {source.property} | stage=api | url={url} | {source_reason}")
                        page = await _replace_pipeline_page(
                            context_browser,
                            page,
                            default_timeout_ms,
                            default_navigation_timeout_ms,
                        )
                        continue

                    if api_rows:
                        tracker.add(source, url, "api", "API", "success", api_reason, len(api_rows))
                        source_raw_count += len(api_rows)
                        for row in api_rows:
                            raw_records.append(
                                {
                                    "city": source.city,
                                    "operator": source.operator,
                                    "property": source.property,
                                    "parser": source.parser,
                                    "method": "API",
                                    "url": url,
                                    "payload": row,
                                }
                            )
                            cleaned_row, _reason = build_candidate_row(source, url, row, context)
                            property_rows.append(cleaned_row)
                        print(f"[URL_DONE] {source.operator} | {source.property} | url={url} | stage=api | rows={len(api_rows)}")
                        continue
                    tracker.add(source, url, "api", "API", "failed", api_reason, 0)

                    # Stage 3: DOM parser.
                    print(f"[STAGE] {source.operator} | {source.property} | url={url} | stage=dom")
                    try:
                        dom_rows, dom_reason = await _await_with_timeout(
                            adapter.parse_dom(
                                page,
                                {"operator": source.operator, "property": source.property, "city": source.city, "url": url},
                            ),
                            step_timeout,
                        )
                    except asyncio.TimeoutError:
                        source_timed_out = True
                        source_reason = f"dom parser timeout after {step_timeout:.1f}s"
                        tracker.add(source, url, "dom", "DOM", "timed_out", source_reason, 0)
                        print(f"[TIMEOUT] {source.operator} | {source.property} | stage=dom | url={url} | {source_reason}")
                        page = await _replace_pipeline_page(
                            context_browser,
                            page,
                            default_timeout_ms,
                            default_navigation_timeout_ms,
                        )
                        continue
                    except Exception as exc:
                        source_failed = True
                        source_reason = f"dom parser exception: {exc}"
                        tracker.add(source, url, "dom", "DOM", "failed", source_reason, 0)
                        print(f"[URL_FAIL] {source.operator} | {source.property} | stage=dom | reason={exc}")
                        continue

                    if dom_rows:
                        tracker.add(source, url, "dom", "DOM", "success", dom_reason or "dom extraction", len(dom_rows))
                        source_raw_count += len(dom_rows)
                        for row in dom_rows:
                            raw_records.append(
                                {
                                    "city": source.city,
                                    "operator": source.operator,
                                    "property": source.property,
                                    "parser": source.parser,
                                    "method": "DOM",
                                    "url": url,
                                    "payload": row,
                                }
                            )
                            cleaned_row, _reason = build_candidate_row(source, url, row, context)
                            property_rows.append(cleaned_row)
                        print(f"[URL_DONE] {source.operator} | {source.property} | url={url} | stage=dom | rows={len(dom_rows)}")
                        continue
                    tracker.add(source, url, "dom", "DOM", "failed", dom_reason or "dom extraction failed", 0)

                    # Stage 4: interactive/deep booking parser.
                    print(f"[STAGE] {source.operator} | {source.property} | url={url} | stage=playwright")
                    try:
                        pl_rows, pl_reason = await _await_with_timeout(
                            adapter.parse_interactive(
                                page,
                                {"operator": source.operator, "property": source.property, "city": source.city, "url": url},
                            ),
                            step_timeout,
                        )
                    except asyncio.TimeoutError:
                        source_timed_out = True
                        source_reason = f"playwright parser timeout after {step_timeout:.1f}s"
                        tracker.add(source, url, "playwright", "Playwright", "timed_out", source_reason, 0)
                        print(
                            f"[TIMEOUT] {source.operator} | {source.property} | stage=playwright | url={url} | {source_reason}"
                        )
                        page = await _replace_pipeline_page(
                            context_browser,
                            page,
                            default_timeout_ms,
                            default_navigation_timeout_ms,
                        )
                        continue
                    except Exception as exc:
                        source_failed = True
                        source_reason = f"playwright parser exception: {exc}"
                        tracker.add(source, url, "playwright", "Playwright", "failed", source_reason, 0)
                        print(f"[URL_FAIL] {source.operator} | {source.property} | stage=playwright | reason={exc}")
                        continue

                    if pl_rows:
                        tracker.add(source, url, "playwright", "Playwright", "success", pl_reason or "interactive extraction", len(pl_rows))
                        source_raw_count += len(pl_rows)
                        for row in pl_rows:
                            raw_records.append(
                                {
                                    "city": source.city,
                                    "operator": source.operator,
                                    "property": source.property,
                                    "parser": source.parser,
                                    "method": "Playwright",
                                    "url": url,
                                    "payload": row,
                                }
                            )
                            cleaned_row, _reason = build_candidate_row(source, url, row, context)
                            property_rows.append(cleaned_row)
                        print(
                            f"[URL_DONE] {source.operator} | {source.property} | url={url} | stage=playwright | rows={len(pl_rows)}"
                        )
                    else:
                        block_reason = await _classify_block_reason(page)
                        status = "blocked" if "anti-bot" in block_reason else "failed"
                        tracker.add(source, url, "playwright", "Playwright", status, pl_reason or block_reason, 0)
                        print(
                            f"[URL_DONE] {source.operator} | {source.property} | url={url} | stage=playwright | rows=0 | reason={pl_reason or block_reason}"
                        )

                property_rows = [r for r in property_rows if is_publishable_row(r)]
                validated_records.extend(property_rows)

                elapsed = round(time.monotonic() - source_start, 2)
                if source_timed_out:
                    source_status = "timed_out"
                elif property_rows:
                    source_status = "completed"
                elif source_failed:
                    source_status = "failed"
                else:
                    source_status = "failed"
                    if not source_reason:
                        source_reason = "no publishable rows"

                print(
                    f"[OPERATOR_END] {source.operator} | {source.property} | status={source_status} | "
                    f"elapsed_s={elapsed} | rows_extracted={source_raw_count} | rows_appended={len(property_rows)} | "
                    f"reason={source_reason or '-'}"
                )
                source_outcomes.append(
                    {
                        "operator": source.operator,
                        "property": source.property,
                        "status": source_status,
                        "elapsed_seconds": elapsed,
                        "rows_extracted": source_raw_count,
                        "rows_appended": len(property_rows),
                        "reason": source_reason,
                    }
                )
        finally:
            await page.close()
            await context_browser.close()
            await browser.close()

    validated_records = dedupe_within_run(validated_records)
    if not validated_records:
        sources_attempted = len(source_outcomes)
        sources_completed = sum(1 for outcome in source_outcomes if outcome.get("status") == "completed")
        sources_failed = sum(1 for outcome in source_outcomes if outcome.get("status") == "failed")
        sources_timed_out = sum(1 for outcome in source_outcomes if outcome.get("status") == "timed_out")
        operator_status: Dict[str, str] = {}
        for outcome in source_outcomes:
            op = outcome.get("operator", "")
            status = outcome.get("status", "failed")
            prev_status = operator_status.get(op)
            if prev_status == "completed":
                continue
            if status == "completed":
                operator_status[op] = "completed"
            elif status == "timed_out":
                if prev_status != "completed":
                    operator_status[op] = "timed_out"
            else:
                if op not in operator_status:
                    operator_status[op] = "failed"

        return {
            "status": "no_rows",
            "event_name": event_name,
            "gate_enforced": gate_enforced,
            "gate_reason": gate_reason,
            "snapshot_id": context.snapshot_id,
            "snapshot_date": context.snapshot_date,
            "workbook_path": str(workbook_path()),
            "sources_attempted": sources_attempted,
            "sources_completed": sources_completed,
            "sources_failed": sources_failed,
            "sources_timed_out": sources_timed_out,
            "operators_attempted_total": len(operator_status),
            "operators_completed_total": sum(1 for status in operator_status.values() if status == "completed"),
            "operators_failed_total": sum(1 for status in operator_status.values() if status == "failed"),
            "operators_timed_out_total": sum(1 for status in operator_status.values() if status == "timed_out"),
            "source_outcomes": source_outcomes,
            "coverage_summary": tracker.property_summary(),
        }

    prev, appended = append_rows(workbook_path(), validated_records)
    coverage_summary = tracker.property_summary()

    sources_attempted = len(source_outcomes)
    sources_completed = sum(1 for outcome in source_outcomes if outcome.get("status") == "completed")
    sources_failed = sum(1 for outcome in source_outcomes if outcome.get("status") == "failed")
    sources_timed_out = sum(1 for outcome in source_outcomes if outcome.get("status") == "timed_out")

    operator_status: Dict[str, str] = {}
    for outcome in source_outcomes:
        op = outcome.get("operator", "")
        status = outcome.get("status", "failed")
        prev_status = operator_status.get(op)
        if prev_status == "completed":
            continue
        if status == "completed":
            operator_status[op] = "completed"
        elif status == "timed_out":
            if prev_status != "completed":
                operator_status[op] = "timed_out"
        else:
            if op not in operator_status:
                operator_status[op] = "failed"

    operators_completed = sum(1 for status in operator_status.values() if status == "completed")
    operators_failed = sum(1 for status in operator_status.values() if status == "failed")
    operators_timed_out = sum(1 for status in operator_status.values() if status == "timed_out")

    with_price = sum(1 for r in validated_records if r.get("Price") is not None and not pd.isna(r.get("Price")))
    with_contract_value = sum(
        1 for r in validated_records if r.get("Contract Value") is not None and not pd.isna(r.get("Contract Value"))
    )
    missing_price_reasons: Dict[str, int] = {}
    for row in validated_records:
        if row.get("Price") is not None and not pd.isna(row.get("Price")):
            continue
        reason = normalize_space(row.get("__missing_price_reason", "")).lower() or "not_shown_publicly"
        missing_price_reasons[reason] = missing_price_reasons.get(reason, 0) + 1

    summary = {
        "status": "ok",
        "event_name": event_name,
        "gate_enforced": gate_enforced,
        "gate_reason": gate_reason,
        "snapshot_id": context.snapshot_id,
        "snapshot_date": context.snapshot_date,
        "cities_attempted": sorted({s.city for s in sources}),
        "operators_attempted": sorted({s.operator for s in sources}),
        "properties_attempted": len({(s.city, s.operator, s.property) for s in sources}),
        "sources_attempted": sources_attempted,
        "sources_completed": sources_completed,
        "sources_failed": sources_failed,
        "sources_timed_out": sources_timed_out,
        "operators_attempted_total": len(operator_status),
        "operators_completed_total": operators_completed,
        "operators_failed_total": operators_failed,
        "operators_timed_out_total": operators_timed_out,
        "rows_appended": appended,
        "rows_with_prices": with_price,
        "rows_without_prices": appended - with_price,
        "rows_with_contract_value": with_contract_value,
        "rows_without_contract_value": appended - with_contract_value,
        "missing_price_reasons": missing_price_reasons,
        "historical_total_rows": prev + appended,
        "coverage_summary": coverage_summary,
        "workbook_path": str(workbook_path()),
        "raw_records_count": len(raw_records),
        "validated_records_count": len(validated_records),
        "source_outcomes": source_outcomes,
    }
    return summary


def run(city: Optional[str] = None, force_9am_gate: bool = True) -> Dict[str, Any]:
    return asyncio.run(run_pipeline(city=city, force_9am_gate=force_9am_gate))
