import asyncio
import datetime as dt
import os
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


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def workbook_path() -> Path:
    return repo_root() / "data" / "Unilife_Pricing_Snapshot.xlsx"


def scrape_source_label() -> str:
    return "GitHub Actions" if os.getenv("GITHUB_ACTIONS", "").lower() == "true" else "Local"


def should_run_for_london_9am() -> bool:
    event_name = os.getenv("GITHUB_EVENT_NAME", "").strip().lower()
    enforce = os.getenv("ENFORCE_LONDON_9AM", "").strip().lower() in {"1", "true", "yes"}
    if event_name != "schedule" and not enforce:
        return True
    now = dt.datetime.now(LONDON_TZ)
    if now.hour == 9 and now.minute == 0:
        return True
    print(f"[INFO] Skipping run: Europe/London time is {now.strftime('%Y-%m-%d %H:%M:%S %Z')} (required: 09:00).")
    return False


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
        "Run Timestamp": context.run_timestamp,
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
    if force_9am_gate and not should_run_for_london_9am():
        return {"status": "skipped"}

    mig = migrate_workbook(workbook_path())
    print(f"[INFO] Workbook migration complete: {mig['before']} -> {mig['after']} rows")

    now = dt.datetime.now(LONDON_TZ)
    context = SnapshotContext(
        snapshot_id=now.strftime("%Y-%m-%dT%H:%M:%S"),
        snapshot_date=now.date().isoformat(),
        run_timestamp=now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        scrape_source=scrape_source_label(),
    )

    sources = load_sources(repo_root() / "scraper" / "config", city_filter=city)
    tracker = CoverageTracker()
    raw_records: List[Dict[str, Any]] = []
    validated_records: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context_browser = await browser.new_context()
        page = await context_browser.new_page()
        try:
            for source in sources:
                adapter = get_adapter(source.parser)
                if not adapter:
                    for url in source.urls:
                        tracker.add(source, url, "adapter", "none", "failed", "parser adapter missing", 0)
                    continue

                print(f"[SOURCE] {source.city} | {source.operator} | {source.property}")
                property_rows: List[Dict[str, Any]] = []
                for url in source.urls:
                    ok, load_reason = await safe_goto(page, url)
                    if not ok:
                        tracker.add(source, url, "load", "none", "failed", f"page unavailable: {load_reason}", 0)
                        continue

                    api_rows, api_reason = await extract_api_rows(page)
                    if api_rows:
                        tracker.add(source, url, "api", "API", "success", api_reason, len(api_rows))
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
                        continue
                    tracker.add(source, url, "api", "API", "failed", api_reason, 0)

                    dom_rows, dom_reason = await adapter.parse_dom(page, {"operator": source.operator, "property": source.property, "city": source.city, "url": url})
                    if dom_rows:
                        tracker.add(source, url, "dom", "DOM", "success", dom_reason or "dom extraction", len(dom_rows))
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
                        continue
                    tracker.add(source, url, "dom", "DOM", "failed", dom_reason or "dom extraction failed", 0)

                    pl_rows, pl_reason = await adapter.parse_interactive(page, {"operator": source.operator, "property": source.property, "city": source.city, "url": url})
                    if pl_rows:
                        tracker.add(source, url, "playwright", "Playwright", "success", pl_reason or "interactive extraction", len(pl_rows))
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
                    else:
                        block_reason = await _classify_block_reason(page)
                        status = "blocked" if "anti-bot" in block_reason else "failed"
                        tracker.add(source, url, "playwright", "Playwright", status, pl_reason or block_reason, 0)

                property_rows = [r for r in property_rows if is_publishable_row(r)]
                validated_records.extend(property_rows)
        finally:
            await page.close()
            await context_browser.close()
            await browser.close()

    validated_records = dedupe_within_run(validated_records)
    if not validated_records:
        return {
            "status": "no_rows",
            "snapshot_id": context.snapshot_id,
            "coverage_summary": tracker.property_summary(),
        }

    prev, appended = append_rows(workbook_path(), validated_records)
    coverage_summary = tracker.property_summary()

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
        "snapshot_id": context.snapshot_id,
        "snapshot_date": context.snapshot_date,
        "run_timestamp": context.run_timestamp,
        "cities_attempted": sorted({s.city for s in sources}),
        "operators_attempted": sorted({s.operator for s in sources}),
        "properties_attempted": len({(s.city, s.operator, s.property) for s in sources}),
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
    }
    return summary


def run(city: Optional[str] = None, force_9am_gate: bool = True) -> Dict[str, Any]:
    return asyncio.run(run_pipeline(city=city, force_9am_gate=force_9am_gate))
