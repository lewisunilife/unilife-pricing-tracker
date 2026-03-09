import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scraper.core.pipeline import run, workbook_path  # noqa: E402
from scraper.core.workbook import migrate_workbook  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PBSA pricing intelligence pipeline (compat wrapper)")
    parser.add_argument("--city", default="Southampton", help="City filter")
    parser.add_argument(
        "--ignore-9am-gate",
        action="store_true",
        help="Bypass London 09:00 gate for local/manual runs.",
    )
    parser.add_argument("--summary-path", default=None, help="Optional path to write JSON run summary.")
    parser.add_argument("--clean-existing", action="store_true", help="Migrate workbook schema without scraping.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.clean_existing:
        result = migrate_workbook(workbook_path())
        print("Workbook migration complete")
        print(f"Before rows: {result['before']}")
        print(f"After rows: {result['after']}")
        raise SystemExit(0)

    summary = run(city=args.city, force_9am_gate=not args.ignore_9am_gate)
    print(json.dumps(summary, indent=2))
    if args.summary_path:
        path = Path(args.summary_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if summary.get("status") in {"ok", "no_rows", "skipped"}:
        raise SystemExit(0)
    raise SystemExit(0)


if __name__ == "__main__":
    main()
