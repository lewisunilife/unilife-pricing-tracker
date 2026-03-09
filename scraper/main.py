import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scraper.core.pipeline import run  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PBSA pricing intelligence pipeline")
    parser.add_argument("--city", default=None, help="Optional city filter (e.g. Southampton)")
    parser.add_argument(
        "--ignore-9am-gate",
        action="store_true",
        help="Bypass London 09:00 gate for local/manual runs.",
    )
    parser.add_argument(
        "--summary-path",
        default=None,
        help="Optional path to write JSON run summary.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
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
