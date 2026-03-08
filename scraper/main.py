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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run(city=args.city, force_9am_gate=not args.ignore_9am_gate)
    print(json.dumps(summary, indent=2))
    if summary.get("status") in {"no_rows"}:
        raise SystemExit(1)
    raise SystemExit(0)


if __name__ == "__main__":
    main()
