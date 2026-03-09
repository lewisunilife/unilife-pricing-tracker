from __future__ import annotations

import datetime as dt
import shutil
from pathlib import Path


WORKBOOK_NAME = "Unilife_Pricing_Snapshot.xlsx"
BACKUP_PREFIX = "Unilife_Pricing_Snapshot_"
BACKUP_SUFFIX = ".xlsx"
KEEP_BACKUPS = 30


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def live_workbook_path() -> Path:
    return repo_root() / "data" / WORKBOOK_NAME


def backups_dir() -> Path:
    return repo_root() / "data" / "backups"


def backup_filename(day: dt.date) -> str:
    return f"{BACKUP_PREFIX}{day.isoformat()}{BACKUP_SUFFIX}"


def sorted_backups(path: Path) -> list[Path]:
    files = [p for p in path.glob(f"{BACKUP_PREFIX}*{BACKUP_SUFFIX}") if p.is_file()]
    return sorted(files, key=lambda p: p.name, reverse=True)


def main() -> int:
    live_path = live_workbook_path()
    backup_root = backups_dir()
    backup_root.mkdir(parents=True, exist_ok=True)

    if not live_path.exists():
        print(f"Live workbook not found: {live_path}")
        return 1

    today = dt.date.today()
    today_backup = backup_root / backup_filename(today)
    created = False

    if today_backup.exists():
        print(f"Backup skipped: {today_backup}")
        return 0

    shutil.copy2(live_path, today_backup)
    created = True
    print(f"Backup created: {today_backup}")

    deleted = 0
    for old in sorted_backups(backup_root)[KEEP_BACKUPS:]:
        old.unlink(missing_ok=True)
        deleted += 1
        print(f"Old backup deleted: {old}")

    if deleted == 0:
        print("Old backups deleted: none")

    print(f"Backup status: {'created' if created else 'skipped'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
