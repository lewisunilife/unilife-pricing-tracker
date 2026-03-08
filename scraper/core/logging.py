import json
from pathlib import Path
from typing import Any, Iterable


def snapshot_paths(repo_root: Path, snapshot_id: str) -> dict:
    folder_id = snapshot_id.replace(":", "-")
    base = repo_root / "data" / "snapshots" / folder_id
    raw = base / "raw"
    validated = base / "validated"
    logs = base / "logs"
    raw.mkdir(parents=True, exist_ok=True)
    validated.mkdir(parents=True, exist_ok=True)
    logs.mkdir(parents=True, exist_ok=True)
    return {"base": base, "raw": raw, "validated": validated, "logs": logs}


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def write_jsonl(path: Path, rows: Iterable[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")
