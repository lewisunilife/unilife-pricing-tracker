from pathlib import Path
from typing import Dict, List

import yaml


def _load() -> Dict[str, List[dict]]:
    config_dir = Path(__file__).resolve().parent / "config"
    with (config_dir / "cities.yaml").open("r", encoding="utf-8") as fh:
        cities = yaml.safe_load(fh) or {}

    out: Dict[str, List[dict]] = {}
    for city_entry in cities.get("cities", []):
        city_path = config_dir / city_entry["config"]
        with city_path.open("r", encoding="utf-8") as fh:
            payload = yaml.safe_load(fh) or {}
        city_name = payload.get("city", city_entry["name"])
        out.setdefault(city_name, [])
        for src in payload.get("sources", []):
            out[city_name].append(
                {
                    "operator": src["operator"],
                    "property": src["property"],
                    "url": src["primary_url"],
                    "scraper": src["parser"],
                    "secondary_urls": src.get("secondary_urls", []) or [],
                    "notes": src.get("notes", ""),
                }
            )
    return out


CITY_SOURCES = _load()
