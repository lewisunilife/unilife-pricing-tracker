from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class SourceRecord:
    city: str
    operator: str
    property: str
    parser: str
    primary_url: str
    secondary_urls: List[str] = field(default_factory=list)
    notes: str = ""

    @property
    def urls(self) -> List[str]:
        out = [self.primary_url]
        for url in self.secondary_urls:
            if url and url not in out:
                out.append(url)
        return out


@dataclass
class SourceAttempt:
    city: str
    operator: str
    property: str
    url: str
    stage: str
    method: str
    status: str
    reason: str
    rows: int = 0


@dataclass
class SnapshotContext:
    snapshot_id: str
    snapshot_date: str
    run_timestamp: str
    scrape_source: str


Row = Dict[str, Any]
