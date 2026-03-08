from dataclasses import asdict
from typing import Dict, List

from .models import SourceAttempt, SourceRecord


class CoverageTracker:
    def __init__(self) -> None:
        self.attempts: List[SourceAttempt] = []

    def add(
        self,
        source: SourceRecord,
        url: str,
        stage: str,
        method: str,
        status: str,
        reason: str,
        rows: int = 0,
    ) -> None:
        self.attempts.append(
            SourceAttempt(
                city=source.city,
                operator=source.operator,
                property=source.property,
                url=url,
                stage=stage,
                method=method,
                status=status,
                reason=reason,
                rows=rows,
            )
        )

    def as_dicts(self) -> List[Dict]:
        return [asdict(x) for x in self.attempts]

    def property_summary(self) -> List[Dict]:
        by_property: Dict[tuple, List[SourceAttempt]] = {}
        for attempt in self.attempts:
            key = (attempt.city, attempt.operator, attempt.property)
            by_property.setdefault(key, []).append(attempt)

        out: List[Dict] = []
        for (city, operator, prop), attempts in by_property.items():
            success_rows = sum(x.rows for x in attempts if x.status == "success")
            statuses = [x.status for x in attempts]
            if success_rows > 0:
                final = "scraped successfully with rows"
            elif all(s == "blocked" for s in statuses):
                final = "fully blocked"
            elif any(s == "blocked" for s in statuses):
                final = "partially blocked; no rows"
            else:
                final = "no rows from checked pages"

            reasons = []
            for attempt in attempts:
                reasons.append(f"{attempt.url} => {attempt.status} ({attempt.reason})")
            out.append(
                {
                    "city": city,
                    "operator": operator,
                    "property": prop,
                    "status": final,
                    "rows": success_rows,
                    "reasons": reasons,
                }
            )
        return out
