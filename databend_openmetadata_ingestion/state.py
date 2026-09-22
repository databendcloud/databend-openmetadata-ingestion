from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


class Watermark:
    def __init__(self, path: str | Path):
        self._path = Path(path)

    def read(self) -> datetime | None:
        if not self._path.exists():
            return None
        raw = json.loads(self._path.read_text())
        value = raw.get("updated_on")
        return datetime.fromisoformat(value) if value else None

    def write(self, updated_on: datetime, extra: dict | None = None) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"updated_on": updated_on.isoformat(), **(extra or {})}
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(self._path)
