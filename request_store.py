"""Small lock-free request ledger using one atomically-written JSON file per event."""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RequestStore:
    """Persist dashboard provenance without SQLite/NFS locking."""

    def __init__(self, root: Path | None = None) -> None:
        configured = os.environ.get("SPUR_DASHBOARD_STATE_DIR")
        self.root = root or Path(configured or (Path.home() / ".spur-dashboard"))
        self.requests = self.root / "requests"
        self.events = self.root / "events"
        self._lock = threading.Lock()

    def _write(self, directory: Path, payload: dict[str, Any]) -> dict[str, Any]:
        directory.mkdir(parents=True, exist_ok=True)
        record = dict(payload)
        record.setdefault("id", uuid.uuid4().hex)
        record.setdefault("recordedAt", utc_now())
        destination = directory / f"{record['recordedAt'].replace(':', '')}-{record['id']}.json"
        temporary = destination.with_suffix(f".{os.getpid()}.tmp")
        encoded = json.dumps(record, sort_keys=True, indent=2)
        with self._lock:
            temporary.write_text(encoded + "\n", encoding="utf-8")
            os.replace(temporary, destination)
        return record

    def record_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._write(self.requests, payload)

    def record_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._write(self.events, payload)

    @staticmethod
    def _read_directory(directory: Path, limit: int) -> list[dict[str, Any]]:
        if not directory.exists():
            return []
        records: list[dict[str, Any]] = []
        for path in sorted(directory.glob("*.json"), reverse=True):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                records.append(payload)
            if len(records) >= limit:
                break
        return records

    def list_requests(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._read_directory(self.requests, max(1, min(limit, 500)))

    def list_events(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._read_directory(self.events, max(1, min(limit, 500)))
