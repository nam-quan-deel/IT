"""Lightweight JSON-backed persistence for channel metadata and sync tokens."""

from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional


class StateStore:
    """Persist watcher channels and sync tokens on disk."""

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.channels_file = self.base_dir / "channels.json"
        self.sync_file = self.base_dir / "sync_tokens.json"
        self._lock = Lock()

    def get_sync_token(self, calendar_id: str) -> Optional[str]:
        sync_tokens = self._read_json(self.sync_file)
        return sync_tokens.get(calendar_id)

    def set_sync_token(self, calendar_id: str, token: str) -> None:
        with self._lock:
            sync_tokens = self._read_json(self.sync_file)
            sync_tokens[calendar_id] = token
            self._write_json(self.sync_file, sync_tokens)

    def save_channel(self, calendar_id: str, channel_data: Dict[str, Any]) -> None:
        with self._lock:
            channels = self._read_json(self.channels_file)
            channels[calendar_id] = channel_data
            self._write_json(self.channels_file, channels)

    def get_channels(self) -> Dict[str, Dict[str, Any]]:
        return self._read_json(self.channels_file)

    def _read_json(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _write_json(self, path: Path, data: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
        tmp_path.replace(path)

