"""Lightweight checkpoint persistence for resumable crawling."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from config.settings import settings


class CheckpointManager:
    """Persist crawler progress as JSON files under logs/checkpoints."""

    def __init__(self, base_dir: Optional[Path] = None):
        self.base_dir = base_dir or (settings.log_path / 'checkpoints')
        self.base_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _normalize_token(value: str) -> str:
        token = re.sub(r'[^0-9A-Za-z\-_.]+', '_', str(value).strip())
        return token.strip('._') or 'unknown'

    def _checkpoint_path(self, namespace: str, key: str) -> Path:
        safe_namespace = self._normalize_token(namespace)
        safe_key = self._normalize_token(key)
        return self.base_dir / f'{safe_namespace}_{safe_key}.json'

    def save(self, namespace: str, key: str, payload: dict[str, Any]) -> str:
        path = self._checkpoint_path(namespace, key)
        record = {
            'namespace': namespace,
            'key': key,
            'updated_at': datetime.now().isoformat(timespec='seconds'),
            'payload': payload,
        }
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding='utf-8')
        return str(path)

    def load(self, namespace: str, key: str) -> Optional[dict[str, Any]]:
        path = self._checkpoint_path(namespace, key)
        if not path.exists():
            return None
        try:
            raw = path.read_text(encoding='utf-8')
            if not raw.strip():
                return None
            data = json.loads(raw)
        except Exception:
            return None
        payload = data.get('payload')
        return payload if isinstance(payload, dict) else None

    def clear(self, namespace: str, key: str) -> None:
        path = self._checkpoint_path(namespace, key)
        try:
            if path.exists():
                path.unlink()
        except Exception:
            try:
                path.write_text("", encoding="utf-8")
            except Exception:
                pass

    def path_for(self, namespace: str, key: str) -> str:
        return str(self._checkpoint_path(namespace, key))


def looks_like_recoverable_error(exc: Exception | str) -> bool:
    """Heuristic for network/browser interruptions that are worth pausing and resuming."""
    text = str(exc or '').lower()
    markers = [
        'err_network_changed',
        'err_internet_disconnected',
        'err_connection_reset',
        'err_connection_closed',
        'err_connection_aborted',
        'err_name_not_resolved',
        'err_timed_out',
        'timeout',
        'timed out',
        'connection aborted',
        'connection reset',
        'connection closed',
        'connection refused',
        'network changed',
        'internet disconnected',
        'websocket',
        'ws://',
        'browser connection',
        'target closed',
        'net::err',
        'page crashed',
        'disconnected',
        'dns',
    ]
    return any(marker in text for marker in markers)
