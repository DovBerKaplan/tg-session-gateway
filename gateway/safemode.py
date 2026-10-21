"""Crash-loop safe mode — the last guard before an AUTH_KEY_DUPLICATED
storm.

A supervisor restarting a crash-looping gateway connects every session
on every boot; overlapping connects are exactly what the singleton
lock exists to prevent, and hammering Telegram through them makes
auth floods worse. When the process boots more than MAX_BOOTS times
within WINDOW_S, the next boot enters SAFE MODE: sessions stay on
disk, nothing auto-connects, and an admin must acknowledge via the
API before traffic resumes.

Boot history lives in a plain JSON file next to the data (not in
SQLite): if we're crash-looping, the DB may be part of the problem.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field


@dataclass
class RestartTracker:
    path: str
    window_s: float = 600.0
    max_boots: int = 5
    _boots: list = field(default_factory=list)

    def record_boot(self) -> tuple[bool, int]:
        """Log this boot; returns (safe_mode, boots_in_window)."""
        now = time.time()
        try:
            with open(self.path) as f:
                boots = json.load(f)
        except (OSError, ValueError):
            boots = []
        boots = [b for b in boots if now - b < self.window_s]
        boots.append(now)
        try:
            with open(self.path, "w") as f:
                json.dump(boots[-50:], f)
        except OSError:
            pass  # read-only data dir is a separate, visible failure
        self._boots = boots
        return (len(boots) > self.max_boots, len(boots))

    def clear(self) -> None:
        """Admin acknowledged — history resets, the next crash loop
        needs a fresh MAX_BOOTS boots to re-trigger."""
        self._boots = []
        try:
            os.unlink(self.path)
        except OSError:
            pass
