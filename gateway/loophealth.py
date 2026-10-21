"""Event-loop liveness — /healthz must prove the loop ticks, not just
that the process exists (a wedged loop serves nothing but still has a
PID). A background beat updates a monotonic timestamp; healthz reports
the lag. Shared by both planes (the mtgateway image ships gateway/).
"""

from __future__ import annotations

import asyncio
import time


class LoopBeat:
    """1s heartbeat task; `lag_s` is how long since the last tick."""

    def __init__(self) -> None:
        self.last = time.monotonic()
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():

            async def _beat() -> None:
                while True:
                    await asyncio.sleep(1.0)
                    self.last = time.monotonic()

            self._task = asyncio.create_task(_beat(), name="loop-beat")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    @property
    def lag_s(self) -> float:
        return time.monotonic() - self.last

    @property
    def alive(self) -> bool:
        return self.lag_s < 5.0
