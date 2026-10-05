"""测试用可控时钟。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .store import to_iso


class ManualClock:
    def __init__(self, start: datetime | None = None) -> None:
        self.current = start or datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self.current

    def advance(self, *, seconds: float = 0, **kwargs: float) -> None:
        if seconds:
            self.current += timedelta(seconds=seconds)
        if kwargs:
            self.current += timedelta(**kwargs)

    def iso(self) -> str:
        return to_iso(self.current)
