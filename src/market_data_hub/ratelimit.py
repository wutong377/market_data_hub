"""按接口滑动窗口限速。"""

from __future__ import annotations

from collections import deque
import logging
import time
from typing import Callable

logger = logging.getLogger(__name__)


class SlidingWindowLimiter:
    def __init__(
        self,
        name: str,
        limit: int,
        window_seconds: float = 30.0,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        reserved: int = 0,
    ):
        if limit <= 0 or window_seconds <= 0:
            raise ValueError("限速参数必须为正数")
        self.name = name
        self.limit = int(limit)
        self.reserved = max(0, min(int(reserved), self.limit - 1))
        self.window_seconds = float(window_seconds)
        self.clock = clock
        self.sleeper = sleeper
        self._events: deque[float] = deque()
        self.total_calls = 0
        self.total_wait_seconds = 0.0

    def acquire(self, *, priority: str = "normal") -> float:
        waited = 0.0
        allowed = self.limit if priority == "high" else self.limit - self.reserved
        while True:
            now = self.clock()
            while self._events and self._events[0] <= now - self.window_seconds:
                self._events.popleft()
            if len(self._events) < allowed:
                self._events.append(now)
                self.total_calls += 1
                self.total_wait_seconds += waited
                if waited:
                    logger.info(
                        "接口限速等待完成: api=%s waited_seconds=%.3f calls=%s limit=%s",
                        self.name, waited, len(self._events), allowed,
                    )
                return waited
            delay = max(0.01, self._events[0] + self.window_seconds - now)
            self.sleeper(delay)
            waited += delay

    def stats(self) -> dict[str, float | int]:
        return {
            "calls": self.total_calls,
            "wait_seconds": round(self.total_wait_seconds, 3),
            "limit": self.limit,
            "reserved": self.reserved,
        }


class RateLimiterSet:
    def __init__(
        self,
        market_snapshot_limit: int = 50,
        option_chain_limit: int = 8,
        market_snapshot_reserved: int = 8,
    ):
        self.market_snapshot = SlidingWindowLimiter(
            "get_market_snapshot", market_snapshot_limit, reserved=market_snapshot_reserved,
        )
        self.option_chain = SlidingWindowLimiter("get_option_chain", option_chain_limit)

    def stats(self) -> dict[str, dict[str, float | int]]:
        return {
            "get_market_snapshot": self.market_snapshot.stats(),
            "get_option_chain": self.option_chain.stats(),
        }
