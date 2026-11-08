"""
    Desc: Circuit breaker and retry with exponential backoff for unreliable external API(s)
    Creator: Kirosha
"""

import asyncio
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")

class BreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitOpenError(Exception):
    def __init__(self, name: str):
        super().__init__(f'breaker "{name}" is open')

class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 3,
        open_for: timedelta = timedelta(seconds=30),
        clock: Callable[[], datetime] | None = None,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.open_for = open_for
        self.__clock = clock or (lambda: datetime.now(timezone.utc))

        self.state = BreakerState.CLOSED
        self.__failures = 0
        self.__opened_at: datetime | None = None

    async def call(self, action: Callable[[], Awaitable[T]]) -> T:
        if self.state == BreakerState.OPEN:
            # after open_for one probe call is let through (half-open)
            if self.__opened_at and self.__clock() - self.__opened_at >= self.open_for:
                self.state = BreakerState.HALF_OPEN
            else:
                raise CircuitOpenError(self.name)

        try:
            result = await action()
        except Exception:
            self.__failures += 1
            # a failed probe reopens the breaker at once
            if self.state == BreakerState.HALF_OPEN or self.__failures >= self.failure_threshold:
                self.state = BreakerState.OPEN
                self.__opened_at = self.__clock()
            raise

        self.__failures = 0
        self.state = BreakerState.CLOSED
        return result

async def retry_with_backoff(
    action: Callable[[], Awaitable[T]],
    max_attempts: int = 3,
    base_delay: float = 0.005,
    retry_if: Callable[[Exception], bool] | None = None,
) -> T:
    """
    Retries `action` with backoff base, 2*base, 4*base...
    `retry_if` lets the caller stop at once for errors that make no sense to retry.
    """
    attempt = 0

    while True:
        attempt += 1
        try:
            return await action()
        except Exception as err:
            retryable = True if retry_if is None else retry_if(err)
            if attempt >= max_attempts or not retryable: raise
            await asyncio.sleep(base_delay * (2 ** (attempt - 1)))
