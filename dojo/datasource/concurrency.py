from __future__ import annotations

import math
import threading
from collections.abc import Callable
from typing import TypeVar

from dojo._exceptions import QueryConcurrencyLimitError

T = TypeVar("T")


class PolarsCollectLimiter:
    """Synchronously bound concurrent Polars collect operations.

    The limiter is deliberately based on ``threading.BoundedSemaphore`` so it
    protects callers from both synchronous SDK clients and AnyIO worker
    threads. A zero concurrency value disables limiting without changing the
    call path.
    """

    def __init__(
        self,
        *,
        max_concurrent: int,
        queue_timeout_seconds: float | None = None,
    ) -> None:
        if (
            not isinstance(max_concurrent, int)
            or isinstance(max_concurrent, bool)
            or max_concurrent < 0
        ):
            raise ValueError("max_concurrent must be a non-negative integer")
        if (
            queue_timeout_seconds is not None
            and (
                isinstance(queue_timeout_seconds, bool)
                or not math.isfinite(queue_timeout_seconds)
                or queue_timeout_seconds < 0
            )
        ):
            raise ValueError("queue_timeout_seconds must be non-negative or None")

        self.max_concurrent = int(max_concurrent)
        self.queue_timeout_seconds = (
            None if queue_timeout_seconds is None else float(queue_timeout_seconds)
        )
        self._semaphore = (
            threading.BoundedSemaphore(self.max_concurrent)
            if self.max_concurrent > 0
            else None
        )

    @property
    def enabled(self) -> bool:
        return self._semaphore is not None

    def run(self, operation: Callable[[], T]) -> T:
        """Run one collect operation after acquiring a bounded execution slot."""
        semaphore = self._semaphore
        if semaphore is None:
            return operation()

        if self.queue_timeout_seconds is None:
            acquired = semaphore.acquire()
        else:
            acquired = semaphore.acquire(timeout=self.queue_timeout_seconds)
        if not acquired:
            raise QueryConcurrencyLimitError(
                max_concurrent=self.max_concurrent,
                queue_timeout_seconds=self.queue_timeout_seconds,
            )

        try:
            return operation()
        finally:
            semaphore.release()


_process_limiter_guard = threading.Lock()
# Protect direct ``PolarsQueryEngine`` users even before an offline datasource
# applies its environment-backed configuration.
_process_limiter = PolarsCollectLimiter(max_concurrent=4)


def get_process_polars_collect_limiter() -> PolarsCollectLimiter:
    """Return the limiter shared by every SDK client in this process."""
    with _process_limiter_guard:
        return _process_limiter


def configure_process_polars_collect_limiter(
    *,
    max_concurrent: int,
    queue_timeout_seconds: float | None = None,
) -> PolarsCollectLimiter:
    """Configure the process limiter before serving queries.

    Reconfiguration replaces the process default for future calls. It does not
    alter operations already running through the previously returned limiter,
    so applications should call this once during process startup.
    """
    configured = PolarsCollectLimiter(
        max_concurrent=max_concurrent,
        queue_timeout_seconds=queue_timeout_seconds,
    )
    global _process_limiter
    with _process_limiter_guard:
        _process_limiter = configured
    return configured


def run_with_process_polars_collect_limiter(operation: Callable[[], T]) -> T:
    """Run an operation through the current process-wide limiter."""
    return get_process_polars_collect_limiter().run(operation)
