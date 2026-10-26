import httpx
from typing import Any


class DojoError(Exception):
    """Base exception for all DojoSDK errors."""

    pass


class APIError(DojoError):
    """Base class for errors related to interacting with the Dojo API."""

    pass


class APIConnectionError(APIError):
    """Raised when the SDK fails to connect to the Dojo API service."""

    def __init__(self, message: str = "Connection error.", *, request: httpx.Request | None = None) -> None:
        super().__init__(message)
        self.request = request


class APITimeoutError(APIConnectionError):
    """Raised when a request to the Dojo API times out."""

    def __init__(self, message: str = "Request timed out.", *, request: httpx.Request | None = None) -> None:
        super().__init__(message, request=request)


class APIResponseValidationError(APIError):
    """Raised when the API response does not conform to the expected Pydantic schema."""

    def __init__(self, response: httpx.Response, body: Any, message: str = "API response did not match expected schema.") -> None:
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code
        self.body = body


class APIStatusError(APIError):
    """Raised when the Dojo API returns a non-2xx status code."""

    def __init__(self, message: str, *, response: httpx.Response, body: Any) -> None:
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code
        self.body = body


class BadRequestError(APIStatusError):
    """HTTP 400 Bad Request."""

    pass


class AuthenticationError(APIStatusError):
    """HTTP 401 Unauthorized."""

    pass


class PermissionDeniedError(APIStatusError):
    """HTTP 403 Forbidden."""

    pass


class NotFoundError(APIStatusError):
    """HTTP 404 Not Found."""

    pass


class ConflictError(APIStatusError):
    """HTTP 409 Conflict."""

    pass


class UnprocessableEntityError(APIStatusError):
    """HTTP 422 Unprocessable Entity."""

    pass


class RateLimitError(APIStatusError):
    """HTTP 429 Rate Limit Exceeded."""

    pass


class InternalServerError(APIStatusError):
    """HTTP 500+ Internal Server Error."""

    pass


class OfflineDataNotAvailableError(DojoError):
    """Offline data is not available for the requested endpoint or parameters."""

    pass


class MaterializationLimitError(DojoError):
    """A lazy offline query would materialize more rows than its safety limit."""

    def __init__(self, *, limit: int, observed_at_least: int) -> None:
        super().__init__(
            f"Offline query exceeds max_materialized_rows={limit}; "
            f"observed at least {observed_at_least} rows. Add symbol/date filters, "
            "use a smaller finite limit, or split the query into bounded windows; "
            "fetch_df() and limit_per_symbol=None do not bypass this safety limit."
        )
        self.limit = limit
        self.observed_at_least = observed_at_least


class QueryConcurrencyLimitError(DojoError):
    """A Polars query could not enter the bounded collect execution slot."""

    def __init__(
        self,
        *,
        max_concurrent: int,
        queue_timeout_seconds: float,
    ) -> None:
        super().__init__(
            "Polars query queue wait exceeded "
            f"queue_timeout_seconds={queue_timeout_seconds}; "
            f"max_concurrent={max_concurrent}. Retry the query later."
        )
        self.max_concurrent = max_concurrent
        self.queue_timeout_seconds = queue_timeout_seconds


class SnapshotLockTimeoutError(DojoError):
    """A snapshot repository lock was not acquired before its deadline.

    Raised instead of blocking forever when another process holds the
    repository lock without making progress (for example a stalled snapshot
    sync). Previously this wait was unbounded, so a single stuck peer could
    freeze every reader in every process.
    """

    def __init__(self, *, path: str, timeout_seconds: float) -> None:
        super().__init__(
            f"Waited {timeout_seconds:g}s for the snapshot repository lock at "
            f"{path} without acquiring it. Another process is publishing, "
            "leasing, or garbage-collecting this repository and appears stuck. "
            "Check for a hung `sync_offline_snapshots` / `gc_offline_snapshots` "
            "process, raise DOJO_SNAPSHOT_LOCK_TIMEOUT_SECONDS, or set it to 0 "
            "to wait indefinitely."
        )
        self.path = path
        self.timeout_seconds = timeout_seconds
