from __future__ import annotations

from typing import Any, Protocol


class DataSource(Protocol):
    """Resolve requests into API-equivalent structured Python envelopes."""

    def fetch(
        self,
        *,
        method: str,
        path: str,
        params: dict[str, Any],
        json: Any | None = None,
    ) -> Any:
        """Return a structured dictionary without HTTP/JSON serialization."""
        ...

    def fetch_df(
        self,
        *,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Return a bounded Pandas DataFrame for offline compatibility."""
        ...
