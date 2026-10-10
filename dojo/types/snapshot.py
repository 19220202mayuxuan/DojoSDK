from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict


class SnapshotFile(BaseModel):
    """One validated file in an immutable local snapshot."""

    model_config = ConfigDict(frozen=True)

    relative_path: str
    local_path: Path
    size: int


class SnapshotRef(BaseModel):
    """A complete, committed snapshot captured by a query."""

    model_config = ConfigDict(frozen=True)

    backend: str
    repo_id: str
    resolved_revision: str
    snapshot_token: str
    files: tuple[SnapshotFile, ...]


class SnapshotMetadata(BaseModel):
    """Immutable snapshot identity returned with a query result."""

    model_config = ConfigDict(frozen=True)

    backend: str
    repo_id: str
    resolved_revision: str
    snapshot_token: str

    @classmethod
    def from_ref(cls, snapshot: SnapshotRef) -> "SnapshotMetadata":
        return cls(
            backend=snapshot.backend,
            repo_id=snapshot.repo_id,
            resolved_revision=snapshot.resolved_revision,
            snapshot_token=snapshot.snapshot_token,
        )


class SnapshotSyncFailure(BaseModel):
    """A repository update that failed before atomic commit."""

    model_config = ConfigDict(frozen=True)

    repo_id: str
    revision: str | None = None
    reason: str


class SnapshotSyncResult(BaseModel):
    """Result of independently synchronizing one or more repositories."""

    model_config = ConfigDict(frozen=True)

    changed: bool
    updated_repos: tuple[str, ...]
    snapshots: tuple[SnapshotRef, ...]
    failures: tuple[SnapshotSyncFailure, ...]


class SnapshotGCResult(BaseModel):
    """Result of an explicit, conservative snapshot revision cleanup."""

    model_config = ConfigDict(frozen=True)

    dry_run: bool
    candidates: tuple[Path, ...]
    removed: tuple[Path, ...]
    retained: tuple[Path, ...]
    reclaimable_bytes: int
    reclaimed_bytes: int
