from __future__ import annotations

import hashlib
import fnmatch
import json
import os
import shutil
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Hashable, Iterator, Sequence

import pyarrow.parquet as pq

from dojo._exceptions import OfflineDataNotAvailableError, SnapshotLockTimeoutError
from dojo.datasource.config import HFConfig
from dojo.datasource.network import resolve_backend
from dojo.datasource.registry import HFEndpointSpec, HF_REGISTRY
from dojo.logging import logger
from dojo.types.snapshot import SnapshotFile, SnapshotGCResult, SnapshotRef, SnapshotSyncFailure, SnapshotSyncResult

SNAPSHOT_LAYOUT_VERSION = 1
STAGING_LEASE_FILENAME = ".lease.json"
REVISION_LEASE_DIRECTORY = ".leases"


@dataclass
class _LockEntry:
    lock: threading.Lock
    users: int = 0


class KeyedLockPool:
    """Reference-counted keyed locks that remain alive while callers wait."""

    def __init__(self) -> None:
        self._entries: dict[Hashable, _LockEntry] = {}
        self._guard = threading.Lock()

    @contextmanager
    def hold(self, key: Hashable) -> Iterator[None]:
        with self._guard:
            entry = self._entries.get(key)
            if entry is None:
                entry = _LockEntry(lock=threading.Lock())
                self._entries[key] = entry
            entry.users += 1

        entry.lock.acquire()
        try:
            yield
        finally:
            entry.lock.release()
            with self._guard:
                entry.users -= 1
                if entry.users == 0:
                    self._entries.pop(key, None)

    def __len__(self) -> int:
        with self._guard:
            return len(self._entries)


@dataclass(frozen=True)
class _RepoRequest:
    backend: str
    canonical_repo_id: str
    provider_repo_id: str
    repo_type: str
    files: tuple[str, ...]
    partition_globs: tuple[str, ...] = ()
    partition_manifests: tuple[str, ...] = ()
    partition_fallbacks: tuple[str, ...] = ()


@dataclass(frozen=True)
class _ResolvedRevision:
    identifier: str
    download_revision: str
    verify_remote_after_download: bool = False
    derive_from_content: bool = False
    files: tuple[str, ...] | None = None


@dataclass(frozen=True)
class _ActiveSnapshotCacheEntry:
    pointer_signature: tuple[int, int, int, int]
    snapshot: SnapshotRef


def snapshot_token(
    *,
    backend: str,
    repo_id: str,
    resolved_revision: str,
    layout_version: int = SNAPSHOT_LAYOUT_VERSION,
) -> str:
    material = "\0".join((backend, repo_id, resolved_revision, str(layout_version)))
    return f"snapshot-v{layout_version}-{hashlib.sha256(material.encode('utf-8')).hexdigest()}"


def combine_snapshot_tokens(snapshots: Sequence[SnapshotRef]) -> str:
    """Build a stable token from exactly the dependency snapshots used."""
    tokens = sorted(snapshot.snapshot_token for snapshot in snapshots)
    material = "\0".join(tokens)
    return f"snapshot-set-{hashlib.sha256(material.encode('utf-8')).hexdigest()}"


class SnapshotResolver:
    """Resolve, validate, and atomically publish immutable Parquet snapshots."""

    def __init__(self, config: HFConfig | None = None, *, path_cache_size: int = 512) -> None:
        self._cfg = config or HFConfig.from_env()
        self._root = Path(self._cfg.cache_dir).expanduser().resolve() / "dojosdk" / "snapshots"
        self._path_cache_size = max(1, int(path_cache_size))
        self._path_cache: OrderedDict[tuple[str, str, str, str], Path] = OrderedDict()
        self._path_cache_guard = threading.Lock()
        self._active_cache: OrderedDict[tuple[str, str], _ActiveSnapshotCacheEntry] = OrderedDict()
        self._active_cache_guard = threading.Lock()
        self._locks = KeyedLockPool()

    @property
    def snapshot_root(self) -> Path:
        return self._root

    def ensure_offline_snapshots(
        self,
        *,
        paths: list[str] | None = None,
        repos: list[str] | None = None,
    ) -> list[SnapshotRef]:
        requests = self._select_requests(paths=paths, repos=repos)
        snapshots: list[SnapshotRef] = []
        missing: list[str] = []

        for request in requests:
            active = self._load_active(request)
            if active is None or not self._snapshot_satisfies_request(active, request):
                missing.append(request.canonical_repo_id)
            else:
                snapshots.append(active)

        if missing:
            result = self.sync_offline_snapshots(
                repos=missing,
                check_remote=not self._cfg.local_only,
                force=False,
            )
            by_canonical = {
                request.canonical_repo_id: snapshot
                for request in requests
                for snapshot in result.snapshots
                if snapshot.backend == request.backend and snapshot.repo_id == request.provider_repo_id
            }
            snapshots = []
            unresolved: list[str] = []
            for request in requests:
                active = self._load_active(request) or by_canonical.get(request.canonical_repo_id)
                if active is None:
                    unresolved.append(request.canonical_repo_id)
                else:
                    snapshots.append(active)
            if unresolved:
                reasons = "; ".join(f"{failure.repo_id}: {failure.reason}" for failure in result.failures)
                raise OfflineDataNotAvailableError(f"Unable to ensure offline snapshots for {', '.join(unresolved)}" + (f": {reasons}" if reasons else ""))

        return snapshots

    def sync_offline_snapshots(
        self,
        *,
        paths: list[str] | None = None,
        repos: list[str] | None = None,
        check_remote: bool = True,
        force: bool = False,
    ) -> SnapshotSyncResult:
        requests = self._select_requests(paths=paths, repos=repos)
        snapshots: list[SnapshotRef] = []
        failures: list[SnapshotSyncFailure] = []
        updated_repos: list[str] = []

        for request in requests:
            revision: str | None = None
            repo_lock_key = ("repo", request.backend, request.provider_repo_id)
            with self._locks.hold(repo_lock_key):
                with self._repo_process_lock(self._repo_root(request)):
                    self._cleanup_staging(self._repo_root(request) / ".staging")
                    previous = self._load_active(request)
                    try:
                        resolved = self._resolve_revision(
                            request,
                            check_remote=check_remote,
                            previous=previous,
                        )
                        if isinstance(resolved, str):
                            resolved = _ResolvedRevision(resolved, resolved)
                        revision = resolved.identifier
                        if previous is not None and previous.resolved_revision == revision and self._snapshot_satisfies_request(previous, request) and not force:
                            snapshots.append(previous)
                            continue

                        committed = self._stage_and_commit(
                            request,
                            resolved=resolved,
                            previous=previous,
                            force=force,
                        )
                        snapshots.append(committed)
                        if previous is None or previous != committed or force:
                            updated_repos.append(request.provider_repo_id)
                    except Exception as error:
                        logger.warning(
                            "Snapshot sync failed: backend=%s repo=%s revision=%s error=%s",
                            request.backend,
                            request.provider_repo_id,
                            revision,
                            error,
                        )
                        failures.append(
                            SnapshotSyncFailure(
                                repo_id=request.provider_repo_id,
                                revision=revision,
                                reason=str(error),
                            )
                        )
                        if previous is not None:
                            snapshots.append(previous)

        return SnapshotSyncResult(
            changed=bool(updated_repos),
            updated_repos=tuple(updated_repos),
            snapshots=tuple(snapshots),
            failures=tuple(failures),
        )

    def get_active_snapshot(self, repo_id: str) -> SnapshotRef | None:
        requests = self._select_requests(paths=None, repos=[repo_id])
        return self._load_active(requests[0])

    def gc_offline_snapshots(
        self,
        *,
        paths: list[str] | None = None,
        repos: list[str] | None = None,
        min_age_seconds: float = 7 * 24 * 60 * 60,
        keep_previous: int = 1,
        dry_run: bool = True,
    ) -> SnapshotGCResult:
        """Conservatively remove old SDK-owned revisions selected explicitly.

        Cleanup is never automatic. The active revision, the requested number
        of most-recent previous revisions, recent revisions, and revisions with
        live query leases are always retained.
        """
        if min_age_seconds < 0:
            raise ValueError("min_age_seconds must be non-negative")
        if keep_previous < 0:
            raise ValueError("keep_previous must be non-negative")

        candidates: list[Path] = []
        removed: list[Path] = []
        retained: list[Path] = []
        reclaimable_bytes = 0
        reclaimed_bytes = 0
        cutoff = time.time() - min_age_seconds

        for request in self._select_requests(paths=paths, repos=repos):
            repo_root = self._repo_root(request)
            lock_key = ("repo", request.backend, request.provider_repo_id)
            with self._locks.hold(lock_key), self._repo_process_lock(repo_root):
                active = self._load_active(request)
                active_dir = self._snapshot_revision_dir(active) if active is not None else None
                revisions_root = repo_root / "revisions"
                try:
                    revisions = sorted(
                        (path for path in revisions_root.iterdir() if path.is_dir()),
                        key=lambda path: path.stat().st_mtime_ns,
                        reverse=True,
                    )
                except OSError:
                    continue

                previous = [path for path in revisions if path != active_dir][:keep_previous]
                protected = ({active_dir} if active_dir is not None else set()) | set(previous)

                for revision_dir in revisions:
                    try:
                        modified = revision_dir.stat().st_mtime
                    except OSError:
                        retained.append(revision_dir)
                        continue
                    if revision_dir in protected or modified > cutoff or self._revision_has_leases(revision_dir):
                        retained.append(revision_dir)
                        continue
                    size = self._directory_size(revision_dir)
                    candidates.append(revision_dir)
                    reclaimable_bytes += size
                    if not dry_run:
                        shutil.rmtree(revision_dir)
                        removed.append(revision_dir)
                        reclaimed_bytes += size

        return SnapshotGCResult(
            dry_run=dry_run,
            candidates=tuple(candidates),
            removed=tuple(removed),
            retained=tuple(retained),
            reclaimable_bytes=reclaimable_bytes,
            reclaimed_bytes=reclaimed_bytes,
        )

    @contextmanager
    def lease_snapshot(self, snapshot: SnapshotRef) -> Iterator[None]:
        """Keep a captured revision safe from explicit GC for a query lifetime."""
        revision_dir = self._snapshot_revision_dir(snapshot)
        if revision_dir is None:
            yield
            return
        repo_root = revision_dir.parent.parent
        lease_dir = revision_dir / REVISION_LEASE_DIRECTORY
        lease = lease_dir / f"{os.getpid()}-{uuid.uuid4().hex}.lease"
        with self._repo_process_lock(repo_root):
            if not revision_dir.is_dir():
                raise OfflineDataNotAvailableError(f"Snapshot revision disappeared before query: {revision_dir}")
            lease_dir.mkdir(parents=True, exist_ok=True)
            lease.write_text(str(time.time()), encoding="ascii")
        try:
            yield
        finally:
            lease.unlink(missing_ok=True)

    def _select_requests(
        self,
        *,
        paths: list[str] | None,
        repos: list[str] | None,
    ) -> list[_RepoRequest]:
        if not paths and not repos:
            raise ValueError("Explicit non-empty paths or repos are required; syncing the entire registry is not allowed.")

        selected_specs: list[HFEndpointSpec] = []
        if paths:
            for path in paths:
                spec = HF_REGISTRY.get(path)
                if spec is None:
                    raise ValueError(f"Unknown offline endpoint path: {path}")
                selected_specs.append(spec)

        if repos:
            for repo_id in repos:
                matches = [spec for spec in HF_REGISTRY.values() if repo_id in {spec.repo_id, spec.ms_repo_id}]
                if not matches:
                    raise ValueError(f"Unknown offline repository: {repo_id}")
                selected_specs.extend(matches)

        backend = resolve_backend(self._cfg)
        grouped: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
        for spec in selected_specs:
            provider_repo_id = spec.ms_repo_id if backend == "modelscope" else spec.repo_id
            if not provider_repo_id:
                raise ValueError(f"No {backend} repository configured for {spec.repo_id}")
            key = (spec.repo_id, provider_repo_id)
            group = grouped.setdefault(
                key,
                {
                    "repo_type": spec.repo_type,
                    "files": OrderedDict(),
                    "partition_globs": OrderedDict(),
                    "partition_manifests": OrderedDict(),
                    "partition_fallbacks": OrderedDict(),
                },
            )
            concrete_path = self._validated_relative_path(spec.path_template)
            group["files"].setdefault(concrete_path, None)
            if spec.partition_glob and spec.partition_manifest:
                group["partition_globs"].setdefault(self._validated_glob(spec.partition_glob), None)
                group["partition_manifests"].setdefault(self._validated_relative_path(spec.partition_manifest), None)
                group["partition_fallbacks"].setdefault(concrete_path, None)
            for companion in spec.companion_files:
                group["files"].setdefault(self._validated_relative_path(companion), None)

        return [
            _RepoRequest(
                backend=backend,
                canonical_repo_id=canonical_repo_id,
                provider_repo_id=provider_repo_id,
                repo_type=values["repo_type"],
                files=tuple(values["files"]),
                partition_globs=tuple(values["partition_globs"]),
                partition_manifests=tuple(values["partition_manifests"]),
                partition_fallbacks=tuple(values["partition_fallbacks"]),
            )
            for (canonical_repo_id, provider_repo_id), values in grouped.items()
        ]

    @staticmethod
    def _validated_relative_path(value: str) -> str:
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts or "{" in value or "}" in value:
            raise ValueError(f"Snapshot file must be a concrete safe relative path: {value!r}")
        return path.as_posix()

    @staticmethod
    def _validated_glob(value: str) -> str:
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts or "{" in value or "}" in value:
            raise ValueError(f"Snapshot glob must be a safe relative pattern: {value!r}")
        return path.as_posix()

    @staticmethod
    def _resolved_request_files(
        request: _RepoRequest,
        available_files: Sequence[str],
    ) -> tuple[str, ...]:
        available = tuple(dict.fromkeys(PurePosixPath(path).as_posix() for path in available_files))
        partition_files = tuple(path for path in available if any(fnmatch.fnmatchcase(path, pattern) for pattern in request.partition_globs))
        manifests_available = bool(request.partition_manifests) and all(manifest in available for manifest in request.partition_manifests)
        if partition_files and manifests_available:
            exact = [path for path in request.files if path not in request.partition_fallbacks]
            return tuple(dict.fromkeys(exact + list(request.partition_manifests) + list(partition_files)))
        return request.files

    def _resolve_revision(
        self,
        request: _RepoRequest,
        *,
        check_remote: bool,
        previous: SnapshotRef | None,
    ) -> _ResolvedRevision:
        if not check_remote:
            requested = self._provider_requested_revision(request)
            return _ResolvedRevision(
                identifier=previous.resolved_revision if previous is not None else requested,
                download_revision=requested,
                derive_from_content=True,
                files=(tuple(file.relative_path for file in previous.files) if previous is not None else request.files),
            )
        if self._cfg.local_only:
            requested = self._provider_requested_revision(request)
            return _ResolvedRevision(
                identifier=previous.resolved_revision if previous is not None else requested,
                download_revision=requested,
                derive_from_content=True,
                files=(tuple(file.relative_path for file in previous.files) if previous is not None else request.files),
            )

        if request.backend == "modelscope":
            return self._resolve_modelscope_revision(request)

        from huggingface_hub import HfApi

        info = HfApi(token=self._cfg.token).repo_info(
            repo_id=request.provider_repo_id,
            repo_type=request.repo_type,
            revision=self._cfg.revision,
        )
        resolved = getattr(info, "sha", None)
        if not resolved:
            raise RuntimeError(f"Hugging Face did not return a resolved revision for {request.provider_repo_id}")
        siblings = getattr(info, "siblings", ()) or ()
        available = [str(getattr(item, "rfilename", None) or getattr(item, "path", None) or "") for item in siblings]
        selected_files = self._resolved_request_files(
            request,
            [path for path in available if path],
        )
        return _ResolvedRevision(str(resolved), str(resolved), files=selected_files)

    def _resolve_modelscope_revision(self, request: _RepoRequest) -> _ResolvedRevision:
        from modelscope.hub.api import HubApi

        api = HubApi()
        requested = self._provider_requested_revision(request)
        entries: dict[str, dict[str, Any]] = {}
        page_number = 1
        page_size = 1000
        while True:
            page = api.get_dataset_files(
                request.provider_repo_id,
                revision=requested,
                page_number=page_number,
                page_size=page_size,
                token=self._cfg.modelscope_token,
            )
            for item in page:
                path = item.get("Path")
                if path:
                    entries[str(path)] = item
            if len(page) < page_size:
                break
            page_number += 1

        selected_files = self._resolved_request_files(request, tuple(entries))
        missing = sorted(set(selected_files) - entries.keys())
        if missing:
            raise FileNotFoundError(f"ModelScope repository {request.provider_repo_id}@{requested} is missing: {missing}")
        manifest = [
            {
                "path": relative_path,
                "revision": entries[relative_path].get("Revision"),
                "sha256": entries[relative_path].get("Sha256"),
                "size": entries[relative_path].get("Size"),
            }
            for relative_path in sorted(selected_files)
        ]
        encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        identifier = f"modelscope-manifest-{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"
        return _ResolvedRevision(
            identifier=identifier,
            download_revision=requested,
            verify_remote_after_download=True,
            files=selected_files,
        )

    def _provider_requested_revision(self, request: _RepoRequest) -> str:
        if request.backend == "modelscope" and self._cfg.revision == "main":
            return "master"
        return self._cfg.revision

    def _stage_and_commit(
        self,
        request: _RepoRequest,
        *,
        resolved: _ResolvedRevision,
        previous: SnapshotRef | None,
        force: bool,
    ) -> SnapshotRef:
        revision = resolved.identifier
        repo_root = self._repo_root(request)
        staging_root = repo_root / ".staging"
        revisions_root = repo_root / "revisions"
        staging_root.mkdir(parents=True, exist_ok=True)
        revisions_root.mkdir(parents=True, exist_ok=True)
        stage = staging_root / uuid.uuid4().hex
        stage.mkdir()
        self._write_staging_lease(stage)

        try:
            staged_by_path: dict[str, tuple[str, Path, int]] = {}
            resolved_revisions: set[str] = set()
            selected_files = resolved.files or request.files
            manifest_files = [path for path in selected_files if path in request.partition_manifests]
            remaining_files = [path for path in selected_files if path not in manifest_files]

            # The new manifest is intentionally fetched first. It is the
            # immutable content map used to decide whether an old bucket can
            # be reused without downloading its replacement.
            for relative_path in manifest_files:
                source = self._download_file(
                    request,
                    revision=resolved.download_revision,
                    cache_revision=resolved.identifier,
                    relative_path=relative_path,
                    force=force,
                )
                provider_revision = self._revision_from_provider_path(source)
                if provider_revision:
                    resolved_revisions.add(provider_revision)
                destination = stage.joinpath(*PurePosixPath(relative_path).parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                self._link_or_copy(source, destination)
                size = self._validate_snapshot_file(destination)
                staged_by_path[relative_path] = (relative_path, destination, size)

            reusable = (
                {}
                if force or previous is None
                else self._reusable_partition_files(
                    stage,
                    request=request,
                    selected_files=selected_files,
                    previous=previous,
                )
            )
            for relative_path in remaining_files:
                destination = stage.joinpath(*PurePosixPath(relative_path).parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                reused_source = reusable.get(relative_path)
                if reused_source is not None:
                    self._link_or_copy(reused_source, destination)
                else:
                    source = self._download_file(
                        request,
                        revision=resolved.download_revision,
                        cache_revision=resolved.identifier,
                        relative_path=relative_path,
                        force=force,
                    )
                    provider_revision = self._revision_from_provider_path(source)
                    if provider_revision:
                        resolved_revisions.add(provider_revision)
                    self._link_or_copy(source, destination)
                size = self._validate_snapshot_file(destination)
                staged_by_path[relative_path] = (relative_path, destination, size)

            staged_files = [staged_by_path[path] for path in selected_files]

            self._validate_partition_manifests(
                stage,
                request=request,
                selected_files=selected_files,
            )

            if len(resolved_revisions) > 1:
                raise RuntimeError(f"Downloaded files resolved to mixed revisions: {sorted(resolved_revisions)}")
            if revision in {"main", "master"} and resolved_revisions:
                revision = next(iter(resolved_revisions))
            elif resolved.derive_from_content:
                revision = self._content_revision(staged_files)

            if resolved.verify_remote_after_download:
                verified = self._resolve_modelscope_revision(request)
                if verified.identifier != resolved.identifier:
                    raise RuntimeError(f"ModelScope revision changed during download: {resolved.identifier} -> {verified.identifier}")

            token = snapshot_token(
                backend=request.backend,
                repo_id=request.provider_repo_id,
                resolved_revision=revision,
            )
            revision_name = self._revision_dir_name(revision, token, selected_files)
            revision_dir = revisions_root / revision_name
            (stage / STAGING_LEASE_FILENAME).unlink(missing_ok=True)
            # Preserve the revision directory's commit-time mtime: query leases
            # churn inside this permanent child directory instead.
            (stage / REVISION_LEASE_DIRECTORY).mkdir(parents=True, exist_ok=True)
            if force:
                # A forced administrative repair publishes a new immutable
                # generation without removing paths held by existing queries.
                revision_dir = revisions_root / f"{revision_name}-force-{uuid.uuid4().hex}"
                os.rename(stage, revision_dir)
            else:
                try:
                    os.rename(stage, revision_dir)
                except OSError:
                    if not revision_dir.is_dir():
                        raise
                    self._validate_revision_dir(revision_dir, selected_files)
                    shutil.rmtree(stage, ignore_errors=True)

            files = tuple(
                SnapshotFile(
                    relative_path=relative_path,
                    local_path=revision_dir.joinpath(*PurePosixPath(relative_path).parts),
                    size=self._validate_snapshot_file(revision_dir.joinpath(*PurePosixPath(relative_path).parts)),
                )
                for relative_path, _staged_path, _size in staged_files
            )
            snapshot = SnapshotRef(
                backend=request.backend,
                repo_id=request.provider_repo_id,
                resolved_revision=revision,
                snapshot_token=token,
                files=files,
            )
            self._write_active(repo_root, snapshot)
            return snapshot
        except Exception:
            shutil.rmtree(stage, ignore_errors=True)
            raise

    @staticmethod
    def _write_staging_lease(stage: Path) -> None:
        lease = stage / STAGING_LEASE_FILENAME
        payload = {
            "pid": os.getpid(),
            "created_at": time.time(),
        }
        with lease.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())

    def _cleanup_staging(self, staging_root: Path) -> None:
        """Remove only expired staging dirs whose owning process is gone.

        Directories without a valid DojoSDK lease are deliberately preserved:
        without ownership evidence they cannot safely be classified as orphaned.
        """
        cutoff = time.time() - self._cfg.snapshot_staging_ttl_seconds
        try:
            candidates = tuple(staging_root.iterdir())
        except OSError:
            return
        for candidate in candidates:
            if not candidate.is_dir():
                continue
            lease = candidate / STAGING_LEASE_FILENAME
            try:
                payload = json.loads(lease.read_text(encoding="utf-8"))
                pid = int(payload["pid"])
                created_at = float(payload["created_at"])
            except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
                continue
            if created_at > cutoff or self._process_is_alive(pid):
                continue
            try:
                shutil.rmtree(candidate)
                logger.info("Removed expired orphan snapshot staging directory: %s", candidate)
            except OSError as error:
                logger.warning("Failed to remove orphan snapshot staging directory %s: %s", candidate, error)

    @staticmethod
    def _process_is_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return True
        return True

    def _download_file(
        self,
        request: _RepoRequest,
        *,
        revision: str,
        cache_revision: str,
        relative_path: str,
        force: bool,
    ) -> Path:
        key = (request.backend, request.provider_repo_id, cache_revision, relative_path)
        with self._locks.hold(("file",) + key):
            cached = self._get_cached_path(key)
            if cached is not None and cached.exists() and not force:
                return cached

            attempts = max(0, int(self._cfg.max_download_retries)) + 1
            for attempt in range(1, attempts + 1):
                try:
                    if request.backend == "modelscope":
                        from modelscope.hub.file_download import dataset_file_download

                        ms_revision = "master" if revision == "main" else revision
                        path = Path(
                            dataset_file_download(
                                dataset_id=request.provider_repo_id,
                                file_path=relative_path,
                                revision=ms_revision,
                                token=self._cfg.modelscope_token,
                                cache_dir=self._cfg.cache_dir,
                                local_files_only=self._cfg.local_only,
                            )
                        )
                    else:
                        from huggingface_hub import hf_hub_download

                        path = Path(
                            hf_hub_download(
                                repo_id=request.provider_repo_id,
                                filename=relative_path,
                                repo_type=request.repo_type,
                                token=self._cfg.token,
                                revision=revision,
                                cache_dir=self._cfg.cache_dir,
                                local_files_only=self._cfg.local_only,
                                force_download=force,
                                etag_timeout=self._cfg.etag_timeout_seconds,
                            )
                        )
                    if not path.is_file():
                        raise FileNotFoundError(f"Provider returned a missing file: {path}")
                    self._remember_path(key, path)
                    return path
                except Exception as error:
                    if attempt >= attempts or not self._is_transient_download_error(error):
                        raise
                    delay = min(0.5 * (2 ** (attempt - 1)), 4.0)
                    time.sleep(delay)
            raise RuntimeError("unreachable snapshot download retry state")

    @staticmethod
    def _is_transient_download_error(error: Exception) -> bool:
        import httpx
        from huggingface_hub.errors import HfHubHTTPError

        if isinstance(error, (httpx.TimeoutException, httpx.TransportError, TimeoutError, ConnectionError)):
            return True
        if not isinstance(error, HfHubHTTPError):
            return False
        response = getattr(error, "response", None)
        status_code = getattr(response, "status_code", None)
        return status_code in {408, 429} or (isinstance(status_code, int) and status_code >= 500)

    def _get_cached_path(self, key: tuple[str, str, str, str]) -> Path | None:
        with self._path_cache_guard:
            path = self._path_cache.get(key)
            if path is not None:
                self._path_cache.move_to_end(key)
            return path

    def _remember_path(self, key: tuple[str, str, str, str], path: Path) -> None:
        with self._path_cache_guard:
            self._path_cache[key] = path
            self._path_cache.move_to_end(key)
            while len(self._path_cache) > self._path_cache_size:
                self._path_cache.popitem(last=False)

    @staticmethod
    def _link_or_copy(source: Path, destination: Path) -> None:
        # Provider cache paths can be relative symlinks that break when moved.
        source = source.resolve(strict=True)
        try:
            os.link(source, destination)
        except OSError:
            shutil.copy2(source, destination)

    @staticmethod
    def _content_revision(staged_files: Sequence[tuple[str, Path, int]]) -> str:
        digest = hashlib.sha256()
        for relative_path, path, size in sorted(staged_files):
            digest.update(relative_path.encode("utf-8"))
            digest.update(b"\0")
            digest.update(str(size).encode("ascii"))
            digest.update(b"\0")
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
        return f"local-manifest-{digest.hexdigest()}"

    @staticmethod
    def _validate_snapshot_file(path: Path) -> int:
        if not path.is_file():
            raise FileNotFoundError(f"Snapshot file does not exist: {path}")
        size = path.stat().st_size
        if size <= 0:
            raise ValueError(f"Snapshot file is empty: {path}")
        if path.suffix.lower() == ".parquet":
            metadata = pq.ParquetFile(path).metadata
            if metadata is None:
                raise ValueError(f"Parquet footer is missing: {path}")
        return size

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _schema_fingerprint(path: Path) -> str:
        schema = pq.ParquetFile(path).schema_arrow
        return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()

    def _validate_partition_manifests(
        self,
        root: Path,
        *,
        request: _RepoRequest,
        selected_files: Sequence[str],
    ) -> None:
        """Validate a selected partition layout before it can become active."""
        selected = set(selected_files)
        selected_manifests = [path for path in request.partition_manifests if path in selected]
        if not selected_manifests:
            return

        partition_files = {path for path in selected if any(fnmatch.fnmatchcase(path, pattern) for pattern in request.partition_globs)}
        if not partition_files:
            raise ValueError("Partition manifest was selected without partition files")

        for manifest_relative in selected_manifests:
            manifest_path = root.joinpath(*PurePosixPath(manifest_relative).parts)
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise ValueError(f"Invalid partition manifest {manifest_relative}: {error}") from error
            if not isinstance(manifest, dict):
                raise ValueError(f"Partition manifest must be an object: {manifest_relative}")

            required = {
                "layout_version",
                "hash_algorithm",
                "bucket_count",
                "symbol_normalization",
                "schema_fingerprint",
                "row_count",
                "min_bar_time",
                "max_bar_time",
                "files",
            }
            missing = sorted(required - manifest.keys())
            if missing:
                raise ValueError(f"Partition manifest is missing fields {missing}: {manifest_relative}")
            if manifest["layout_version"] != 1:
                raise ValueError(f"Unsupported partition layout_version: {manifest['layout_version']!r}")
            if manifest["hash_algorithm"] != "sha256":
                raise ValueError(f"Unsupported partition hash_algorithm: {manifest['hash_algorithm']!r}")
            if manifest["symbol_normalization"] != "strip_upper":
                raise ValueError(f"Unsupported partition symbol_normalization: {manifest['symbol_normalization']!r}")
            if not isinstance(manifest["bucket_count"], int) or manifest["bucket_count"] <= 0:
                raise ValueError("Partition manifest bucket_count must be a positive integer")

            declared = manifest["files"]
            if not isinstance(declared, dict):
                raise ValueError("Partition manifest files must be an object")
            declared_files = set(declared)
            if declared_files != partition_files:
                missing_files = sorted(partition_files - declared_files)
                extra_files = sorted(declared_files - partition_files)
                raise ValueError("Partition manifest file set mismatch: " f"missing={missing_files}, extra={extra_files}")

            expected_schema = str(manifest["schema_fingerprint"])
            total_rows = 0
            for relative_path in sorted(declared_files):
                safe_path = self._validated_relative_path(relative_path)
                entry = declared[relative_path]
                if not isinstance(entry, dict):
                    raise ValueError(f"Partition manifest entry must be an object: {relative_path}")
                file_path = root.joinpath(*PurePosixPath(safe_path).parts)
                actual_size = self._validate_snapshot_file(file_path)
                metadata = pq.ParquetFile(file_path).metadata
                expected_size = entry.get("size")
                expected_rows = entry.get("row_count")
                expected_sha256 = entry.get("sha256")
                if actual_size != expected_size:
                    raise ValueError(f"Partition file size mismatch for {relative_path}: " f"expected={expected_size}, actual={actual_size}")
                if metadata.num_rows != expected_rows:
                    raise ValueError(f"Partition row_count mismatch for {relative_path}: " f"expected={expected_rows}, actual={metadata.num_rows}")
                actual_sha256 = self._file_sha256(file_path)
                if actual_sha256 != expected_sha256:
                    raise ValueError(f"Partition checksum mismatch for {relative_path}")
                if self._schema_fingerprint(file_path) != expected_schema:
                    raise ValueError(f"Partition schema mismatch for {relative_path}")
                total_rows += metadata.num_rows

            if total_rows != manifest["row_count"]:
                raise ValueError(f"Partition total row_count mismatch: expected={manifest['row_count']}, actual={total_rows}")

    def _reusable_partition_files(
        self,
        stage: Path,
        *,
        request: _RepoRequest,
        selected_files: Sequence[str],
        previous: SnapshotRef,
    ) -> dict[str, Path]:
        """Return unchanged buckets that can be linked from the old revision."""
        previous_files = {item.relative_path: item.local_path for item in previous.files}
        reusable: dict[str, Path] = {}
        for manifest_relative in request.partition_manifests:
            if manifest_relative not in selected_files:
                continue
            old_manifest_path = previous_files.get(manifest_relative)
            new_manifest_path = stage.joinpath(*PurePosixPath(manifest_relative).parts)
            if old_manifest_path is None or not old_manifest_path.is_file():
                continue
            try:
                old_manifest = json.loads(old_manifest_path.read_text(encoding="utf-8"))
                new_manifest = json.loads(new_manifest_path.read_text(encoding="utf-8"))
                old_entries = old_manifest["files"]
                new_entries = new_manifest["files"]
            except (OSError, KeyError, TypeError, json.JSONDecodeError):
                continue
            if not isinstance(old_entries, dict) or not isinstance(new_entries, dict):
                continue

            for relative_path, new_entry in new_entries.items():
                if relative_path not in selected_files or not isinstance(new_entry, dict):
                    continue
                old_entry = old_entries.get(relative_path)
                old_path = previous_files.get(relative_path)
                if not isinstance(old_entry, dict) or old_path is None:
                    continue
                if not all(old_entry.get(field) == new_entry.get(field) for field in ("size", "sha256", "row_count")):
                    continue
                try:
                    if old_path.stat().st_size != new_entry.get("size"):
                        continue
                    if self._file_sha256(old_path) != new_entry.get("sha256"):
                        continue
                except OSError:
                    continue
                reusable[relative_path] = old_path
        return reusable

    def _validate_revision_dir(self, revision_dir: Path, files: Sequence[str]) -> None:
        for relative_path in files:
            self._validate_snapshot_file(revision_dir.joinpath(*PurePosixPath(relative_path).parts))

    def _load_active(self, request: _RepoRequest) -> SnapshotRef | None:
        active_path = self._repo_root(request) / "active.json"
        cache_key = (request.backend, request.provider_repo_id)
        try:
            pointer_signature = self._pointer_signature(active_path.stat())
        except OSError:
            self._forget_active(cache_key)
            return None

        cached = self._get_cached_active(cache_key, pointer_signature)
        if cached is not None:
            if self._snapshot_files_match_sizes(cached):
                return cached
            self._forget_active(cache_key)
            return None

        try:
            snapshot = SnapshotRef.model_validate_json(active_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            self._forget_active(cache_key)
            return None
        if snapshot.backend != request.backend or snapshot.repo_id != request.provider_repo_id:
            self._forget_active(cache_key)
            return None
        try:
            for file in snapshot.files:
                size = self._validate_snapshot_file(file.local_path)
                if size != file.size:
                    self._forget_active(cache_key)
                    return None
        except (OSError, ValueError):
            self._forget_active(cache_key)
            return None
        self._remember_active(cache_key, pointer_signature, snapshot)
        return snapshot

    @staticmethod
    def _pointer_signature(stat_result: os.stat_result) -> tuple[int, int, int, int]:
        return (
            int(stat_result.st_ino),
            int(stat_result.st_size),
            int(stat_result.st_mtime_ns),
            int(stat_result.st_ctime_ns),
        )

    def _get_cached_active(
        self,
        key: tuple[str, str],
        pointer_signature: tuple[int, int, int, int],
    ) -> SnapshotRef | None:
        with self._active_cache_guard:
            entry = self._active_cache.get(key)
            if entry is None or entry.pointer_signature != pointer_signature:
                return None
            self._active_cache.move_to_end(key)
            return entry.snapshot

    def _remember_active(
        self,
        key: tuple[str, str],
        pointer_signature: tuple[int, int, int, int],
        snapshot: SnapshotRef,
    ) -> None:
        with self._active_cache_guard:
            self._active_cache[key] = _ActiveSnapshotCacheEntry(pointer_signature, snapshot)
            self._active_cache.move_to_end(key)
            while len(self._active_cache) > self._path_cache_size:
                self._active_cache.popitem(last=False)

    def _forget_active(self, key: tuple[str, str]) -> None:
        with self._active_cache_guard:
            self._active_cache.pop(key, None)

    @staticmethod
    def _snapshot_files_match_sizes(snapshot: SnapshotRef) -> bool:
        """Cheap immutable-snapshot check used after one full footer validation."""
        try:
            return all(file.local_path.is_file() and file.local_path.stat().st_size == file.size for file in snapshot.files)
        except OSError:
            return False

    @staticmethod
    def _snapshot_revision_dir(snapshot: SnapshotRef | None) -> Path | None:
        if snapshot is None or not snapshot.files:
            return None
        file = snapshot.files[0]
        revision_dir = file.local_path
        for _part in PurePosixPath(file.relative_path).parts:
            revision_dir = revision_dir.parent
        return revision_dir

    @staticmethod
    def _revision_has_leases(revision_dir: Path) -> bool:
        lease_dir = revision_dir / REVISION_LEASE_DIRECTORY
        try:
            leases = tuple(path for path in lease_dir.iterdir() if path.is_file())
        except OSError:
            return False
        has_live_lease = False
        for lease in leases:
            try:
                pid = int(lease.name.split("-", 1)[0])
            except (ValueError, IndexError):
                # An unrecognized lease cannot safely be declared stale.
                has_live_lease = True
                continue
            if SnapshotResolver._process_is_alive(pid):
                has_live_lease = True
                continue
            lease.unlink(missing_ok=True)
        return has_live_lease

    @staticmethod
    def _directory_size(directory: Path) -> int:
        total = 0
        for root, _directories, files in os.walk(directory):
            for filename in files:
                try:
                    total += (Path(root) / filename).stat().st_size
                except OSError:
                    continue
        return total

    @contextmanager
    def _repo_process_lock(self, repo_root: Path) -> Iterator[None]:
        """Serialize publication, lease acquisition, and GC across processes.

        Acquisition is bounded by ``snapshot_lock_timeout_seconds``. Without a
        deadline a peer that takes this lock and then stalls (for example a
        snapshot sync blocked on a dead network) freezes every reader in every
        process forever, because readers need this same lock. ``0`` restores
        the legacy unbounded wait.
        """
        repo_root.mkdir(parents=True, exist_ok=True)
        lock_path = repo_root / ".repo.lock"
        handle = lock_path.open("a+b")
        acquired = False
        try:
            acquired = self._acquire_repo_process_lock(handle, lock_path)
            if not acquired:
                raise SnapshotLockTimeoutError(
                    path=str(lock_path),
                    timeout_seconds=self._cfg.snapshot_lock_timeout_seconds,
                )
            yield
        finally:
            try:
                if acquired:
                    self._unlock_repo_process_handle(handle)
            finally:
                handle.close()

    def _acquire_repo_process_lock(self, handle: Any, lock_path: Path) -> bool:
        """Bounded acquisition of the exclusive repository lock.

        Returns ``False`` when the deadline expires instead of blocking
        forever. ``snapshot_lock_timeout_seconds <= 0`` keeps the legacy
        unbounded wait.
        """
        timeout = self._cfg.snapshot_lock_timeout_seconds
        if timeout <= 0:
            self._lock_repo_process_handle(handle, lock_path, blocking=True)
            return True

        deadline = time.monotonic() + timeout
        interval = 0.05
        while True:
            if self._lock_repo_process_handle(handle, lock_path, blocking=False):
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(interval, remaining))
            interval = min(interval * 2, 0.5)

    @staticmethod
    def _lock_repo_process_handle(handle: Any, lock_path: Path, *, blocking: bool) -> bool:
        """Take the exclusive repository lock; ``False`` when it is busy."""
        if os.name == "nt":
            import msvcrt

            if lock_path.stat().st_size == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
            try:
                msvcrt.locking(handle.fileno(), mode, 1)
            except OSError:
                return False
            return True

        import fcntl

        flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
        try:
            fcntl.flock(handle.fileno(), flags)
        except OSError:
            return False
        return True

    @staticmethod
    def _unlock_repo_process_handle(handle: Any) -> None:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            return

        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _snapshot_has_files(snapshot: SnapshotRef, required_files: Sequence[str]) -> bool:
        available = {file.relative_path for file in snapshot.files}
        return set(required_files).issubset(available)

    @staticmethod
    def _snapshot_satisfies_request(snapshot: SnapshotRef, request: _RepoRequest) -> bool:
        available = {file.relative_path for file in snapshot.files}
        exact_required = set(request.files) - set(request.partition_fallbacks)
        if not exact_required.issubset(available):
            return False
        if not request.partition_globs:
            return set(request.files).issubset(available)
        has_partition_layout = all(manifest in available for manifest in request.partition_manifests) and any(
            fnmatch.fnmatchcase(path, pattern) for path in available for pattern in request.partition_globs
        )
        has_legacy_layout = set(request.partition_fallbacks).issubset(available)
        return has_partition_layout or has_legacy_layout

    @staticmethod
    def _revision_from_provider_path(path: Path) -> str | None:
        parts = path.parts
        try:
            index = parts.index("snapshots")
        except ValueError:
            return None
        return parts[index + 1] if index + 1 < len(parts) else None

    def _repo_root(self, request: _RepoRequest) -> Path:
        identity = f"{request.backend}\0{request.provider_repo_id}"
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
        slug = request.provider_repo_id.replace("/", "--").replace(os.sep, "--")
        return self._root / request.backend / f"{slug}-{digest}"

    @staticmethod
    def _revision_dir_name(revision: str, token: str, files: Sequence[str]) -> str:
        safe = "".join(character if character.isalnum() or character in "._-" else "-" for character in revision)
        manifest = hashlib.sha256("\0".join(sorted(files)).encode("utf-8")).hexdigest()[:12]
        return f"{safe[:80]}-{token[-16:]}-{manifest}"

    @staticmethod
    def _write_active(repo_root: Path, snapshot: SnapshotRef) -> None:
        repo_root.mkdir(parents=True, exist_ok=True)
        active_path = repo_root / "active.json"
        temporary = repo_root / f".active-{uuid.uuid4().hex}.tmp"
        payload = snapshot.model_dump(mode="json")
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, active_path)
