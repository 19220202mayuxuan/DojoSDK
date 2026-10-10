# Offline Parquet snapshots

`SnapshotResolver` separates provider downloads from data loading. It does not
read Parquet rows and does not expose a partially downloaded repository.

## Storage and commit protocol

Committed files are stored below:

```text
<cache_dir>/dojosdk/snapshots/<backend>/<repo>/
  active.json
  revisions/<resolved-revision>-<token>-<file-manifest>/...
  .staging/<unique-id>/...
```

For every repository, all requested files are downloaded into a unique staging
directory, checked for a positive size and valid Parquet footer, and then moved
into `revisions/` with one atomic directory rename. `active.json` is replaced
atomically only after the complete revision is valid. A failed update leaves the
old pointer and files untouched.

Provider cache files are linked or copied into the DojoSDK-owned snapshot. The
resolver never deletes Hugging Face or ModelScope public cache revisions.

Each in-progress staging directory contains a process lease. Before a new sync,
the resolver removes only staging directories that are older than
`DOJO_SNAPSHOT_STAGING_TTL` (seven days by default) and whose owning process no
longer exists. Directories without a valid lease are preserved because their
ownership cannot be proven. Repeated reads of an unchanged `active.json` use a
bounded in-process cache: the Parquet footer is validated once per active
pointer generation, while later reads perform only cheap existence/size checks.
Atomic replacement of `active.json` invalidates that cache.

Committed `revisions/` are not deleted automatically. Cleanup is an explicit
administrative operation and defaults to dry-run:

```python
preview = client.gc_offline_snapshots(
    paths=["/api/qdata/v1/stock/kline"],
    min_age_seconds=7 * 24 * 60 * 60,
    keep_previous=1,
)

result = client.gc_offline_snapshots(
    paths=["/api/qdata/v1/stock/kline"],
    min_age_seconds=7 * 24 * 60 * 60,
    keep_previous=1,
    dry_run=False,
)
```

GC uses a per-repository cross-process lock, always retains the active revision,
retains the requested number of recent previous revisions, skips revisions
younger than `min_age_seconds`, and skips revisions with active query leases.
It only touches DojoSDK's own `revisions/` directories and never deletes the
Hugging Face or ModelScope public cache. Callers must explicitly select paths or
repositories, just as with synchronization.

## Revision identity

- Hugging Face snapshots use the immutable repository commit SHA.
- ModelScope file-tree metadata exposes per-file revisions rather than one
  repository head SHA. The resolver therefore hashes the required files'
  `Revision`, `Sha256`, and `Size` metadata into a stable manifest revision and
  verifies the manifest again after downloading.
- Local-only bootstrapping without remote metadata uses a content manifest.

The public `snapshot_token` includes backend, provider repository, resolved
revision, and snapshot layout version. Multi-repository consumers should use
`combine_snapshot_tokens()` over exactly the `SnapshotRef` objects captured at
the start of the query.

## Public APIs

Both `Dojo` and `AsyncDojo` expose:

```python
ensure_offline_snapshots(paths=["/api/qdata/v1/stock/kline"])

sync_offline_snapshots(
    repos=["AlphaDojo/dojo_stock_kline"],
    check_remote=True,
    force=False,
)

gc_offline_snapshots(
    repos=["AlphaDojo/dojo_stock_kline"],
    dry_run=True,
)
```

At least one non-empty `paths` or `repos` argument is mandatory. Omitting both
never means “download the complete registry.” `force=True` is reserved for
explicit repair or administration workflows.
