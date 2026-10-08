# Attribution canonical view and small-table cache

Attribution-factor reads no longer use the legacy process-wide `_df_cache`.
They capture an attribution snapshot, derive a canonical `sector_ref` and
normalized `sector_id_list` with Polars, then query the canonical LazyFrame.

Only two reusable views are allowed in the instance-local small-table cache:

- `/api/qdata/v1/analysis/attribution_factor`: `attribution_canonical`
- `/api/qdata/v1/sector/info`: `sector_info`

Each key contains the endpoint, view version, and every contributing snapshot's
backend, repository, resolved revision, and snapshot token. Scope queries include
both attribution and sector-info snapshot identities. A revision change therefore
creates a cache miss without requiring mutable cache invalidation.

The cache rejects entries above its row or per-entry byte threshold, enforces a
total byte budget and entry count, and evicts least-recently-used entries. K-line,
news, full financial tables, and user-filtered query results are never eligible.
