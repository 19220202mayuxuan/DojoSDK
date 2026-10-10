# S4 offline datasource rewrite

`HuggingFaceDataSource` no longer contains the legacy full-table Arrow or
Pandas loaders. Ordinary endpoint queries and the bounded `fetch_df()`
compatibility method both use the captured snapshot and Polars lazy query path.
Preload and background refresh synchronize snapshots without constructing
tables.

The remaining caches are instance-local and bounded:

- `SmallTableCache` for explicitly approved attribution/taxonomy views;
- `ParquetMetadataCache` for JSON column metadata;
- `SnapshotResolver`'s revision-aware path LRU.

Offline client requests pass the datasource's Python envelope directly into the
shared response parser. `StructuredOfflineResponse` provides the existing raw
response interface by referencing that payload; it does not encode and decode
JSON. Online responses continue through the normal `httpx.Response` JSON path.
