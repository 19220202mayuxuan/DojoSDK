# Polars lazy offline queries

Ordinary offline endpoint reads now capture one immutable `SnapshotRef` and
query its Parquet files through `PolarsQueryEngine`. The execution order is:

1. scan the captured snapshot with `polars.scan_parquet()`;
2. apply symbol, date, and compatible legacy field predicates;
3. sort, offset, and limit;
4. project the caller's output columns;
5. apply `max_materialized_rows + 1` before collecting;
6. deserialize JSON columns on the bounded result only.

`QueryBuilder` keeps `execution_columns` separate from `output_columns` so
filtering and sorting columns remain available until their work is complete.
Unknown legacy parameters and mapping-valued filters remain ignored. New code
can use `QueryFilter` with strict schema validation.

Stock K-line queries use the same lazy engine while retaining their historical
per-symbol `limit` behavior. Preload and background refresh now synchronize
snapshots without creating full in-memory tables. Attribution-factor queries
now use a revision-aware canonical Polars view and bounded small-table cache;
the former process-wide `_df_cache` has been removed.
