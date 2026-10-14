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

## Process-wide collect concurrency

Every Polars materialization path uses `PolarsQueryEngine.collect_frame()` and
therefore shares one process-wide `threading.BoundedSemaphore`. This covers
synchronous SDK callers and asynchronous callers running in AnyIO worker
threads. Only the blocking `collect()` operation holds a slot; lazy-plan
construction, cache lookup, JSON conversion, and response parsing do not.

At most four `collect()` operations run concurrently by default. Override the
concurrency and optional queue timeout before serving queries:

```bash
export DOJO_POLARS_MAX_CONCURRENT_QUERIES=4
export DOJO_POLARS_QUEUE_TIMEOUT_SECONDS=60
```

`DOJO_POLARS_MAX_CONCURRENT_QUERIES=0` explicitly disables the limiter. When
the environment variable is absent, the default is `4`; no queue timeout is
applied by default, so waiters do not fail solely because of a fixed deadline.
A host that needs to preserve a programmatically configured limiter can
explicitly construct `HFConfig(polars_max_concurrent_queries=None)`. The queue
timeout cannot interrupt a query already executing in Polars; it only limits
how long a new query waits for a collect slot.

## Optional materialized-byte telemetry

Endpoint specs may set `max_materialized_bytes`. After collection and row-limit
validation, but before `DataFrame.to_dicts()`, the engine compares this value
with `DataFrame.estimated_size()`. Exceeding the threshold currently emits a
structured warning with `enforcement=disabled` and still returns the result.
The default is `None`, so no byte threshold is applied until real workload
measurements provide a defensible production value.
