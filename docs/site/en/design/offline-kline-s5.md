# S5 snapshot-aware stock K-lines

S5 adds a bounded batch API for offline stock K-line queries:

```python
result = client.stocks.get_klines(
    ["AAPL", "MSFT"],
    start=datetime(2026, 1, 1),
    end=datetime(2026, 6, 30),
    fields=["open", "high", "low", "close", "vol"],
    limit_per_symbol=252,
    adjust="pre",
)
```

`get_klines()` is available only when the client uses the offline snapshot data
source. Online clients continue to use `get_kline()`, because an online response
cannot truthfully provide the identity of a local Parquet snapshot.

## Result contract

The return value is `KlineBatchResult`:

```python
class KlineBatchResult:
    rows: list[Kline]
    snapshot: SnapshotMetadata
```

`SnapshotMetadata` contains `backend`, `repo_id`, `resolved_revision`, and
`snapshot_token`. The resolver captures the `SnapshotRef` before constructing
the lazy query, and the metadata returned with the rows comes from that exact
reference. Consumers must retain this metadata with cached or derived results;
they must not read the current revision after the query finishes.

## Filtering, limit, and ordering

The execution contract is:

1. filter symbols, inclusive start/end boundaries, interval, and other fields;
2. for each symbol, order by time descending and select the newest
   `limit_per_symbol` rows;
3. order the selected result by symbol and time ascending;
4. project requested output fields and collect only the bounded final result.

`limit_per_symbol=None` disables the per-symbol limit but not the global
materialization limit. `limit_per_symbol=0` returns no rows. A symbol with fewer
than N rows returns all available rows.

This is intentionally not an opt-in full-table mode. For example, asking for an
8.77-million-row K-line result with `limit_per_symbol=None` still raises
`MaterializationLimitError` once `max_materialized_rows + 1` rows are observed.
Use symbol/date filters and a finite `limit_per_symbol`, or split an export into
bounded windows. The limit is applied before full collection, so the exception
protects process memory rather than reporting an OOM after the fact.

The generic offline `fetch_df()` API follows the same rule. It converts only a
bounded final result to Pandas; it does not bypass `max_materialized_rows` and
does not silently paginate or truncate a query. Callers that need every row must
implement explicit bounded pagination/windowing instead of relying on
`fetch_df()` as a full-dataset loader.

This ascending presentation order is an explicit S5 opt-out from any incidental
descending order observed through the S0 implementation. S0 guaranteed that
`limit` selected the newest N rows; it did not define their presentation order.

## Adjustment ownership

`adjust="pre"`/`"qfq"` and `adjust="post"`/`"hfq"` are applied inside DojoSDK.
The SDK may scan a reference factor outside the requested date window, but only
the requested rows are returned. Downstream applications must not apply a
second adjustment.

## Compatibility and deprecation

- Online `get_kline()` remains supported and emits no S5 deprecation warning.
- Offline `get_kline()` delegates to `get_klines()` and emits a deprecation
  warning while retaining its legacy response envelope.
- `get_all_klines()` and `get_all_klines_with_df()` are deprecated. They now
  require an explicit non-empty symbol list and delegate to the bounded batch
  API; unbounded full-market loading is rejected.
