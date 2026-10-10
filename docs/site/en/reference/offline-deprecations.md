# Offline compatibility removals for the next major release

Do not remove these interfaces from the current `0.x` line. They remain thin,
bounded compatibility wrappers and emit `DeprecationWarning`.

| Remove in next major | Replacement |
| --- | --- |
| `Dojo.preload_offline_data()` | `ensure_offline_snapshots(paths=..., repos=...)` |
| `AsyncDojo.preload_offline_data()` | `ensure_offline_snapshots(paths=..., repos=...)` |
| Offline `Stocks.get_kline()` | `Stocks.get_klines()` |
| Offline `AsyncStocks.get_kline()` | `AsyncStocks.get_klines()` |
| `Stocks.get_all_klines()` | `Stocks.get_klines()` with explicit symbols |
| `AsyncStocks.get_all_klines()` | `AsyncStocks.get_klines()` with explicit symbols |
| `Stocks.get_all_klines_with_df()` | `Stocks.get_klines()` and caller-owned bounded conversion |
| `AsyncStocks.get_all_klines_with_df()` | `AsyncStocks.get_klines()` and caller-owned bounded conversion |

Before removal, DojoAgents must have no runtime references to these names and
the minimum DojoSDK version must point at the first release containing the
snapshot-aware batch API.

## Removal gate

The next major release may remove the wrappers only after every item below is
true:

- DojoAgents contains no runtime reference to the deprecated names.
- DojoAgents requires at least DojoSDK `0.1.29`, the first version carrying
  the snapshot-aware batch contract used by its main K-line path.
- Known external callers have migrated.
- This guide has shipped in at least one formal deprecation release.
- The replacement APIs have completed one full release cycle without a
  compatibility-breaking contract change.
- Compatibility tests are removed in the same major change as the wrappers,
  never earlier.

Current repository status: the first two requirements are satisfied. External
caller migration and the formal deprecation cycle remain release-management
work, so the wrappers must remain in the current release line.

## Migration examples

Replace registry-wide preload:

```python
client.ensure_offline_snapshots(
    paths=["/api/qdata/v1/stock/kline"],
)
```

Replace full-market K-line materialization with an explicitly bounded query:

```python
result = client.stocks.get_klines(
    ["AAPL", "MSFT"],
    fields=["symbol", "bar_time", "close"],
    limit_per_symbol=252,
)
```

Callers that cache the response must include
`backend + repo_id + resolved_revision` (or `snapshot_token`) in their cache
identity. They must not cache a full-market DataFrame or a LazyFrame.
