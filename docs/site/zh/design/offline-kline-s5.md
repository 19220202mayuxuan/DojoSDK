# S5 快照感知的股票 K 线

S5 为离线股票 K 线查询增加了有界批量 API：

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

`get_klines()` 仅在客户端使用离线快照数据源时可用。在线客户端继续使用
`get_kline()`，因为在线响应无法真实地提供本地 Parquet 快照的身份信息。

## 结果契约

返回值是 `KlineBatchResult`：

```python
class KlineBatchResult:
    rows: list[Kline]
    snapshot: SnapshotMetadata
```

`SnapshotMetadata` 包含 `backend`、`repo_id`、`resolved_revision` 和
`snapshot_token`。resolver 在构建 lazy 查询之前捕获 `SnapshotRef`，
随行数据返回的元数据来自这个确切的引用。消费方在缓存或派生结果时必须
保留这些元数据；不得在查询结束后重新读取当前 revision。

## 过滤、限制与排序

执行契约为：

1. 过滤 symbols、闭区间 start/end 边界、interval 及其他字段；
2. 对每个 symbol 按时间降序排列，选取最新的 `limit_per_symbol` 行；
3. 将选出的结果按 symbol 和时间升序排列；
4. 投影请求的输出字段，只收集有界的最终结果。

`limit_per_symbol=None` 关闭每 symbol 限制，但不关闭全局物化限制。
`limit_per_symbol=0` 返回空行。行数不足 N 的 symbol 返回全部可用行。

这不是可选的全表模式。例如，请求 877 万行的 K 线结果且
`limit_per_symbol=None` 时，一旦观察到 `max_materialized_rows + 1` 行
仍会抛出 `MaterializationLimitError`。请使用 symbol/日期过滤和有限的
`limit_per_symbol`，或将导出拆分为有界窗口。该限制在完整收集之前生效，
因此异常保护的是进程内存，而不是事后报告 OOM。

通用离线 `fetch_df()` API 遵循相同规则：只把有界的最终结果转换为
Pandas，不会绕过 `max_materialized_rows`，也不会静默分页或截断查询。
需要全部行的调用方必须实现显式的有界分页/窗口化，而不是把 `fetch_df()`
当作全量数据加载器。

这个升序展示顺序是 S5 相对 S0 实现中偶然出现的降序顺序的显式 opt-out。
S0 只保证 `limit` 选取最新的 N 行，并未定义这些行的展示顺序。

## 复权归属

`adjust="pre"`/`"qfq"` 与 `adjust="post"`/`"hfq"` 在 DojoSDK 内部完成。
SDK 可能会扫描请求日期窗口之外的参考因子，但只返回请求的行。下游应用
不得二次复权。

## 兼容与弃用

- 在线 `get_kline()` 继续受支持，不发出 S5 弃用警告。
- 离线 `get_kline()` 委托给 `get_klines()` 并发出弃用警告，同时保留
  旧响应 envelope。
- `get_all_klines()` 和 `get_all_klines_with_df()` 已弃用。它们现在要求
  显式非空 symbol 列表并委托给有界批量 API；无界的全市场加载会被拒绝。
