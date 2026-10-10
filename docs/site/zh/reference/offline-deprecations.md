# 下一代大版本的离线兼容性移除清单

在当前 `0.x` 版本线内不要移除这些接口。它们保持为薄、有边界的兼容包装层，
并发出 `DeprecationWarning`。

| 下一个大版本移除 | 替代方案 |
| --- | --- |
| `Dojo.preload_offline_data()` | `ensure_offline_snapshots(paths=..., repos=...)` |
| `AsyncDojo.preload_offline_data()` | `ensure_offline_snapshots(paths=..., repos=...)` |
| 离线 `Stocks.get_kline()` | `Stocks.get_klines()` |
| 离线 `AsyncStocks.get_kline()` | `AsyncStocks.get_klines()` |
| `Stocks.get_all_klines()` | `Stocks.get_klines()` 并显式指定 symbols |
| `AsyncStocks.get_all_klines()` | `AsyncStocks.get_klines()` 并显式指定 symbols |
| `Stocks.get_all_klines_with_df()` | `Stocks.get_klines()` 加调用方自有的有界转换 |
| `AsyncStocks.get_all_klines_with_df()` | `AsyncStocks.get_klines()` 加调用方自有的有界转换 |

移除之前，DojoAgents 必须对这些名字没有任何运行时引用，且最低 DojoSDK
版本要求必须指向首个包含快照感知批量 API 的版本。

## 移除门槛

只有当以下每一项都成立时，下一个大版本才允许移除这些包装层：

- DojoAgents 中没有任何对弃用名字的运行时引用。
- DojoAgents 至少要求 DojoSDK `0.1.29`，即首个携带其主 K 线路径所依赖的
  快照感知批量契约的版本。
- 已知的外部调用方已完成迁移。
- 本指南已随至少一次正式弃用版本发布。
- 替代 API 已经完整度过一个发布周期且未发生破坏兼容性的契约变更。
- 兼容性测试必须与包装层在同一个大版本变更中移除，不能更早。

当前仓库状态：前两项要求已满足。外部调用方迁移和正式弃用发布周期仍属于
发布管理工作，因此这些包装层必须保留在当前发布线中。

## 迁移示例

替换注册表级预加载：

```python
client.ensure_offline_snapshots(
    paths=["/api/qdata/v1/stock/kline"],
)
```

用显式有界查询替换全市场 K 线物化：

```python
result = client.stocks.get_klines(
    ["AAPL", "MSFT"],
    fields=["symbol", "bar_time", "close"],
    limit_per_symbol=252,
)
```

缓存响应的调用方必须在缓存身份中包含
`backend + repo_id + resolved_revision`（或 `snapshot_token`）。
不得缓存全市场 DataFrame 或 LazyFrame。
