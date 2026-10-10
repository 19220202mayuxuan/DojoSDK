# DojoSDK

DojoSDK 是量化金融数据客户端。它通过统一的 `DataSource` 协议在在线
HTTP API 与离线 Parquet 快照两种模式之间透明切换，基于 `httpx` 与
`anyio` 同时提供同步（`Dojo`）和异步（`AsyncDojo`）客户端，并使用
`pydantic` 对所有出入参做严格类型校验。

## 从哪里开始

- 想理解离线快照的存储与提交协议，读 [离线 Parquet 快照](design/offline-snapshots.md)。
- 想了解离线查询的执行方式，读 [Polars lazy 离线查询](design/polars-lazy-queries.md)。
- 想迁移到新的 K 线批量 API，读 [S5 快照感知的股票 K 线](design/offline-kline-s5.md) 和
  [弃用与迁移清单](reference/offline-deprecations.md)。
- 想运行或扩展测试，读 [测试套件](development/testing.md)。

## 文档分类

| 分类 | 内容 |
| --- | --- |
| 设计（design） | 当前有效的子系统设计与契约：快照存储、Polars 查询引擎、K 线批量 API、小表缓存 |
| Reference | 调用方可依赖的稳定接口：弃用清单与移除门槛 |
| 开发（development） | 贡献者操作手册：测试套件与本地命令 |
| 计划（plans） | 实施方案与历史决策；完成后迁入上述分类 |

## 核心能力

| 能力 | 入口 |
| --- | --- |
| 在线 / 离线双数据源 | [离线 Parquet 快照](design/offline-snapshots.md) |
| 有界批量 K 线查询 | [S5 快照感知的股票 K 线](design/offline-kline-s5.md) |
| 兼容性契约 | [S0 兼容性契约](design/offline-s0-compatibility.md) |
| 弃用与迁移 | [弃用与迁移清单](reference/offline-deprecations.md) |
