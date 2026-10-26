# Polars lazy 离线查询

普通离线端点读取现在会捕获一个不可变的 `SnapshotRef`，并通过
`PolarsQueryEngine` 查询其 Parquet 文件。执行顺序为：

1. 用 `polars.scan_parquet()` 扫描捕获的快照；
2. 应用 symbol、日期和兼容的遗留字段谓词；
3. 排序、offset、limit；
4. 投影调用方的输出列；
5. 在收集（collect）之前应用 `max_materialized_rows + 1`；
6. 只对有界结果反序列化 JSON 列。

`QueryBuilder` 将 `execution_columns` 与 `output_columns` 分开，使过滤和
排序列在其工作完成前保持可用。未知的遗留参数和映射值过滤仍被忽略。
新代码可以使用带严格 schema 校验的 `QueryFilter`。

股票 K 线查询使用同一个 lazy 引擎，同时保留其历史上的每 symbol `limit`
行为。预加载和后台刷新现在只同步快照，不再创建完整内存表。
Attribution 因子查询现在使用 revision 感知的规范 Polars 视图和有界
小表缓存；原进程级 `_df_cache` 已移除。

## 进程级 collect 并发

每条 Polars 物化路径都使用 `PolarsQueryEngine.collect_frame()`，因此共享
一个进程级 `threading.BoundedSemaphore`。这同时覆盖同步 SDK 调用方和在
AnyIO worker 线程中运行的异步调用方。只有阻塞的 `collect()` 操作占用
槽位；lazy 计划构建、缓存查找、JSON 转换和响应解析都不占用。

默认最多允许 4 个并发 `collect()`。可以在服务启动前覆盖并发数和可选的
排队超时：

```bash
export DOJO_POLARS_MAX_CONCURRENT_QUERIES=4
export DOJO_POLARS_QUEUE_TIMEOUT_SECONDS=60
```

`DOJO_POLARS_MAX_CONCURRENT_QUERIES=0` 显式禁用限流器。环境变量缺席时使用
默认值 `4`，且默认不设置排队超时，因此等待者不会仅因固定时限而失败。
若宿主进程需要保留以编程方式配置的限流器，可显式构造
`HFConfig(polars_max_concurrent_queries=None)`。队列超时无法中断已在 Polars
中执行的查询；它只限制新查询等待 collect 槽位的时间。

## 可选的物化字节遥测

端点 spec 可以设置 `max_materialized_bytes`。在收集和行数上限校验之后、
`DataFrame.to_dicts()` 之前，引擎将该值与 `DataFrame.estimated_size()`
比较。超过阈值目前会发出带 `enforcement=disabled` 的结构化警告并仍返回
结果。默认值为 `None`，因此在真实工作负载测量给出可靠的生产阈值之前，
不应用任何字节阈值。
