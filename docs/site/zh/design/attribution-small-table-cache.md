# Attribution 规范视图与小表缓存

Attribution 因子读取不再使用遗留的进程级 `_df_cache`。读取时会捕获一个
attribution 快照，用 Polars 派生规范的 `sector_ref` 和归一化的
`sector_id_list`，然后查询规范的 LazyFrame。

实例本地的小表缓存只允许两个可复用视图：

- `/api/qdata/v1/analysis/attribution_factor`: `attribution_canonical`
- `/api/qdata/v1/sector/info`: `sector_info`

每个缓存键包含端点、视图版本，以及每个贡献快照的 backend、repository、
resolved revision 和 snapshot token。范围查询（scope query）会同时包含
attribution 和 sector-info 两个快照的身份。因此 revision 变化会自然产生
缓存未命中，不需要可变的缓存失效机制。

缓存会拒绝超过行数或单条字节数阈值的条目，强制执行总字节预算和条目数上限，
并按最近最少使用（LRU）淘汰。K 线、新闻、完整财务表和用户过滤后的查询结果
永远不会进入该缓存。
