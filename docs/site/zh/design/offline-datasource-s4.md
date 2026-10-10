# S4 离线数据源重写

`HuggingFaceDataSource` 不再包含遗留的全表 Arrow 或 Pandas 加载器。
普通端点查询和有界的 `fetch_df()` 兼容方法都使用捕获的快照和 Polars
lazy 查询路径。预加载和后台刷新只同步快照，不再构建表。

剩余的缓存均为实例本地且有界：

- `SmallTableCache`：用于显式批准的 attribution/taxonomy 视图；
- `ParquetMetadataCache`：用于 JSON 列元数据；
- `SnapshotResolver` 的 revision 感知路径 LRU。

离线客户端请求将数据源的 Python envelope 直接传入共享响应解析器。
`StructuredOfflineResponse` 通过引用该 payload 提供原有的 raw response
接口，不再对 JSON 进行编码和解码。在线响应继续走正常的
`httpx.Response` JSON 路径。
