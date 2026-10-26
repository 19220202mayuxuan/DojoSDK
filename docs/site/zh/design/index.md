# 设计

本分类保存当前有效的技术设计和集成契约，用于说明离线数据通路必须如何工作。

- [离线 Parquet 快照](offline-snapshots.md)：快照存储布局、staging 提交协议、revision 身份与 GC。
- [Polars lazy 离线查询](polars-lazy-queries.md)：快照引用捕获、有界物化与进程级 collect 并发。
- [离线数据源 S0 兼容性契约](offline-s0-compatibility.md)：遗留离线查询行为的冻结契约。
- [S4 离线数据源重写](offline-datasource-s4.md)：移除全表加载器后的数据源结构。
- [S5 快照感知的股票 K 线](offline-kline-s5.md)：有界批量 K 线 API 及其结果契约。
- [Attribution 规范视图与小表缓存](attribution-small-table-cache.md)：attribution 读取与有界缓存。

跨子系统的历史方案和迁移步骤放在 **计划**；稳定的对外接口清单放在 **Reference**。
