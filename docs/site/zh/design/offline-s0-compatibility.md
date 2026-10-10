# 离线数据源 S0 兼容性契约

本文档冻结遗留离线查询的行为。后续的 Snapshot 和 Polars 实现必须保留这些
行为，除非新的严格 API 显式 opt-out。

## Backend 选择

- `DOJO_DATA_BACKEND=huggingface` 和 `modelscope` 是权威配置，永远不会触发
  网络探测。
- `DOJO_DATA_BACKEND=auto` 使用 `httpx` 探测 Hugging Face，并按
  `DOJO_BACKEND_PROBE_TTL` 秒缓存探测结果。
- 探测失败时使用 `DOJO_DATA_BACKEND_FALLBACK`，默认为 `modelscope`，
  以在受限网络下保持既有行为。
- `DOJO_HF_OFFLINE=true` 永不探测网络，立即使用配置的 fallback。

## 通用过滤

- JSON body 中的同名参数覆盖查询参数。
- 配置的 `symbol_param` 映射到 `symbol_field`；逗号分隔的 symbols 以及
  list/tuple/set 值使用成员过滤。
- 字典值、包含字典的集合以及未知字段会被遗留 API 忽略。
- start 和 end 时间边界为闭区间（包含边界）。
- 过滤和时间排序发生在 offset 和 limit 之前；投影发生在它们之后。
- JSON 列只在最终输出行中解码。日期和 datetime 值使用 `isoformat()`
  序列化。

## 股票 K 线

- `get_kline()` 保留其既有的 raw-field 行为。
- `limit` 表示在 symbol、interval 和日期过滤之后，**每个 symbol**
  取最新 N 行。
- S0 未冻结这些被选中行的展示顺序。S5 快照感知 API 显式 opt-out 了此前
  偶然的降序顺序：它按 `symbol` 升序、然后按时间升序返回。离线
  `get_kline()` 在委托给快照 API 时继承该 S5 排序。
- 旧的 DataFrame 缓存在 S0 期间保留，因为 attribution 因子查询仍依赖它。
  只有在 attribution 规范视图和有界小表缓存实现并验证之后才允许移除。

严格的未知参数校验属于未来的 opt-in API，不得对这些遗留调用启用。
