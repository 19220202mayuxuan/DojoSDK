# Reference

本分类记录调用方可以依赖的稳定接口、契约与清单。

- [下一代大版本的离线兼容性移除清单](offline-deprecations.md)：待移除接口、替代方案、移除门槛与迁移示例。

## MarketResearch 写入契约（0.1.31）

`create_market_research_result(research_uid, *, body)` 追加一次结果，必须同时提供 HTML `outcome` 和非空白 Markdown `outcome_markdown`；缺失、空字符串、null 或纯空白 Markdown 会在发送前拒绝。历史响应仍允许缺失 Markdown，并按空字符串读取。

同步与异步结果追加均强制 `max_retries=0`，不改变客户端读请求的重试配置。500、超时和连接失败可能存在写入副作用；先读取结果核验，再人工决定恢复，禁止盲目重复追加。HTTP 错误文本同时支持 FastAPI `detail` 和 qdata `message`，原始响应仍保存在异常 `body` 中。

## MarketResearch 结果内容契约（0.1.32）

结果快照可通过可空的 `thumbnail_base64` 字段写入 Base64 编码的缩略封面图，包括创建 research 时的 `initial_result`、追加 result 和修正 result。

research 列表、`public_research` 列表、research 详情和 result 历史查询均接受 `result_content_fields`。该参数使用逗号分隔，可选择 `outcome`、`outcome_markdown` 和 `thumbnail_base64`，例如 `result_content_fields="outcome,thumbnail_base64"`。未选择的内容字段返回 `None`；不传参数时返回全部三个字段。服务端也会按相同选择过滤 `producer_payload`。
