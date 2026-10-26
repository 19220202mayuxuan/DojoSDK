# Reference

本分类记录调用方可以依赖的稳定接口、契约与清单。

- [下一代大版本的离线兼容性移除清单](offline-deprecations.md)：待移除接口、替代方案、移除门槛与迁移示例。

## MarketResearch 写入契约（0.1.31）

`create_market_research_result(research_uid, *, body)` 追加一次结果，必须同时提供 HTML `outcome` 和非空白 Markdown `outcome_markdown`；缺失、空字符串、null 或纯空白 Markdown 会在发送前拒绝。历史响应仍允许缺失 Markdown，并按空字符串读取。

同步与异步结果追加均强制 `max_retries=0`，不改变客户端读请求的重试配置。500、超时和连接失败可能存在写入副作用；先读取结果核验，再人工决定恢复，禁止盲目重复追加。HTTP 错误文本同时支持 FastAPI `detail` 和 qdata `message`，原始响应仍保存在异常 `body` 中。
