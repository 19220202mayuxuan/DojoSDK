# 测试套件

DojoSDK 测试按执行边界拆分：

- `unit`：确定性测试，不依赖 provider 或持久化快照。
- `offline_integration`：本地 Parquet 快照和离线数据源测试。
- `online_integration`：真实 Dojo API 测试。需显式 opt-in，要求
  `DOJO_RUN_ONLINE_TESTS=1` 以及凭据。
- `contract`：OpenAPI 和兼容性契约。
- `slow`：大数据量或长时间运行的验收测试。

典型本地命令：

```bash
uv run python -m pytest -m "not online_integration and not slow" -q
uv run python -m pytest -m offline_integration -q
DOJO_RUN_ONLINE_TESTS=1 uv run python -m pytest -m online_integration -q
uv run python -m pytest -m contract -q
```

S7 真实 provider 验收脚本有意与 pytest 分离：

```bash
uv run python scripts/verify_kline_snapshot_backend.py --backend huggingface --cache-dir ~/.cache/huggingface/hub
uv run python scripts/verify_kline_snapshot_backend.py --backend modelscope --cache-dir ~/.cache/huggingface/hub
```
