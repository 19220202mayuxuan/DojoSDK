# Test suites

DojoSDK tests are split by execution boundary:

- `unit`: deterministic tests without providers or persistent snapshots.
- `offline_integration`: local Parquet snapshot and offline datasource tests.
- `online_integration`: live Dojo API tests. These are opt-in and require
  `DOJO_RUN_ONLINE_TESTS=1` plus credentials.
- `contract`: OpenAPI and compatibility contracts.
- `slow`: large-data or long-running acceptance tests.

Typical local commands:

```bash
uv run python -m pytest -m "not online_integration and not slow" -q
uv run python -m pytest -m offline_integration -q
DOJO_RUN_ONLINE_TESTS=1 uv run python -m pytest -m online_integration -q
uv run python -m pytest -m contract -q
```

The S7 real-provider acceptance script is intentionally separate from pytest:

```bash
uv run python scripts/verify_kline_snapshot_backend.py --backend huggingface --cache-dir ~/.cache/huggingface/hub
uv run python scripts/verify_kline_snapshot_backend.py --backend modelscope --cache-dir ~/.cache/huggingface/hub
```

