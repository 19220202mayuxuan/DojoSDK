"""Real-provider acceptance check for one S7 K-line snapshot backend.

Run once for ``huggingface`` and once for ``modelscope`` after the same S7
revision has been published to each provider. The second query is forced into
local-only mode and must return identical rows from the committed snapshot.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dojo import Dojo
from dojo.datasource.config import HFConfig

KLINE_PATH = "/api/qdata/v1/stock/kline"


def _config(args: argparse.Namespace, *, local_only: bool) -> HFConfig:
    return HFConfig(
        backend=args.backend,
        cache_dir=str(args.cache_dir.expanduser().resolve()),
        revision=args.revision,
        local_only=local_only,
        token=args.hf_token,
        modelscope_token=args.modelscope_token,
    )


def _query(args: argparse.Namespace, *, local_only: bool):
    client = Dojo(online=False, offline_config=_config(args, local_only=local_only))
    try:
        if not local_only:
            sync = client.sync_offline_snapshots(
                paths=[KLINE_PATH], check_remote=True, force=False
            )
            if sync.failures:
                raise RuntimeError(f"snapshot sync failed: {sync.failures}")
        result = client.stocks.get_klines(
            [args.symbol],
            fields=["symbol", "bar_time", "close"],
            limit_per_symbol=args.limit,
        )
        snapshot = result.snapshot
        active = client.ensure_offline_snapshots(paths=[KLINE_PATH])[0]
        relative_paths = {item.relative_path for item in active.files}
        if "manifest.json" not in relative_paths or not any(
            path.startswith("bucket=") and path.endswith(".parquet")
            for path in relative_paths
        ):
            raise RuntimeError(
                f"{args.backend} active revision is not an S7 partition layout: "
                f"{active.resolved_revision}"
            )
        return result, snapshot
    finally:
        http_client = getattr(client, "_client", None)
        close = getattr(http_client, "close", None)
        if callable(close):
            close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", required=True, choices=("huggingface", "modelscope"))
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--revision", default="main")
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--hf-token")
    parser.add_argument("--modelscope-token")
    args = parser.parse_args()

    online_result, online_snapshot = _query(args, local_only=False)
    offline_result, offline_snapshot = _query(args, local_only=True)
    if online_result.data != offline_result.data:
        raise RuntimeError("online-sync query and disconnected query returned different rows")
    if online_snapshot.snapshot_token != offline_snapshot.snapshot_token:
        raise RuntimeError("disconnected query did not reuse the committed snapshot token")
    print(
        json.dumps(
            {
                "backend": args.backend,
                "repo_id": online_snapshot.repo_id,
                "resolved_revision": online_snapshot.resolved_revision,
                "snapshot_token": online_snapshot.snapshot_token,
                "rows": len(online_result.data),
                "offline_match": True,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
