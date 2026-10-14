"""Convert a legacy K-line Parquet file into an integrity-checked S7 layout.

Input batches are first routed into per-bucket Arrow IPC spools. Each bucket is
then encoded once with deliberately sized Parquet row groups, avoiding the
hundreds of tiny row groups produced by direct fan-out writes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def symbol_bucket(symbol: object, bucket_count: int) -> int:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise ValueError("K-line symbol must be non-empty")
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return int(digest, 16) % bucket_count


def _schema_fingerprint(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _scalar_text(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "as_py"):
        value = value.as_py()
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def partition_snapshot(
    source: Path,
    destination: Path,
    *,
    bucket_count: int = 256,
    batch_size: int = 200_000,
    row_group_size: int = 131_072,
    compression: str = "zstd",
    compression_level: int | None = 6,
    use_dictionary: bool | tuple[str, ...] = ("symbol", "kline_t", "exchange", "name"),
) -> dict[str, object]:
    if bucket_count <= 0:
        raise ValueError("bucket_count must be positive")
    if batch_size <= 0 or row_group_size <= 0:
        raise ValueError("batch_size and row_group_size must be positive")
    if not source.is_file():
        raise FileNotFoundError(source)
    if destination.exists() and any(destination.iterdir()):
        raise FileExistsError(f"destination must be empty: {destination}")
    destination.mkdir(parents=True, exist_ok=True)

    parquet = pq.ParquetFile(source)
    schema = parquet.schema_arrow
    if "symbol" not in schema.names:
        raise ValueError("source Parquet is missing the symbol column")
    bar_time_index = schema.get_field_index("bar_time")

    spool_root = destination / ".bucket-spool"
    spool_root.mkdir()
    writers: dict[int, pa.ipc.RecordBatchFileWriter] = {}
    sinks: dict[int, pa.OSFile] = {}
    rows_by_bucket: dict[int, int] = {}
    row_count = 0
    min_bar_time: Any = None
    max_bar_time: Any = None
    try:
        for batch in parquet.iter_batches(batch_size=batch_size):
            if bar_time_index >= 0 and batch.num_rows:
                times = batch.column(bar_time_index)
                batch_min = pc.min(times).as_py()
                batch_max = pc.max(times).as_py()
                if batch_min is not None and (min_bar_time is None or batch_min < min_bar_time):
                    min_bar_time = batch_min
                if batch_max is not None and (max_bar_time is None or batch_max > max_bar_time):
                    max_bar_time = batch_max

            symbols = batch.column(batch.schema.get_field_index("symbol")).to_pylist()
            buckets = pa.array(
                [symbol_bucket(symbol, bucket_count) for symbol in symbols],
                type=pa.int32(),
            )
            for bucket in sorted(set(buckets.to_pylist())):
                filtered = batch.filter(pc.equal(buckets, pa.scalar(bucket, pa.int32())))
                if filtered.num_rows == 0:
                    continue
                writer = writers.get(bucket)
                if writer is None:
                    sink = pa.OSFile(str(spool_root / f"bucket-{bucket:03d}.arrow"), "wb")
                    writer = pa.ipc.new_file(sink, schema)
                    sinks[bucket] = sink
                    writers[bucket] = writer
                writer.write_batch(filtered)
                rows_by_bucket[bucket] = rows_by_bucket.get(bucket, 0) + filtered.num_rows
                row_count += filtered.num_rows
    finally:
        for writer in writers.values():
            writer.close()
        for sink in sinks.values():
            sink.close()

    files: dict[str, dict[str, object]] = {}
    try:
        for bucket in sorted(rows_by_bucket):
            spool_path = spool_root / f"bucket-{bucket:03d}.arrow"
            with pa.memory_map(str(spool_path), "r") as source_handle:
                table = pa.ipc.open_file(source_handle).read_all()
            relative_path = f"bucket={bucket:03d}/part-00000.parquet"
            output_path = destination / relative_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            options: dict[str, object] = {
                "compression": compression,
                "use_dictionary": list(use_dictionary) if isinstance(use_dictionary, tuple) else use_dictionary,
                "row_group_size": row_group_size,
            }
            if compression_level is not None and compression.lower() != "snappy":
                options["compression_level"] = compression_level
            pq.write_table(table, output_path, **options)
            metadata = pq.ParquetFile(output_path).metadata
            files[relative_path] = {
                "size": output_path.stat().st_size,
                "sha256": _file_sha256(output_path),
                "row_count": metadata.num_rows,
            }
    finally:
        shutil.rmtree(spool_root, ignore_errors=True)

    manifest: dict[str, object] = {
        "layout_version": 1,
        "hash_algorithm": "sha256",
        "bucket_count": bucket_count,
        "symbol_normalization": "strip_upper",
        "schema_fingerprint": _schema_fingerprint(schema),
        "row_count": row_count,
        "min_bar_time": _scalar_text(min_bar_time),
        "max_bar_time": _scalar_text(max_bar_time),
        "compression": compression,
        "compression_level": None if compression.lower() == "snappy" else compression_level,
        "use_dictionary": list(use_dictionary) if isinstance(use_dictionary, tuple) else use_dictionary,
        "row_group_size": row_group_size,
        "non_empty_buckets": len(rows_by_bucket),
        "files": files,
    }
    temporary = destination / ".manifest.json.tmp"
    temporary.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, destination / "manifest.json")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--bucket-count", type=int, default=256)
    parser.add_argument("--batch-size", type=int, default=200_000)
    parser.add_argument("--row-group-size", type=int, default=131_072)
    parser.add_argument("--compression", choices=("zstd", "snappy"), default="zstd")
    parser.add_argument("--compression-level", type=int, default=6)
    parser.add_argument("--no-dictionary", action="store_true")
    args = parser.parse_args()
    manifest = partition_snapshot(
        args.source.expanduser().resolve(),
        args.destination.expanduser().resolve(),
        bucket_count=args.bucket_count,
        batch_size=args.batch_size,
        row_group_size=args.row_group_size,
        compression=args.compression,
        compression_level=args.compression_level,
        use_dictionary=(
            False
            if args.no_dictionary
            else ("symbol", "kline_t", "exchange", "name")
        ),
    )
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
