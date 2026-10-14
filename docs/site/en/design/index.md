# Design

This category holds current, authoritative technical designs and
integration contracts describing how the offline data path must work.

- [Offline Parquet snapshots](offline-snapshots.md): snapshot storage layout, staging commit protocol, revision identity, and GC.
- [Polars lazy offline queries](polars-lazy-queries.md): snapshot reference capture, bounded materialization, and process-wide collect concurrency.
- [Offline datasource S0 compatibility contract](offline-s0-compatibility.md): the frozen contract for legacy offline-query behavior.
- [S4 offline datasource rewrite](offline-datasource-s4.md): the datasource structure after removing full-table loaders.
- [S5 snapshot-aware stock K-lines](offline-kline-s5.md): the bounded batch K-line API and its result contract.
- [Attribution canonical view and small-table cache](attribution-small-table-cache.md): attribution reads and the bounded cache.

Historical proposals and migration steps live under **Plans**; stable
external interface lists live under **Reference**.
