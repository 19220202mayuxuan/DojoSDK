# DojoSDK

DojoSDK is a quantitative finance data client. A unified `DataSource`
protocol switches transparently between the online HTTP API and offline
Parquet snapshot modes. `httpx` and `anyio` back consistent synchronous
(`Dojo`) and asynchronous (`AsyncDojo`) clients, and `pydantic` enforces
strict typing on every request and response payload.

## Where to start

- For the offline snapshot storage and commit protocol, read
  [Offline Parquet snapshots](design/offline-snapshots.md).
- For offline query execution, read
  [Polars lazy offline queries](design/polars-lazy-queries.md).
- To migrate to the batch K-line API, read
  [S5 snapshot-aware stock K-lines](design/offline-kline-s5.md) and the
  [deprecation and removal list](reference/offline-deprecations.md).
- To run or extend tests, read [Test suites](development/testing.md).

## Documentation categories

| Category | Contents |
| --- | --- |
| Design | Current, authoritative subsystem designs and contracts: snapshot storage, Polars query engine, batch K-line API, small-table cache |
| Reference | Stable interfaces callers can rely on: deprecation list and removal gate |
| Development | Contributor procedures: test suites and local commands |
| Plans | Proposals and historical decisions; moved into the categories above once implemented |

## Core capabilities

| Capability | Entry point |
| --- | --- |
| Online / offline dual datasource | [Offline Parquet snapshots](design/offline-snapshots.md) |
| Bounded batch K-line queries | [S5 snapshot-aware stock K-lines](design/offline-kline-s5.md) |
| Compatibility contract | [S0 compatibility contract](design/offline-s0-compatibility.md) |
| Deprecations and migration | [Deprecation and removal list](reference/offline-deprecations.md) |
