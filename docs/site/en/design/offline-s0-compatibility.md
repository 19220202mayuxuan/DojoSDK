# Offline datasource S0 compatibility contract

This document freezes the legacy offline-query behavior that later Snapshot and
Polars implementations must preserve unless a new strict API explicitly opts out.

## Backend selection

- `DOJO_DATA_BACKEND=huggingface` and `modelscope` are authoritative and never
  trigger a network probe.
- `DOJO_DATA_BACKEND=auto` probes Hugging Face with `httpx` and caches the result
  for `DOJO_BACKEND_PROBE_TTL` seconds.
- `DOJO_DATA_BACKEND_FALLBACK` is used when probing fails. Its default is
  `modelscope`, preserving the previous behavior on restricted networks.
- `DOJO_HF_OFFLINE=true` never probes the network and immediately uses the
  configured fallback.

## Generic filtering

- A JSON body overrides query parameters with the same name.
- A configured `symbol_param` maps to `symbol_field`; comma-separated symbols
  and list/tuple/set values use membership filtering.
- Dictionary values, collections containing dictionaries, and unknown fields
  are ignored by the legacy API.
- Start and end time boundaries are inclusive.
- Filtering and time sorting happen before offset and limit; projection happens
  after them.
- JSON columns are decoded only in final output rows. Date and datetime values
  are serialized with `isoformat()`.

## Stock K-lines

- `get_kline()` retains its existing raw-field behavior.
- `limit` means the latest N rows **for each symbol**, after symbol, interval, and
  date filtering.
- S0 did not freeze the presentation order of those selected rows. The S5
  snapshot-aware API explicitly opts out of any earlier incidental descending
  order: it returns rows ordered by `symbol`, then time ascending. Offline
  `get_kline()` inherits that S5 ordering when it delegates to the snapshot API.
- The old DataFrame cache remains in place during S0 because attribution-factor
  queries still depend on it. It may only be removed after the attribution
  canonical view and bounded small-table cache are implemented and verified.

Strict unknown-parameter validation belongs to a future opt-in API and must not
be enabled for these legacy calls.
