# Reference

This category records stable interfaces, contracts, and lists that
callers can rely on.

- [Offline compatibility removals for the next major release](offline-deprecations.md): interfaces pending removal, replacements, the removal gate, and migration examples.

## MarketResearch write contract (0.1.31)

`create_market_research_result(research_uid, *, body)` appends one result and requires HTML `outcome` plus non-blank Markdown `outcome_markdown`. Missing, empty, null, or whitespace-only Markdown is rejected before sending. Historical responses may still omit Markdown and default to an empty string.

Sync and async appends force `max_retries=0` without changing retry configuration for reads. A 500, timeout, or connection failure may follow a successful write; read back the result and resolve manually before appending again. HTTP error messages support both FastAPI `detail` and qdata `message`; the original response remains available as exception `body`.

## MarketResearch result content contract (0.1.32)

Result snapshots accept an optional `thumbnail_base64` field containing a Base64-encoded thumbnail cover image. This applies to `initial_result` during research creation, result appends, and result corrections.

Research lists, `public_research` lists, research detail, and result history queries all accept `result_content_fields`. It is a comma-separated selection of `outcome`, `outcome_markdown`, and `thumbnail_base64`, for example `result_content_fields="outcome,thumbnail_base64"`. Unselected content fields are returned as `None`; omitting the parameter returns all three fields. The server applies the same selection to `producer_payload`.
