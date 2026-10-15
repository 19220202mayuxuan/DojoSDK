# Reference

This category records stable interfaces, contracts, and lists that
callers can rely on.

- [Offline compatibility removals for the next major release](offline-deprecations.md): interfaces pending removal, replacements, the removal gate, and migration examples.

## MarketResearch write contract (0.1.31)

`create_market_research_result(research_uid, *, body)` appends one result and requires HTML `outcome` plus non-blank Markdown `outcome_markdown`. Missing, empty, null, or whitespace-only Markdown is rejected before sending. Historical responses may still omit Markdown and default to an empty string.

Sync and async appends force `max_retries=0` without changing retry configuration for reads. A 500, timeout, or connection failure may follow a successful write; read back the result and resolve manually before appending again. HTTP error messages support both FastAPI `detail` and qdata `message`; the original response remains available as exception `body`.
