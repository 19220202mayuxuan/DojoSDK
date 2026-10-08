from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class QueryFilter(BaseModel):
    """A schema-validated predicate in a Parquet query plan."""

    model_config = ConfigDict(frozen=True)

    field: str
    operator: Literal["eq", "in", "gte", "lte"]
    value: Any


class ParquetQuery(BaseModel):
    """Immutable query plan consumed by the Polars lazy executor."""

    model_config = ConfigDict(frozen=True)

    filters: tuple[QueryFilter, ...] = ()
    output_columns: tuple[str, ...] | None = None
    execution_columns: tuple[str, ...]
    sort_fields: tuple[str, ...] = ()
    sort_descending: tuple[bool, ...] = ()
    row_index: int | None = None
    offset: int = 0
    limit: int | None = None
    limit_scope: Literal["global", "per_group"] = "global"
    limit_group_field: str | None = None
    max_materialized_rows: int = Field(gt=0)
