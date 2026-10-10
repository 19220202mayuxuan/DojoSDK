from __future__ import annotations

import datetime
from collections.abc import Collection, Mapping
from typing import Any, Iterable

import polars as pl

from dojo.datasource.registry import HFEndpointSpec
from dojo.types.query import ParquetQuery, QueryFilter


class QueryBuilder:
    """Compile legacy request parameters into a typed lazy Parquet plan."""

    def __init__(self, spec: HFEndpointSpec, schema_columns: Iterable[str] | Mapping[str, Any]) -> None:
        self._spec = spec
        self._schema_types = dict(schema_columns) if isinstance(schema_columns, Mapping) else {}
        self._schema = tuple(schema_columns)
        self._schema_set = set(self._schema)

    def build(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        explicit_filters: Iterable[QueryFilter] = (),
        strict_query_validation: bool = False,
        extra_execution_columns: Iterable[str] = (),
        limit_scope: str = "global",
        limit_group_field: str | None = None,
        row_index_param: str | None = None,
    ) -> ParquetQuery:
        normalized = dict(params or {})
        filters: list[QueryFilter] = []

        if self._spec.symbol_field and self._spec.symbol_param in normalized:
            symbol_value = normalized[self._spec.symbol_param]
            if isinstance(symbol_value, str) and "," in symbol_value:
                symbol_value = symbol_value.split(",")
            filters.append(self._membership_or_equality(self._spec.symbol_field, symbol_value))

        ignored = {
            self._spec.limit_param,
            self._spec.offset_param,
            self._spec.start_param,
            self._spec.end_param,
            self._spec.fields_param,
        }
        if self._spec.symbol_field:
            ignored.add(self._spec.symbol_param)
        if row_index_param:
            ignored.add(row_index_param)

        for field, value in normalized.items():
            if field in ignored or value is None:
                continue
            if isinstance(value, dict):
                continue
            if self._collection_contains_mapping(value):
                continue
            if field not in self._schema_set:
                if strict_query_validation:
                    raise ValueError(f"Unknown query field: {field}")
                continue
            filters.append(self._membership_or_equality(field, value))

        if self._spec.time_field and self._spec.time_field in self._schema_set:
            start = normalized.get(self._spec.start_param)
            end = normalized.get(self._spec.end_param)
            if start is not None:
                filters.append(
                    QueryFilter(
                        field=self._spec.time_field,
                        operator="gte",
                        value=self._coerce_value(self._spec.time_field, start),
                    )
                )
            if end is not None:
                filters.append(
                    QueryFilter(
                        field=self._spec.time_field,
                        operator="lte",
                        value=self._coerce_value(self._spec.time_field, end),
                    )
                )

        for query_filter in explicit_filters:
            if query_filter.field not in self._schema_set:
                if strict_query_validation:
                    raise ValueError(f"Unknown query field: {query_filter.field}")
                continue
            filters.append(query_filter)

        output_columns = self._output_columns(normalized)
        sort_fields = self._sort_fields()
        required = set(extra_execution_columns)
        required.update(query_filter.field for query_filter in filters)
        required.update(sort_fields)
        if self._spec.symbol_field:
            required.add(self._spec.symbol_field)
        if self._spec.time_field:
            required.add(self._spec.time_field)
        if limit_group_field:
            required.add(limit_group_field)
        if output_columns is None:
            required.update(self._schema)
        else:
            required.update(output_columns)
        required.update(self._spec.execution_columns)
        execution_columns = tuple(column for column in self._schema if column in required)

        offset_value = normalized.get(self._spec.offset_param)
        offset = offset_value if isinstance(offset_value, int) and offset_value > 0 else 0
        limit_value = normalized.get(self._spec.limit_param)
        limit = limit_value if isinstance(limit_value, int) and limit_value > 0 else None
        row_index_value = normalized.get(row_index_param) if row_index_param else None
        row_index = (
            row_index_value
            if isinstance(row_index_value, int) and not isinstance(row_index_value, bool)
            else None
        )

        if limit_scope not in {"global", "per_group"}:
            raise ValueError(f"Unsupported limit scope: {limit_scope}")
        if limit_scope == "per_group" and not limit_group_field:
            raise ValueError("limit_group_field is required for per_group limits")

        return ParquetQuery(
            filters=tuple(filters),
            output_columns=output_columns,
            execution_columns=execution_columns,
            sort_fields=sort_fields,
            sort_descending=tuple(self._spec.order_desc for _field in sort_fields),
            row_index=row_index,
            offset=offset,
            limit=limit,
            limit_scope=limit_scope,
            limit_group_field=limit_group_field,
            max_materialized_rows=self._spec.max_materialized_rows,
            max_materialized_bytes=self._spec.max_materialized_bytes,
        )

    def _output_columns(self, params: Mapping[str, Any]) -> tuple[str, ...] | None:
        if not self._spec.fields_param or not params.get(self._spec.fields_param):
            return None
        requested = params[self._spec.fields_param]
        try:
            keep = set(requested)
        except TypeError:
            keep = set()
        return tuple(column for column in self._schema if column in keep)

    def _sort_fields(self) -> tuple[str, ...]:
        configured = tuple(field for field in self._spec.sort_fields if field in self._schema_set)
        if configured:
            return configured
        if self._spec.time_field and self._spec.time_field in self._schema_set:
            return (self._spec.time_field,)
        return ()

    def _membership_or_equality(self, field: str, value: Any) -> QueryFilter:
        if isinstance(value, (list, tuple, set, frozenset)):
            return QueryFilter(
                field=field,
                operator="in",
                value=[self._coerce_value(field, item) for item in value],
            )
        return QueryFilter(field=field, operator="eq", value=self._coerce_value(field, value))

    def _coerce_value(self, field: str, value: Any) -> Any:
        """Match ISO request boundaries to temporal Parquet column types."""
        data_type = self._schema_types.get(field)
        base_type = data_type.base_type() if data_type is not None and hasattr(data_type, "base_type") else None
        if data_type == pl.Date:
            if isinstance(value, datetime.datetime):
                return value.date()
            if isinstance(value, str):
                try:
                    return datetime.date.fromisoformat(value[:10])
                except ValueError:
                    return value
        if base_type == pl.Datetime:
            if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                return datetime.datetime.combine(value, datetime.time())
            if isinstance(value, str):
                try:
                    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    return value
        return value

    @staticmethod
    def _collection_contains_mapping(value: Any) -> bool:
        return isinstance(value, Collection) and not isinstance(value, (str, bytes, Mapping)) and any(
            isinstance(item, Mapping) for item in value
        )
