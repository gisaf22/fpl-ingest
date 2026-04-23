"""Generated test expectations for the compiled public contract."""

from __future__ import annotations

from typing import Any


def generate_test_contracts(schema_version: str, tables: dict[str, object]) -> dict[str, Any]:
    """Build test fixtures derived entirely from the compiled contract."""
    return {
        "schema_version": schema_version,
        "tables": {
            name: {
                "columns": [
                    {
                        "name": column.name,
                        "sqlite_type": column.sqlite_type,
                        "nullable": column.nullable,
                        "primary_key": column.primary_key,
                        "source": column.source,
                    }
                    for column in table.columns
                ],
                "primary_key": list(table.primary_key),
                "unique_constraints": [list(table.unique_key)] if table.unique_key else [],
                "indexes": [list(index) for index in table.indexes],
                "non_nullable_columns": [
                    column.name for column in table.columns if not column.nullable
                ],
            }
            for name, table in sorted(tables.items())
        },
        "nullability_probes": [
            {"table": name, "column": column.name}
            for name, table in sorted(tables.items())
            for column in table.columns
            if not column.nullable and not column.primary_key
        ],
        "uniqueness_probes": [
            {"table": name, "columns": list(table.unique_key)}
            for name, table in sorted(tables.items())
            if table.unique_key
        ],
    }
