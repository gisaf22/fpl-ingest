"""Compile the public schema contract into DB, validation, and test projections."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence, Type, get_args, get_origin

from pydantic import BaseModel

from fpl_ingest.domain.models import PYTHON_TO_SQLITE


SCHEMA_CONTRACT_ARTIFACT_PATH = Path(__file__).resolve().parents[3] / "artifacts" / "contract" / "schema_contract.json"
DDL_CONTRACT_ARTIFACT_PATH = Path(__file__).resolve().parents[3] / "artifacts" / "contract" / "ddl_contract.sql"
VALIDATION_CONTRACT_ARTIFACT_PATH = Path(__file__).resolve().parents[3] / "artifacts" / "contract" / "validation_contract.json"


@dataclass(frozen=True)
class CompiledColumn:
    name: str
    sqlite_type: str
    nullable: bool
    primary_key: bool = False
    source: str = "model"

    @property
    def column_sql(self) -> str:
        parts = [self.name, self.sqlite_type]
        if not self.nullable:
            parts.append("NOT NULL")
        if self.primary_key:
            parts.append("PRIMARY KEY")
        return " ".join(parts)

    @property
    def alter_sql(self) -> str:
        if self.primary_key:
            raise ValueError("PRIMARY KEY columns cannot be added with ALTER TABLE")
        if not self.nullable:
            raise ValueError("NOT NULL columns require a manual migration")
        return f"{self.name} {self.sqlite_type}"


@dataclass(frozen=True)
class CompiledTable:
    name: str
    description: str
    grain: str
    columns: tuple[CompiledColumn, ...]
    unique_key: tuple[str, ...]
    indexes: tuple[tuple[str, ...], ...]
    field_notes: dict[str, str]
    system_columns: tuple[str, ...]
    create_table_sql: str
    index_sql: tuple[str, ...]

    @property
    def primary_key(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.primary_key)


@dataclass(frozen=True)
class CompiledContract:
    schema_version: str
    tables: dict[str, CompiledTable]
    schema_contract: dict[str, Any]
    validation_contract: dict[str, Any]
    test_contracts: dict[str, Any]
    ddl_contract: str


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    origin = get_origin(annotation)
    if origin is None:
        return annotation, False

    args = get_args(annotation)
    if type(None) in args:
        non_none = next(arg for arg in args if arg is not type(None))
        return non_none, True
    return annotation, False


def _sqlite_type_for_annotation(annotation: Any) -> str:
    unwrapped, _ = _unwrap_optional(annotation)
    return PYTHON_TO_SQLITE.get(unwrapped, "TEXT")


def _compile_model_columns(model: Type[BaseModel]) -> list[CompiledColumn]:
    columns: list[CompiledColumn] = []
    for field_name, field_info in model.model_fields.items():
        _, nullable = _unwrap_optional(field_info.annotation)
        columns.append(
            CompiledColumn(
                name=field_name,
                sqlite_type=_sqlite_type_for_annotation(field_info.annotation),
                nullable=nullable,
                primary_key=field_name == "id",
            )
        )
    return columns


def _compile_system_columns(names: Sequence[str]) -> list[CompiledColumn]:
    return [
        CompiledColumn(
            name=name,
            sqlite_type="TEXT",
            nullable=True,
            source="system",
        )
        for name in names
    ]


def _render_create_table_sql(
    table_name: str,
    columns: Sequence[CompiledColumn],
    unique_key: Sequence[str],
) -> str:
    statements = [column.column_sql for column in columns]
    if unique_key:
        statements.append(f"UNIQUE({', '.join(unique_key)})")
    joined = ",\n    ".join(statements)
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {joined}\n);"


def _render_index_sql(table_name: str, indexes: Sequence[Sequence[str]]) -> tuple[str, ...]:
    statements: list[str] = []
    for columns in indexes:
        index_name = f"idx_{table_name}_{'_'.join(columns)}"
        statements.append(
            f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({', '.join(columns)});"
        )
    return tuple(statements)


def _build_schema_contract(schema_version: str, tables: dict[str, CompiledTable]) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "tables": {
            name: {
                "description": table.description,
                "grain": table.grain,
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
                "unique_key": list(table.unique_key),
                "indexes": [list(index) for index in table.indexes],
                "field_notes": dict(sorted(table.field_notes.items())),
            }
            for name, table in sorted(tables.items())
        },
    }


def write_contract_artifact(payload: dict[str, Any], destination: Path) -> Path:
    """Write a JSON contract artifact to disk."""
    resolved = destination.resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return resolved


@lru_cache(maxsize=1)
def compile_contract() -> CompiledContract:
    """Compile the public schema metadata into enforcement artifacts."""
    from fpl_ingest.contract.ddl_generator import generate_ddl
    from fpl_ingest.contract.test_contracts import generate_test_contracts
    from fpl_ingest.contract.validation_rules import generate_validation_rules
    from fpl_ingest.domain.schema import PUBLIC_TABLES, SCHEMA_VERSION

    tables: dict[str, CompiledTable] = {}
    for table in PUBLIC_TABLES:
        columns = tuple(_compile_model_columns(table.model) + _compile_system_columns(table.system_columns))
        create_table_sql = _render_create_table_sql(table.name, columns, table.unique_key)
        index_sql = _render_index_sql(table.name, table.indexes)
        tables[table.name] = CompiledTable(
            name=table.name,
            description=table.description,
            grain=table.grain,
            columns=columns,
            unique_key=table.unique_key,
            indexes=table.indexes,
            field_notes=dict(table.field_notes),
            system_columns=table.system_columns,
            create_table_sql=create_table_sql,
            index_sql=index_sql,
        )

    schema_contract = _build_schema_contract(SCHEMA_VERSION, tables)
    validation_contract = generate_validation_rules(SCHEMA_VERSION, tables)
    test_contracts = generate_test_contracts(SCHEMA_VERSION, tables)
    ddl_contract = generate_ddl(SCHEMA_VERSION, tables.values())
    return CompiledContract(
        schema_version=SCHEMA_VERSION,
        tables=tables,
        schema_contract=schema_contract,
        validation_contract=validation_contract,
        test_contracts=test_contracts,
        ddl_contract=ddl_contract,
    )
