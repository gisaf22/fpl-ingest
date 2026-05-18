"""Schema definition, contract compilation, and integrity validation.

Exports the compiled contract (``compile_contract``) and the artifact path
constants that the CLI and store setup consume.
"""

from fpl_ingest.schema.compiler import (
    DDL_CONTRACT_ARTIFACT_PATH,
    SCHEMA_CONTRACT_ARTIFACT_PATH,
    VALIDATION_CONTRACT_ARTIFACT_PATH,
    CompiledColumn,
    CompiledContract,
    CompiledTable,
    compile_contract,
)
from fpl_ingest.schema.ddl import generate_ddl
from fpl_ingest.schema.test_data import generate_test_contracts
from fpl_ingest.schema.validation import (
    generate_validation_rules,
    validate_contract_db,
)

__all__ = [
    "CompiledColumn",
    "CompiledContract",
    "CompiledTable",
    "SCHEMA_CONTRACT_ARTIFACT_PATH",
    "DDL_CONTRACT_ARTIFACT_PATH",
    "VALIDATION_CONTRACT_ARTIFACT_PATH",
    "compile_contract",
    "generate_ddl",
    "generate_validation_rules",
    "generate_test_contracts",
    "validate_contract_db",
]
