from enum import Enum
from typing import Any
from typing import Optional

from pydantic import BaseModel, Field


class SourceKind(str, Enum):
    auto = "auto"
    polaris = "polaris"
    mysql = "mysql"
    postgresql = "postgresql"
    oracle = "oracle"


class BackupScope(str, Enum):
    table = "table"
    schema = "schema"
    database = "database"


class QueryRequest(BaseModel):
    question: str = Field(min_length=3)
    source_preference: list[SourceKind] = Field(default_factory=lambda: [SourceKind.auto])
    max_rows: int = Field(default=100, ge=1, le=1000)
    include_explain: bool = True


class GeneratedSQL(BaseModel):
    source: SourceKind
    statement: str
    rationale: str


class GuardrailReport(BaseModel):
    approved: bool
    issues: list[str] = Field(default_factory=list)
    normalized_statement: Optional[str] = None


class ExecutionResult(BaseModel):
    source: SourceKind
    execution_summary: str
    rows: list[dict[str, Any]] = Field(default_factory=list)


class QueryResponse(BaseModel):
    mode: str
    model: Optional[str] = None
    selected_sources: list[SourceKind]
    generated_sql: list[GeneratedSQL]
    guardrails: list[GuardrailReport]
    execution_summary: str
    execution_results: list[ExecutionResult] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class HealthResponse(BaseModel):
    status: str
    app: str
    llm_mode: str
    spark_ready: bool


class SourceOverview(BaseModel):
    source: SourceKind
    enabled: bool
    description: str
    tables: list[str] = Field(default_factory=list)


class BackupTableOption(BaseModel):
    logical_name: str
    physical_name: str
    table_name: str
    schema_name: str
    database_name: str


class BackupContainerOption(BaseModel):
    name: str
    tables: list[BackupTableOption] = Field(default_factory=list)


class BackupSourceOption(BaseModel):
    source: SourceKind
    enabled: bool
    database_name: Optional[str] = None
    schema_label: str
    database_label: str
    containers: list[BackupContainerOption] = Field(default_factory=list)


class BackupDiscoveryResponse(BaseModel):
    sources: list[BackupSourceOption] = Field(default_factory=list)


class BackupRequest(BaseModel):
    source: SourceKind
    scope: BackupScope
    targets: list[str] = Field(default_factory=list)
    destination_namespace: str = Field(default="backups", min_length=1)
    overwrite: bool = True


class BackupTableResult(BaseModel):
    source_table: str
    destination_table: str
    row_count: int = 0
    status: str
    message: str


class BackupResponse(BaseModel):
    source: SourceKind
    scope: BackupScope
    execution_summary: str
    destination_namespace: str
    copied_tables: list[BackupTableResult] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
