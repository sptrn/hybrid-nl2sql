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


class QueryResponse(BaseModel):
    mode: str
    model: Optional[str] = None
    selected_sources: list[SourceKind]
    generated_sql: list[GeneratedSQL]
    guardrails: list[GuardrailReport]
    execution_summary: str
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
