from functools import lru_cache

from fastapi import APIRouter

from app.core.config import Settings, get_settings
from app.models.schemas import (
    BackupDiscoveryResponse,
    BackupRequest,
    BackupResponse,
    ExecutionResult,
    HealthResponse,
    QueryRequest,
    QueryResponse,
)
from app.services.agent import NL2SQLAgentService
from app.services.backup import IcebergBackupService
from app.services.connectors import get_jdbc_sources
from app.services.metadata import CatalogMetadataService
from app.services.spark import SparkManager
from app.services.sql_guardrails import SQLGuardrails

router = APIRouter()


@lru_cache
def get_metadata_service() -> CatalogMetadataService:
    settings = get_settings()
    return CatalogMetadataService(settings=settings, spark_manager=get_spark_manager())


@lru_cache
def get_agent_service() -> NL2SQLAgentService:
    settings = get_settings()
    return NL2SQLAgentService(settings=settings, metadata_service=get_metadata_service())


@lru_cache
def get_spark_manager() -> SparkManager:
    return SparkManager(get_settings())


@lru_cache
def get_guardrails() -> SQLGuardrails:
    return SQLGuardrails()


@lru_cache
def get_backup_service() -> IcebergBackupService:
    settings = get_settings()
    return IcebergBackupService(settings=settings, spark_manager=get_spark_manager())


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    agent = get_agent_service()
    spark = get_spark_manager()
    return HealthResponse(
        status="ok",
        app=settings.app_name,
        llm_mode=agent.mode,
        spark_ready=spark.is_ready(),
    )


@router.get("/sources")
def sources() -> dict:
    settings: Settings = get_settings()
    metadata_service = get_metadata_service()
    jdbc_sources = get_jdbc_sources(settings)

    return {
        "configured": get_spark_manager().configured_sources(),
        "metadata": metadata_service.source_overview(
            polaris_enabled=settings.polaris_enabled,
            jdbc_sources=jdbc_sources,
        ),
    }


@router.get("/backup/options", response_model=BackupDiscoveryResponse)
def backup_options() -> BackupDiscoveryResponse:
    return get_backup_service().discover()


@router.post("/backup/run", response_model=BackupResponse)
def run_backup(request: BackupRequest) -> BackupResponse:
    return get_backup_service().execute(request)


@router.post("/query", response_model=QueryResponse)
def query(request: QueryRequest) -> QueryResponse:
    agent = get_agent_service()
    spark = get_spark_manager()
    guardrails = get_guardrails()

    selected_sources, generated_sql, model, llm_generation_error = agent.generate(request)
    unavailable_sources = [
        source
        for source in selected_sources
        if not spark.is_source_enabled(source)
    ]

    reports = [
        guardrails.validate(
            statement=query.statement,
            max_rows=request.max_rows,
            source=query.source,
        )
        for query in generated_sql
    ]

    rows: list[dict] = []
    execution_results: list[ExecutionResult] = []
    if not generated_sql:
        source_labels = ", ".join(source.value for source in unavailable_sources or selected_sources)
        execution_summary = (
            f"No SQL generated because the requested source is not configured: {source_labels}."
        )
    else:
        execution_summary = "Generated SQL only."

    summaries: list[str] = []
    for query, report in zip(generated_sql, reports):
        if not report.approved or not report.normalized_statement:
            execution_results.append(
                ExecutionResult(
                    source=query.source,
                    execution_summary="Query was not executed because guardrails did not approve it.",
                    rows=[],
                )
            )
            continue

        query.statement = report.normalized_statement or query.statement
        result_rows, result_summary = spark.execute(query, request.max_rows)
        execution_results.append(
            ExecutionResult(
                source=query.source,
                execution_summary=result_summary,
                rows=result_rows,
            )
        )
        summaries.append(f"{query.source.value}: {result_summary}")

    if execution_results:
        rows = execution_results[0].rows
        execution_summary = " | ".join(summaries) if summaries else execution_summary

    return QueryResponse(
        mode=agent.mode,
        model=model,
        selected_sources=selected_sources,
        generated_sql=generated_sql,
        guardrails=reports,
        execution_summary=execution_summary,
        execution_results=execution_results,
        rows=rows,
        metadata={
            "source_count": len(selected_sources),
            "spark_sources": spark.configured_sources(),
            "unavailable_sources": [source.value for source in unavailable_sources],
            "llm_generation_error": llm_generation_error,
        },
    )
