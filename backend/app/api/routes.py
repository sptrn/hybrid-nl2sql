from functools import lru_cache

from fastapi import APIRouter

from app.core.config import Settings, get_settings
from app.models.schemas import HealthResponse, QueryRequest, QueryResponse
from app.services.agent import NL2SQLAgentService
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
            polaris_enabled=bool(settings.polaris_uri),
            jdbc_sources=jdbc_sources,
        ),
    }


@router.post("/query", response_model=QueryResponse)
def query(request: QueryRequest) -> QueryResponse:
    agent = get_agent_service()
    spark = get_spark_manager()
    guardrails = get_guardrails()

    selected_sources, generated_sql, model = agent.generate(request)

    reports = [
        guardrails.validate(statement=query.statement, max_rows=request.max_rows)
        for query in generated_sql
    ]

    rows: list[dict] = []
    execution_summary = "Generated SQL only."
    approved_query = next(
        (
            query
            for query, report in zip(generated_sql, reports)
            if report.approved and report.normalized_statement
        ),
        None,
    )
    approved_report = next((report for report in reports if report.approved), None)

    if approved_query and approved_report:
        approved_query.statement = approved_report.normalized_statement or approved_query.statement
        rows, execution_summary = spark.execute(approved_query, request.max_rows)

    return QueryResponse(
        mode=agent.mode,
        model=model,
        selected_sources=selected_sources,
        generated_sql=generated_sql,
        guardrails=reports,
        execution_summary=execution_summary,
        rows=rows,
        metadata={
            "source_count": len(selected_sources),
            "spark_sources": spark.configured_sources(),
        },
    )
