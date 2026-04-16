from __future__ import annotations

from collections import defaultdict
from uuid import uuid4

from app.core.config import Settings
from app.models.schemas import (
    BackupContainerOption,
    BackupDiscoveryResponse,
    BackupRequest,
    BackupResponse,
    BackupScope,
    BackupSourceOption,
    BackupTableOption,
    BackupTableResult,
    SourceKind,
)
from app.services.spark import SparkManager


class IcebergBackupService:
    def __init__(self, settings: Settings, spark_manager: SparkManager) -> None:
        self.settings = settings
        self.spark_manager = spark_manager

    def discover(self) -> BackupDiscoveryResponse:
        return BackupDiscoveryResponse(
            sources=[
                self._source_option(SourceKind.mysql),
                self._source_option(SourceKind.postgresql),
            ]
        )

    def execute(self, request: BackupRequest) -> BackupResponse:
        source = request.source
        if source not in {SourceKind.mysql, SourceKind.postgresql}:
            return BackupResponse(
                source=source,
                scope=request.scope,
                execution_summary="Only MySQL and PostgreSQL backups are supported in this workflow.",
                destination_namespace=request.destination_namespace,
                metadata={"copied_count": 0, "total_rows": 0},
            )

        if not self.settings.polaris_enabled:
            return BackupResponse(
                source=source,
                scope=request.scope,
                execution_summary="Polaris is not configured, so Iceberg backups cannot be created yet.",
                destination_namespace=request.destination_namespace,
                metadata={"copied_count": 0, "total_rows": 0},
            )

        jdbc_source = self.spark_manager.get_jdbc_source(source)
        if jdbc_source is None or not jdbc_source.enabled:
            return BackupResponse(
                source=source,
                scope=request.scope,
                execution_summary=f"Source '{source.value}' is not configured for JDBC backup.",
                destination_namespace=request.destination_namespace,
                metadata={"copied_count": 0, "total_rows": 0},
            )

        all_tables = self._list_tables(source)
        selected_tables = self._resolve_targets(all_tables, request)
        if not selected_tables:
            return BackupResponse(
                source=source,
                scope=request.scope,
                execution_summary="No matching source tables were selected for backup.",
                destination_namespace=request.destination_namespace,
                metadata={"copied_count": 0, "total_rows": 0},
            )

        namespace_prefix = [
            segment.strip()
            for segment in request.destination_namespace.split(".")
            if segment.strip()
        ]
        if not namespace_prefix:
            namespace_prefix = ["backups"]

        results: list[BackupTableResult] = []
        total_rows = 0
        for table in selected_tables:
            destination_parts = self._destination_parts(source, table, namespace_prefix)
            destination_table = ".".join([self.settings.polaris_catalog_name, *destination_parts])
            try:
                row_count = self.spark_manager.backup_jdbc_table_to_polaris(
                    source=source,
                    physical_table_name=table.physical_name,
                    destination_parts=destination_parts,
                    overwrite=request.overwrite,
                )
                results.append(
                    BackupTableResult(
                        source_table=table.physical_name,
                        destination_table=destination_table,
                        row_count=row_count,
                        status="copied",
                        message="Iceberg table materialized in Polaris.",
                    )
                )
                total_rows += row_count
            except Exception as exc:
                results.append(
                    BackupTableResult(
                        source_table=table.physical_name,
                        destination_table=destination_table,
                        row_count=0,
                        status="failed",
                        message=str(exc),
                    )
                )

        copied_count = sum(1 for result in results if result.status == "copied")
        summary = (
            f"Backed up {copied_count} of {len(results)} selected tables from "
            f"{source.value} into Polaris/Iceberg."
        )
        return BackupResponse(
            source=source,
            scope=request.scope,
            execution_summary=summary,
            destination_namespace=".".join(namespace_prefix),
            copied_tables=results,
            metadata={
                "copied_count": copied_count,
                "failed_count": len(results) - copied_count,
                "selected_count": len(selected_tables),
                "total_rows": total_rows,
                "overwrite": request.overwrite,
            },
        )

    def _source_option(self, source: SourceKind) -> BackupSourceOption:
        jdbc_source = self.spark_manager.get_jdbc_source(source)
        database_name = self.spark_manager.database_name_for_source(source)
        grouped: dict[str, list[BackupTableOption]] = defaultdict(list)
        for table in self._list_tables(source):
            grouped[table.schema_name].append(table)

        containers = [
            BackupContainerOption(
                name=name,
                tables=sorted(tables, key=lambda item: item.table_name),
            )
            for name, tables in sorted(grouped.items())
        ]
        return BackupSourceOption(
            source=source,
            enabled=bool(jdbc_source and jdbc_source.enabled),
            database_name=database_name,
            schema_label="schema",
            database_label="database",
            containers=containers,
        )

    def _list_tables(self, source: SourceKind) -> list[BackupTableOption]:
        if source == SourceKind.mysql:
            metadata_tables = self.spark_manager.introspect_mysql_metadata()
            default_database = self.spark_manager.database_name_for_source(source) or "mysql"
        elif source == SourceKind.postgresql:
            metadata_tables = self.spark_manager.introspect_postgresql_metadata()
            default_database = self.spark_manager.database_name_for_source(source) or "postgresql"
        else:
            return []

        tables: list[BackupTableOption] = []
        for table in metadata_tables:
            logical_name = str(table.get("name", "")).strip()
            physical_name = str(table.get("physical_name", "")).strip()
            if not logical_name or not physical_name:
                continue

            schema_name, table_name = self._split_physical_name(physical_name)
            tables.append(
                BackupTableOption(
                    logical_name=logical_name,
                    physical_name=physical_name,
                    table_name=table_name,
                    schema_name=schema_name,
                    database_name=default_database,
                )
            )

        return tables

    def _resolve_targets(
        self, all_tables: list[BackupTableOption], request: BackupRequest
    ) -> list[BackupTableOption]:
        if request.scope == BackupScope.database:
            if not request.targets:
                return all_tables
            target_set = {target.strip() for target in request.targets if target.strip()}
            return [table for table in all_tables if table.database_name in target_set]

        if request.scope == BackupScope.schema:
            target_set = {target.strip() for target in request.targets if target.strip()}
            return [table for table in all_tables if table.schema_name in target_set]

        target_set = {target.strip() for target in request.targets if target.strip()}
        return [
            table
            for table in all_tables
            if table.logical_name in target_set
            or table.physical_name in target_set
            or table.table_name in target_set
        ]

    def _destination_parts(
        self,
        source: SourceKind,
        table: BackupTableOption,
        namespace_prefix: list[str],
    ) -> list[str]:
        parts = [*namespace_prefix, source.value, table.database_name]
        if table.schema_name and table.schema_name != table.database_name:
            parts.append(table.schema_name)
        parts.append(table.table_name)
        return parts

    @staticmethod
    def _split_physical_name(physical_name: str) -> tuple[str, str]:
        parts = [segment for segment in physical_name.split(".") if segment]
        if len(parts) >= 2:
            return parts[-2], parts[-1]
        return "default", parts[-1] if parts else "unknown_table"
