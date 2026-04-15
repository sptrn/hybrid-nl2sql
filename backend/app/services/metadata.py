import json
from pathlib import Path
from typing import Optional

from app.core.config import Settings
from app.models.schemas import SourceKind, SourceOverview
from app.services.connectors import JDBCSourceConfig
from app.services.spark import SparkManager


class CatalogMetadataService:
    def __init__(
        self,
        settings: Settings,
        spark_manager: SparkManager,
        metadata_path: Optional[Path] = None,
    ) -> None:
        self.settings = settings
        self.spark_manager = spark_manager
        default_path = Path(__file__).resolve().parent.parent / "static" / "example_catalog.json"
        self.metadata_path = metadata_path or default_path

    def load(self) -> dict:
        payload = {"sources": {}}
        if self.metadata_path.exists():
            payload = json.loads(self.metadata_path.read_text(encoding="utf-8"))

        sources = payload.setdefault("sources", {})
        live_sources = {
            SourceKind.polaris.value: self.spark_manager.introspect_polaris_metadata(),
            SourceKind.mysql.value: self.spark_manager.introspect_mysql_metadata(),
            SourceKind.postgresql.value: self.spark_manager.introspect_postgresql_metadata(),
        }
        for source_name, live_tables in live_sources.items():
            if live_tables:
                sources[source_name] = live_tables

        return payload

    def prompt_context(self) -> str:
        payload = self.load()
        fragments: list[str] = []
        for source_name, tables in payload.get("sources", {}).items():
            fragments.append(f"Source: {source_name}")
            for table in tables:
                columns = ", ".join(
                    f"{col['name']} ({col['type']})" for col in table.get("columns", [])
                )
                fragments.append(
                    f"- Table {table['name']}: {table.get('description', 'No description')}. "
                    f"Columns: {columns}"
                )
        return "\n".join(fragments)

    def source_overview(
        self,
        polaris_enabled: bool,
        jdbc_sources: list[JDBCSourceConfig],
    ) -> list[SourceOverview]:
        payload = self.load().get("sources", {})
        overviews = [
            SourceOverview(
                source=SourceKind.polaris,
                enabled=polaris_enabled,
                description="Iceberg catalog accessed through Polaris.",
                tables=[table["name"] for table in payload.get(SourceKind.polaris.value, [])],
            )
        ]

        for source in jdbc_sources:
            overviews.append(
                SourceOverview(
                    source=source.source,
                    enabled=source.enabled,
                    description=source.description,
                    tables=[table["name"] for table in payload.get(source.source.value, [])],
                )
            )

        return overviews
