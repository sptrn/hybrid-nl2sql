from collections import defaultdict
from typing import Any

from app.core.config import Settings
from app.models.schemas import GeneratedSQL, SourceKind
from app.services.connectors import get_jdbc_sources


class SparkManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._spark = None

    def is_ready(self) -> bool:
        try:
            return self.session is not None
        except Exception:
            return False

    @property
    def session(self):
        if self._spark is None:
            self._spark = self._build_session()
        return self._spark

    def _build_session(self):
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            return None

        builder = SparkSession.builder.appName(self.settings.spark_app_name).master(
            self.settings.spark_master
        )

        if self.settings.spark_jars_packages:
            builder = builder.config("spark.jars.packages", self.settings.spark_jars_packages)

        if self._iceberg_enabled:
            builder = builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )

        if self.settings.polaris_uri:
            prefix = f"spark.sql.catalog.{self.settings.polaris_catalog_name}"
            builder = (
                builder.config(prefix, "org.apache.iceberg.spark.SparkCatalog")
                .config(f"{prefix}.type", "rest")
                .config(f"{prefix}.uri", self.settings.polaris_uri)
            )
            if self.settings.polaris_warehouse:
                builder = builder.config(f"{prefix}.warehouse", self.settings.polaris_warehouse)
            if self.settings.polaris_scope:
                builder = builder.config(f"{prefix}.scope", self.settings.polaris_scope)

        return builder.getOrCreate()

    @property
    def _iceberg_enabled(self) -> bool:
        packages = [pkg.strip().lower() for pkg in self.settings.spark_jars_packages.split(",") if pkg.strip()]
        return any("iceberg-spark-runtime" in pkg or pkg.startswith("org.apache.iceberg:") for pkg in packages)

    def configured_sources(self) -> list[dict[str, Any]]:
        jdbc_sources = get_jdbc_sources(self.settings)
        return [
            {
                "source": SourceKind.polaris.value,
                "enabled": bool(self.settings.polaris_uri),
                "via": "iceberg-rest-catalog",
            },
            *[
                {
                    "source": source.source.value,
                    "enabled": source.enabled,
                    "via": "jdbc",
                }
                for source in jdbc_sources
            ],
        ]

    def introspect_polaris_metadata(self) -> list[dict[str, Any]]:
        spark = self.session
        if spark is None or not self.settings.polaris_uri:
            return []

        catalog = self._quote_identifier(self.settings.polaris_catalog_name)
        try:
            namespaces = self._list_namespaces_recursive(catalog)
        except Exception:
            return []

        tables: list[dict[str, Any]] = []
        for namespace in namespaces[: self.settings.metadata_max_namespaces]:
            namespace_ref = ".".join(
                [
                    self._quote_identifier(self.settings.polaris_catalog_name),
                    *[self._quote_identifier(part) for part in namespace],
                ]
            )
            try:
                table_rows = spark.sql(f"SHOW TABLES IN {namespace_ref}").collect()
            except Exception:
                continue

            for row in table_rows:
                row_dict = row.asDict(recursive=True)
                table_name = row_dict.get("tableName") or row_dict.get("tableName".lower())
                if not table_name:
                    values = list(row_dict.values())
                    table_name = values[1] if len(values) > 1 else values[0]
                if not table_name:
                    continue

                full_name = ".".join(
                    [self.settings.polaris_catalog_name, *namespace, str(table_name)]
                )
                columns = self._describe_table_columns(full_name)
                tables.append(
                    {
                        "name": ".".join([*namespace, str(table_name)]),
                        "description": "Live metadata from Polaris via Spark catalog inspection.",
                        "columns": columns,
                    }
                )
                if len(tables) >= self.settings.metadata_max_tables_per_source:
                    return tables

        return tables

    def introspect_postgresql_metadata(self) -> list[dict[str, Any]]:
        postgres = next(
            (source for source in get_jdbc_sources(self.settings) if source.source == SourceKind.postgresql),
            None,
        )
        if postgres is None or not postgres.enabled:
            return []

        schema_list = self.settings.postgres_metadata_schema_list or ["public"]
        return self._introspect_jdbc_information_schema(
            jdbc_url=postgres.jdbc_url or "",
            user=postgres.username or "",
            password=postgres.password or "",
            driver=postgres.driver,
            schema_list=schema_list,
            source_label="PostgreSQL",
            query_alias_prefix="postgres",
        )

    def introspect_mysql_metadata(self) -> list[dict[str, Any]]:
        mysql = next(
            (source for source in get_jdbc_sources(self.settings) if source.source == SourceKind.mysql),
            None,
        )
        if mysql is None or not mysql.enabled:
            return []

        schema_list = self.settings.mysql_metadata_schema_list
        if not schema_list:
            schema_list = [self._extract_database_name_from_jdbc_url(mysql.jdbc_url or "")]
        schema_list = [schema for schema in schema_list if schema]
        if not schema_list:
            return []

        return self._introspect_jdbc_information_schema(
            jdbc_url=mysql.jdbc_url or "",
            user=mysql.username or "",
            password=mysql.password or "",
            driver=mysql.driver,
            schema_list=schema_list,
            source_label="MySQL",
            query_alias_prefix="mysql",
        )

    def _introspect_jdbc_information_schema(
        self,
        jdbc_url: str,
        user: str,
        password: str,
        driver: str,
        schema_list: list[str],
        source_label: str,
        query_alias_prefix: str,
    ) -> list[dict[str, Any]]:
        spark = self.session
        if spark is None:
            return []

        schema_filter = ", ".join(self._sql_literal(schema) for schema in schema_list)
        tables_query = f"""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema IN ({schema_filter})
              AND table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY table_schema, table_name
        """
        columns_query = f"""
            SELECT
                table_schema,
                table_name,
                column_name,
                data_type,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema IN ({schema_filter})
            ORDER BY table_schema, table_name, ordinal_position
        """

        try:
            table_rows = self._read_jdbc_query(
                jdbc_url=jdbc_url,
                query=tables_query,
                user=user,
                password=password,
                driver=driver,
            ).collect()
            column_rows = self._read_jdbc_query(
                jdbc_url=jdbc_url,
                query=columns_query,
                user=user,
                password=password,
                driver=driver,
            ).collect()
        except Exception:
            return []

        columns_by_table: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in column_rows:
            row_dict = self._normalize_row_keys(row.asDict(recursive=True))
            columns_by_table[(str(row_dict["table_schema"]), str(row_dict["table_name"]))].append(
                {
                    "name": str(row_dict["column_name"]),
                    "type": str(row_dict["data_type"]),
                }
            )

        tables: list[dict[str, Any]] = []
        for row in table_rows[: self.settings.metadata_max_tables_per_source]:
            row_dict = self._normalize_row_keys(row.asDict(recursive=True))
            schema = str(row_dict["table_schema"])
            table_name = str(row_dict["table_name"])
            table_type = str(row_dict["table_type"])
            tables.append(
                {
                    "name": f"{schema}.{table_name}",
                    "description": f"Live {source_label} {table_type.lower()} metadata from information_schema.",
                    "columns": columns_by_table.get((schema, table_name), []),
                }
            )

        return tables

    def execute(self, generated_sql: GeneratedSQL, max_rows: int) -> tuple[list[dict[str, Any]], str]:
        spark = self.session
        if spark is None:
            return (
                [],
                "Spark is not available in this environment yet. Returning generated SQL only.",
            )

        statement = generated_sql.statement.rstrip(";")
        if "limit" not in statement.lower():
            statement = f"{statement}\nLIMIT {max_rows}"

        try:
            dataframe = spark.sql(statement)
            rows = [row.asDict(recursive=True) for row in dataframe.limit(max_rows).collect()]
            return rows, f"Executed against Spark for source '{generated_sql.source.value}'."
        except Exception as exc:
            return [], f"Spark execution skipped or failed: {exc}"

    def _list_namespaces_recursive(self, catalog: str) -> list[list[str]]:
        spark = self.session
        if spark is None:
            return []

        discovered: list[list[str]] = []
        queue: list[list[str]] = [[]]
        seen: set[tuple[str, ...]] = set()

        while queue:
            namespace = queue.pop(0)
            namespace_key = tuple(namespace)
            if namespace_key in seen:
                continue
            seen.add(namespace_key)

            target = ".".join([catalog, *[self._quote_identifier(part) for part in namespace]])
            command = f"SHOW NAMESPACES IN {target}"
            try:
                rows = spark.sql(command).collect()
            except Exception:
                continue

            for row in rows:
                row_dict = row.asDict(recursive=True)
                values = list(row_dict.values())
                raw_value = values[0] if values else None
                if raw_value is None:
                    continue
                if isinstance(raw_value, (list, tuple)):
                    child_parts = [str(part) for part in raw_value]
                else:
                    child_parts = [part for part in str(raw_value).split(".") if part]

                if namespace and child_parts[: len(namespace)] == namespace:
                    child_namespace = child_parts
                else:
                    child_namespace = [*namespace, *child_parts]

                if child_namespace and child_namespace not in discovered:
                    discovered.append(child_namespace)
                    queue.append(child_namespace)

        return discovered

    def _describe_table_columns(self, table_name: str) -> list[dict[str, Any]]:
        spark = self.session
        if spark is None:
            return []

        identifier = ".".join(self._quote_identifier(part) for part in table_name.split("."))
        try:
            rows = spark.sql(f"DESCRIBE TABLE {identifier}").collect()
        except Exception:
            return []

        columns: list[dict[str, Any]] = []
        for row in rows:
            row_dict = row.asDict(recursive=True)
            column_name = (row_dict.get("col_name") or "").strip()
            if not column_name or column_name.startswith("#"):
                break
            columns.append(
                {
                    "name": column_name,
                    "type": row_dict.get("data_type", "unknown"),
                }
            )
        return columns

    def _read_jdbc_query(
        self,
        jdbc_url: str,
        query: str,
        user: str,
        password: str,
        driver: str,
    ):
        spark = self.session
        if spark is None:
            raise RuntimeError("Spark session is unavailable.")

        return (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)
            .option("user", user)
            .option("password", password)
            .option("driver", driver)
            .load()
        )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return f"`{identifier.replace('`', '``')}`"

    @staticmethod
    def _sql_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def _extract_database_name_from_jdbc_url(jdbc_url: str) -> str:
        prefix, _, remainder = jdbc_url.partition("://")
        if not prefix or not remainder:
            return ""

        path = remainder.split("/", 1)
        if len(path) < 2:
            return ""

        database = path[1].split("?", 1)[0].strip()
        return database

    @staticmethod
    def _normalize_row_keys(row_dict: dict[str, Any]) -> dict[str, Any]:
        return {str(key).lower(): value for key, value in row_dict.items()}
