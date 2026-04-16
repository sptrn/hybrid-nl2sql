import re
from collections import defaultdict
from typing import Any
from typing import Optional
from uuid import uuid4

from app.core.config import Settings
from app.models.schemas import GeneratedSQL, SourceKind
from app.services.connectors import JDBCSourceConfig, get_jdbc_sources


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

        effective_packages = self.settings.spark_packages
        if effective_packages:
            builder = builder.config("spark.jars.packages", ",".join(effective_packages))

        if self._iceberg_enabled:
            builder = builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )

        for key, value in self._polaris_catalog_configs().items():
            builder = builder.config(key, value)

        return builder.getOrCreate()

    @property
    def _iceberg_enabled(self) -> bool:
        packages = [pkg.strip().lower() for pkg in self.settings.spark_packages if pkg.strip()]
        return any("iceberg-spark-runtime" in pkg for pkg in packages)

    def configured_sources(self) -> list[dict[str, Any]]:
        jdbc_sources = get_jdbc_sources(self.settings)
        return [
            {
                "source": SourceKind.polaris.value,
                "enabled": self.settings.polaris_enabled,
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

    def enabled_sources(self) -> set[SourceKind]:
        jdbc_sources = get_jdbc_sources(self.settings)
        enabled = {
            source.source
            for source in jdbc_sources
            if source.enabled
        }
        if self.settings.polaris_enabled:
            enabled.add(SourceKind.polaris)
        return enabled

    def is_source_enabled(self, source: SourceKind) -> bool:
        return source in self.enabled_sources()

    def get_jdbc_source(self, source: SourceKind) -> Optional[JDBCSourceConfig]:
        return next((cfg for cfg in get_jdbc_sources(self.settings) if cfg.source == source), None)

    def database_name_for_source(self, source: SourceKind) -> Optional[str]:
        jdbc_source = self.get_jdbc_source(source)
        if jdbc_source is None or not jdbc_source.jdbc_url:
            return None
        return self._extract_database_name_from_jdbc_url(jdbc_source.jdbc_url)

    def introspect_polaris_metadata(self) -> list[dict[str, Any]]:
        try:
            spark = self.session
        except Exception:
            return []
        if spark is None or not self.settings.polaris_enabled:
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
                description = "Live metadata from Polaris via Spark catalog inspection."
                if len(namespace) >= 4 and namespace[0] == "backups":
                    source_name = namespace[1]
                    database_name = namespace[2]
                    schema_name = namespace[3]
                    description = (
                        "Iceberg backup table materialized into Polaris from "
                        f"{source_name} database {database_name}, schema {schema_name}."
                    )
                tables.append(
                    {
                        "name": full_name,
                        "description": description,
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
        try:
            return self._introspect_jdbc_information_schema(
                jdbc_url=postgres.jdbc_url or "",
                user=postgres.username or "",
                password=postgres.password or "",
                driver=postgres.driver,
                schema_list=schema_list,
                source_label="PostgreSQL",
                query_alias_prefix="postgresql",
            )
        except Exception:
            return []

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

        try:
            return self._introspect_jdbc_information_schema(
                jdbc_url=mysql.jdbc_url or "",
                user=mysql.username or "",
                password=mysql.password or "",
                driver=mysql.driver,
                schema_list=schema_list,
                source_label="MySQL",
                query_alias_prefix="mysql",
            )
        except Exception:
            return []

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
                    "name": f"{query_alias_prefix}.{table_name}",
                    "physical_name": f"{schema}.{table_name}",
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
        if generated_sql.source == SourceKind.polaris:
            statement = self._qualify_polaris_sql(statement)
        else:
            statement = self._qualify_jdbc_sql(statement, generated_sql.source)
        statement = self._repair_join_column_references(statement, generated_sql.source)
        if "limit" not in statement.lower():
            statement = f"{statement}\nLIMIT {max_rows}"

        jdbc_source = self.get_jdbc_source(generated_sql.source)
        if jdbc_source and jdbc_source.enabled:
            try:
                dataframe = self._read_jdbc_query(
                    jdbc_url=jdbc_source.jdbc_url or "",
                    query=statement,
                    user=jdbc_source.username or "",
                    password=jdbc_source.password or "",
                    driver=jdbc_source.driver,
                )
                rows = [row.asDict(recursive=True) for row in dataframe.limit(max_rows).collect()]
                return rows, f"Executed against JDBC source '{generated_sql.source.value}'."
            except Exception as exc:
                return [], f"JDBC execution skipped or failed: {exc}"

        try:
            dataframe = spark.sql(statement)
            rows = [row.asDict(recursive=True) for row in dataframe.limit(max_rows).collect()]
            return rows, f"Executed against Spark for source '{generated_sql.source.value}'."
        except Exception as exc:
            return [], f"Spark execution skipped or failed: {exc}"

    def backup_jdbc_table_to_polaris(
        self,
        source: SourceKind,
        physical_table_name: str,
        destination_parts: list[str],
        overwrite: bool = True,
    ) -> int:
        spark = self.session
        if spark is None:
            raise RuntimeError("Spark session is unavailable.")

        if not self.settings.polaris_enabled:
            raise RuntimeError("Polaris is not enabled.")

        jdbc_source = self.get_jdbc_source(source)
        if jdbc_source is None or not jdbc_source.enabled:
            raise RuntimeError(f"JDBC source '{source.value}' is not enabled.")

        dataframe = self._read_jdbc_query(
            jdbc_url=jdbc_source.jdbc_url or "",
            query=f"SELECT * FROM {physical_table_name}",
            user=jdbc_source.username or "",
            password=jdbc_source.password or "",
            driver=jdbc_source.driver,
        )
        row_count = dataframe.count()

        namespace_parts = destination_parts[:-1]
        table_name = destination_parts[-1]
        self._ensure_polaris_namespace(namespace_parts)

        destination_identifier = ".".join(
            [self._quote_identifier(self.settings.polaris_catalog_name)]
            + [self._quote_identifier(part) for part in namespace_parts]
            + [self._quote_identifier(table_name)]
        )

        temp_view = f"backup_{source.value}_{uuid4().hex}"
        dataframe.createOrReplaceTempView(temp_view)
        try:
            if overwrite:
                spark.sql(f"DROP TABLE IF EXISTS {destination_identifier}")
                spark.sql(
                    f"CREATE TABLE {destination_identifier} USING iceberg "
                    f"AS SELECT * FROM {self._quote_identifier(temp_view)}"
                )
            elif self._polaris_table_exists(namespace_parts, table_name):
                spark.sql(
                    f"INSERT INTO {destination_identifier} "
                    f"SELECT * FROM {self._quote_identifier(temp_view)}"
                )
            else:
                spark.sql(
                    f"CREATE TABLE {destination_identifier} USING iceberg "
                    f"AS SELECT * FROM {self._quote_identifier(temp_view)}"
                )
        finally:
            spark.catalog.dropTempView(temp_view)

        return row_count

    def _polaris_catalog_configs(self) -> dict[str, str]:
        if not self.settings.polaris_enabled:
            return {}

        prefix = f"spark.sql.catalog.{self.settings.polaris_catalog_name}"
        configs = {
            prefix: "org.apache.iceberg.spark.SparkCatalog",
            f"{prefix}.type": "rest",
            f"{prefix}.uri": self.settings.polaris_uri or "",
            f"{prefix}.warehouse": self.settings.polaris_warehouse or "",
        }

        if self.settings.polaris_scope:
            configs[f"{prefix}.scope"] = self.settings.polaris_scope
        if self.settings.polaris_credential:
            configs[f"{prefix}.credential"] = self.settings.polaris_credential
            configs[f"{prefix}.token-refresh-enabled"] = str(
                self.settings.polaris_token_refresh_enabled
            ).lower()
        if self.settings.polaris_token:
            configs[f"{prefix}.token"] = self.settings.polaris_token
        if self.settings.polaris_access_delegation:
            configs[f"{prefix}.header.X-Iceberg-Access-Delegation"] = (
                self.settings.polaris_access_delegation
            )
        if self.settings.polaris_client_region:
            configs[f"{prefix}.client.region"] = self.settings.polaris_client_region

        for key, value in self.settings.polaris_catalog_option_map.items():
            configs[f"{prefix}.{key}"] = value

        return configs

    def _ensure_polaris_namespace(self, namespace_parts: list[str]) -> None:
        spark = self.session
        if spark is None:
            raise RuntimeError("Spark session is unavailable.")

        accumulated: list[str] = []
        for part in namespace_parts:
            accumulated.append(part)
            namespace_identifier = ".".join(
                [self._quote_identifier(self.settings.polaris_catalog_name)]
                + [self._quote_identifier(segment) for segment in accumulated]
            )
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace_identifier}")

    def _polaris_table_exists(self, namespace_parts: list[str], table_name: str) -> bool:
        spark = self.session
        if spark is None:
            return False

        namespace_identifier = ".".join(
            [self._quote_identifier(self.settings.polaris_catalog_name)]
            + [self._quote_identifier(segment) for segment in namespace_parts]
        )
        try:
            rows = spark.sql(
                f"SHOW TABLES IN {namespace_identifier} LIKE {self._sql_literal(table_name)}"
            ).collect()
        except Exception:
            return False
        return bool(rows)

    def _qualify_polaris_sql(self, statement: str) -> str:
        replacements: dict[str, str] = {}
        for table in self.introspect_polaris_metadata():
            full_name = str(table.get("name", "")).strip()
            parts = [part for part in full_name.split(".") if part]
            if len(parts) < 3 or parts[0] != self.settings.polaris_catalog_name:
                continue
            namespace_qualified = ".".join(parts[1:])
            replacements[namespace_qualified] = full_name

        qualified_statement = statement
        for short_name, full_name in sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True):
            pattern = rf"(?<![\w`.]){re.escape(short_name)}(?![\w`])"
            qualified_statement = re.sub(pattern, full_name, qualified_statement)

        return qualified_statement

    def _qualify_jdbc_sql(self, statement: str, source: SourceKind) -> str:
        metadata_tables: list[dict[str, Any]]
        if source == SourceKind.mysql:
            metadata_tables = self.introspect_mysql_metadata()
        elif source == SourceKind.postgresql:
            metadata_tables = self.introspect_postgresql_metadata()
        else:
            return statement

        replacements: dict[str, str] = {}
        for table in metadata_tables:
            logical_name = str(table.get("name", "")).strip()
            physical_name = str(table.get("physical_name", "")).strip()
            if logical_name and physical_name:
                replacements[logical_name] = physical_name

        qualified_statement = statement
        for logical_name, physical_name in sorted(
            replacements.items(), key=lambda item: len(item[0]), reverse=True
        ):
            pattern = rf"(?<![\w`.]){re.escape(logical_name)}(?![\w`])"
            qualified_statement = re.sub(pattern, physical_name, qualified_statement)

        return qualified_statement

    def _repair_join_column_references(self, statement: str, source: SourceKind) -> str:
        if " join " not in statement.lower():
            return statement

        alias_sequence = self._extract_table_aliases(statement)
        if len(alias_sequence) < 2:
            return statement

        metadata_lookup = self._metadata_table_lookup(source)
        alias_columns: dict[str, set[str]] = {}
        ordered_aliases: list[str] = []
        for table_name, alias in alias_sequence:
            table_metadata = metadata_lookup.get(table_name.lower())
            if not table_metadata:
                continue
            ordered_aliases.append(alias)
            alias_columns[alias] = {
                str(column.get("name", "")).lower()
                for column in table_metadata.get("columns", [])
                if str(column.get("name", "")).strip()
            }

        if len(alias_columns) < 2:
            return statement

        column_alias_map: dict[str, list[str]] = defaultdict(list)
        for alias in ordered_aliases:
            for column_name in alias_columns.get(alias, set()):
                column_alias_map[column_name].append(alias)

        repaired = statement
        for column_name, aliases in sorted(
            column_alias_map.items(),
            key=lambda item: len(item[0]),
            reverse=True,
        ):
            chosen_alias = aliases[0]
            pattern = rf"(?<![\w`.]){re.escape(column_name)}(?![\w`])"
            repaired = re.sub(pattern, f"{chosen_alias}.{column_name}", repaired, flags=re.IGNORECASE)

        return repaired

    def _metadata_table_lookup(self, source: SourceKind) -> dict[str, dict[str, Any]]:
        if source == SourceKind.polaris:
            metadata_tables = self.introspect_polaris_metadata()
        elif source == SourceKind.mysql:
            metadata_tables = self.introspect_mysql_metadata()
        elif source == SourceKind.postgresql:
            metadata_tables = self.introspect_postgresql_metadata()
        else:
            metadata_tables = []

        lookup: dict[str, dict[str, Any]] = {}
        for table in metadata_tables:
            logical_name = str(table.get("name", "")).strip()
            physical_name = str(table.get("physical_name", "")).strip()
            if logical_name:
                lookup[logical_name.lower()] = table
            if physical_name:
                lookup[physical_name.lower()] = table
        return lookup

    @staticmethod
    def _extract_table_aliases(statement: str) -> list[tuple[str, str]]:
        matches = re.finditer(
            r"\b(?:from|join)\s+([`\"\w.]+)(?:\s+(?:as\s+)?([`\"\w]+))?",
            statement,
            flags=re.IGNORECASE,
        )
        aliases: list[tuple[str, str]] = []
        for match in matches:
            table_name = match.group(1).replace("`", "").replace('"', "")
            alias = match.group(2)
            if alias:
                alias = alias.replace("`", "").replace('"', "")
            else:
                alias = table_name.split(".")[-1]
            aliases.append((table_name, alias))
        return aliases

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
