import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: str = Field(default="development", alias="APP_ENV")
    app_name: str = "Hybrid NL2SQL API"
    api_prefix: str = "/api/v1"
    cors_origins: list[str] = ["http://localhost:5173"]

    oci_region: Optional[str] = Field(default=None, alias="OCI_REGION")
    oci_compartment_id: Optional[str] = Field(default=None, alias="OCI_COMPARTMENT_ID")
    oci_model_id: str = Field(default="cohere.command-a-reasoning", alias="OCI_MODEL_ID")
    oci_service_endpoint: Optional[str] = Field(default=None, alias="OCI_SERVICE_ENDPOINT")
    oci_config_file: Optional[str] = Field(default=None, alias="OCI_CONFIG_FILE")
    oci_auth_profile: str = Field(default="DEFAULT", alias="OCI_AUTH_PROFILE")

    spark_app_name: str = Field(default="hybrid-nl2sql", alias="SPARK_APP_NAME")
    spark_master: str = Field(default="local[*]", alias="SPARK_MASTER")
    spark_jars_packages: str = Field(default="", alias="SPARK_JARS_PACKAGES")
    spark_iceberg_runtime_package: str = Field(
        default="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",
        alias="SPARK_ICEBERG_RUNTIME_PACKAGE",
    )
    metadata_max_namespaces: int = Field(default=25, alias="METADATA_MAX_NAMESPACES")
    metadata_max_tables_per_source: int = Field(default=100, alias="METADATA_MAX_TABLES_PER_SOURCE")
    mysql_metadata_schemas: str = Field(default="", alias="MYSQL_METADATA_SCHEMAS")
    postgres_metadata_schemas: str = Field(default="public", alias="POSTGRES_METADATA_SCHEMAS")

    polaris_catalog_name: str = Field(default="polaris", alias="POLARIS_CATALOG_NAME")
    polaris_uri: Optional[str] = Field(default=None, alias="POLARIS_URI")
    polaris_warehouse: Optional[str] = Field(default=None, alias="POLARIS_WAREHOUSE")
    polaris_scope: Optional[str] = Field(default=None, alias="POLARIS_SCOPE")
    polaris_credential: Optional[str] = Field(default=None, alias="POLARIS_CREDENTIAL")
    polaris_token: Optional[str] = Field(default=None, alias="POLARIS_TOKEN")
    polaris_access_delegation: Optional[str] = Field(
        default="vended-credentials",
        alias="POLARIS_ACCESS_DELEGATION",
    )
    polaris_token_refresh_enabled: bool = Field(
        default=True,
        alias="POLARIS_TOKEN_REFRESH_ENABLED",
    )
    polaris_client_region: Optional[str] = Field(default=None, alias="POLARIS_CLIENT_REGION")
    polaris_catalog_options: str = Field(default="", alias="POLARIS_CATALOG_OPTIONS")

    mysql_jdbc_url: Optional[str] = Field(default=None, alias="MYSQL_JDBC_URL")
    mysql_jdbc_user: Optional[str] = Field(default=None, alias="MYSQL_JDBC_USER")
    mysql_jdbc_password: Optional[str] = Field(default=None, alias="MYSQL_JDBC_PASSWORD")
    mysql_jdbc_driver: str = Field(default="com.mysql.cj.jdbc.Driver", alias="MYSQL_JDBC_DRIVER")

    postgres_jdbc_url: Optional[str] = Field(default=None, alias="POSTGRES_JDBC_URL")
    postgres_jdbc_user: Optional[str] = Field(default=None, alias="POSTGRES_JDBC_USER")
    postgres_jdbc_password: Optional[str] = Field(default=None, alias="POSTGRES_JDBC_PASSWORD")
    postgres_jdbc_driver: str = Field(default="org.postgresql.Driver", alias="POSTGRES_JDBC_DRIVER")

    oracle_jdbc_url: Optional[str] = Field(default=None, alias="ORACLE_JDBC_URL")
    oracle_jdbc_user: Optional[str] = Field(default=None, alias="ORACLE_JDBC_USER")
    oracle_jdbc_password: Optional[str] = Field(default=None, alias="ORACLE_JDBC_PASSWORD")
    oracle_jdbc_driver: str = Field(default="oracle.jdbc.OracleDriver", alias="ORACLE_JDBC_DRIVER")

    default_max_rows: int = 100

    @property
    def mysql_metadata_schema_list(self) -> list[str]:
        return [schema.strip() for schema in self.mysql_metadata_schemas.split(",") if schema.strip()]

    @property
    def postgres_metadata_schema_list(self) -> list[str]:
        return [schema.strip() for schema in self.postgres_metadata_schemas.split(",") if schema.strip()]

    @property
    def spark_packages(self) -> list[str]:
        packages = [pkg.strip() for pkg in self.spark_jars_packages.split(",") if pkg.strip()]
        if self.polaris_enabled and self.spark_iceberg_runtime_package:
            lowered = [pkg.lower() for pkg in packages]
            if not any("iceberg-spark-runtime" in pkg for pkg in lowered):
                packages.append(self.spark_iceberg_runtime_package)
        return packages

    @property
    def polaris_enabled(self) -> bool:
        return bool(self.polaris_uri and self.polaris_warehouse)

    @property
    def polaris_catalog_option_map(self) -> dict[str, str]:
        options: dict[str, str] = {}
        for raw_pair in self.polaris_catalog_options.split(";"):
            pair = raw_pair.strip()
            if not pair or "=" not in pair:
                continue
            key, value = pair.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key and value:
                options[key] = value
        return options

    @property
    def oci_ready(self) -> bool:
        return bool(
            self.oci_region
            and self.oci_compartment_id
            and self.oci_model_id
            and self.oci_service_endpoint
        )


@lru_cache
def get_settings() -> Settings:
    env_file_override = os.getenv("APP_ENV_FILE")
    if env_file_override:
        return Settings(_env_file=env_file_override)

    candidate_files = [
        Path(".env"),
        Path(".env.local-services"),
        Path("../.env"),
        Path("../.env.local-services"),
    ]
    selected_file = next((str(path) for path in candidate_files if path.exists()), None)

    if selected_file:
        return Settings(_env_file=selected_file)

    return Settings()
