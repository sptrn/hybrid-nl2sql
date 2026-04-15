from dataclasses import dataclass
from typing import Optional

from app.core.config import Settings
from app.models.schemas import SourceKind


@dataclass(frozen=True)
class JDBCSourceConfig:
    source: SourceKind
    jdbc_url: Optional[str]
    username: Optional[str]
    password: Optional[str]
    driver: str
    description: str

    @property
    def enabled(self) -> bool:
        return bool(self.jdbc_url and self.username and self.password)


def get_jdbc_sources(settings: Settings) -> list[JDBCSourceConfig]:
    return [
        JDBCSourceConfig(
            source=SourceKind.mysql,
            jdbc_url=settings.mysql_jdbc_url,
            username=settings.mysql_jdbc_user,
            password=settings.mysql_jdbc_password,
            driver=settings.mysql_jdbc_driver,
            description="MySQL exposed through Spark JDBC.",
        ),
        JDBCSourceConfig(
            source=SourceKind.postgresql,
            jdbc_url=settings.postgres_jdbc_url,
            username=settings.postgres_jdbc_user,
            password=settings.postgres_jdbc_password,
            driver=settings.postgres_jdbc_driver,
            description="PostgreSQL exposed through Spark JDBC.",
        ),
        JDBCSourceConfig(
            source=SourceKind.oracle,
            jdbc_url=settings.oracle_jdbc_url,
            username=settings.oracle_jdbc_user,
            password=settings.oracle_jdbc_password,
            driver=settings.oracle_jdbc_driver,
            description="Oracle Database exposed through Spark JDBC.",
        ),
    ]
