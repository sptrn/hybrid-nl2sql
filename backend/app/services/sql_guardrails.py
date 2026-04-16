import re

from app.models.schemas import GuardrailReport, SourceKind


READ_ONLY_START = re.compile(r"^\s*(select|with|show|describe|explain)\b", re.IGNORECASE)
BLOCKED_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|replace|grant|revoke|merge|call)\b",
    re.IGNORECASE,
)
SOURCE_PREFIXES = {
    SourceKind.polaris: ("mysql.", "postgresql.", "oracle."),
    SourceKind.mysql: ("polaris.", "postgresql.", "oracle."),
    SourceKind.postgresql: ("polaris.", "mysql.", "oracle."),
    SourceKind.oracle: ("polaris.", "mysql.", "postgresql."),
}


class SQLGuardrails:
    def validate(self, statement: str, max_rows: int, source: SourceKind) -> GuardrailReport:
        normalized = statement.strip().rstrip(";")
        issues: list[str] = []

        if not READ_ONLY_START.search(normalized):
            issues.append("Only read-only SQL statements are allowed.")

        if BLOCKED_KEYWORDS.search(normalized):
            issues.append("Detected blocked DDL or DML keywords.")

        lowered = normalized.lower()
        if source == SourceKind.polaris:
            lowered = self._strip_allowed_polaris_backup_namespaces(lowered)
        disallowed_prefixes = SOURCE_PREFIXES.get(source, ())
        referenced_foreign_sources = [
            prefix.rstrip(".")
            for prefix in disallowed_prefixes
            if prefix in lowered
        ]
        if referenced_foreign_sources:
            issues.append(
                "Query references tables from a different source: "
                + ", ".join(sorted(set(referenced_foreign_sources)))
                + "."
            )

        if re.search(r"\blimit\s+\d+\b", normalized, re.IGNORECASE):
            bounded = normalized
        else:
            bounded = f"{normalized}\nLIMIT {max_rows}"

        return GuardrailReport(
            approved=not issues,
            issues=issues,
            normalized_statement=bounded,
        )

    @staticmethod
    def _strip_allowed_polaris_backup_namespaces(statement: str) -> str:
        return re.sub(
            r"polaris\.backups\.(mysql|postgresql|oracle)\.",
            "polaris.backups.allowed.",
            statement,
            flags=re.IGNORECASE,
        )
