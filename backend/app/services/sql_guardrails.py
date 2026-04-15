import re

from app.models.schemas import GuardrailReport


READ_ONLY_START = re.compile(r"^\s*(select|with|show|describe|explain)\b", re.IGNORECASE)
BLOCKED_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|replace|grant|revoke|merge|call)\b",
    re.IGNORECASE,
)


class SQLGuardrails:
    def validate(self, statement: str, max_rows: int) -> GuardrailReport:
        normalized = statement.strip().rstrip(";")
        issues: list[str] = []

        if not READ_ONLY_START.search(normalized):
            issues.append("Only read-only SQL statements are allowed.")

        if BLOCKED_KEYWORDS.search(normalized):
            issues.append("Detected blocked DDL or DML keywords.")

        if re.search(r"\blimit\s+\d+\b", normalized, re.IGNORECASE):
            bounded = normalized
        else:
            bounded = f"{normalized}\nLIMIT {max_rows}"

        return GuardrailReport(
            approved=not issues,
            issues=issues,
            normalized_statement=bounded,
        )

