import json
import re
from typing import Any
from typing import Optional

from pydantic import BaseModel

from app.core.config import Settings
from app.models.schemas import GeneratedSQL, QueryRequest, SourceKind
from app.services.connectors import get_jdbc_sources
from app.services.llm import OCIChatModelFactory
from app.services.metadata import CatalogMetadataService


class OCIQueryCandidate(BaseModel):
    source: SourceKind
    statement: str
    rationale: str


class OCIQueryBatch(BaseModel):
    queries: list[OCIQueryCandidate]


class NL2SQLAgentService:
    def __init__(self, settings: Settings, metadata_service: CatalogMetadataService) -> None:
        self.settings = settings
        self.metadata_service = metadata_service
        self.llm = OCIChatModelFactory(settings).build()
        self.last_generation_error: Optional[str] = None

    @property
    def mode(self) -> str:
        return "oci" if self.llm else "stub"

    def generate(
        self, request: QueryRequest
    ) -> tuple[list[SourceKind], list[GeneratedSQL], Optional[str], Optional[str]]:
        self.last_generation_error = None
        sources = self._resolve_sources(request)
        available_sources = self._available_sources()
        executable_sources = [source for source in sources if source in available_sources]

        if not executable_sources:
            return sources, [], self.settings.oci_model_id, None

        if self.llm:
            candidate = self._generate_with_oci(request, executable_sources)
            if candidate:
                return sources, candidate, self.settings.oci_model_id, None

        return (
            sources,
            self._fallback_sql(request, executable_sources, self.last_generation_error),
            self.settings.oci_model_id,
            self.last_generation_error,
        )

    def _resolve_sources(self, request: QueryRequest) -> list[SourceKind]:
        preferred = [source for source in request.source_preference if source != SourceKind.auto]
        if preferred:
            return preferred

        question = request.question.lower()
        explicit_sources: list[SourceKind] = []
        if "iceberg" in question or "polaris" in question or "lakehouse" in question:
            explicit_sources.append(SourceKind.polaris)
        if "mysql" in question:
            explicit_sources.append(SourceKind.mysql)
        if "postgres" in question or "postgresql" in question:
            explicit_sources.append(SourceKind.postgresql)
        if "oracle" in question:
            explicit_sources.append(SourceKind.oracle)
        if explicit_sources:
            deduped_sources: list[SourceKind] = []
            for source in explicit_sources:
                if source not in deduped_sources:
                    deduped_sources.append(source)
            return deduped_sources
        return [SourceKind.polaris, SourceKind.mysql, SourceKind.postgresql, SourceKind.oracle]

    def _available_sources(self) -> set[SourceKind]:
        available = {
            source.source
            for source in get_jdbc_sources(self.settings)
            if source.enabled
        }
        if self.settings.polaris_enabled:
            available.add(SourceKind.polaris)
        return available

    def _generate_with_oci(
        self,
        request: QueryRequest,
        sources: list[SourceKind],
    ) -> Optional[list[GeneratedSQL]]:
        system_prompt = self._build_system_prompt()
        source_list = ", ".join(source.value for source in sources)
        backup_question = self._is_backup_question(request.question)
        human_prompt = f"""
Generate read-only SQL for the following question.

Question: {request.question}
Candidate sources: {source_list}
Maximum rows: {request.max_rows}

If the question is broad, ambiguous, or naturally comparative, prefer returning SQL for multiple relevant sources so the UI can showcase cross-source analysis.
For ambiguous business questions, return queries for at least two relevant configured sources when possible.
If sources cannot be joined safely, return separate per-source queries instead of inventing unsupported cross-database joins.
When a query joins tables, qualify every selected, grouped, ordered, and filtered column with a table name or alias. Never use bare column names in joined queries.
If required join keys or columns are not present in the provided metadata, do not assume them. Skip that source or use a simpler query that matches the metadata.
Do not generate a query for a source unless the provided metadata clearly supports answering the user's question from that source.
Only use the candidate sources listed above.
For each query object, every table reference in `statement` must belong to that query's `source`.
If a `postgresql` query cannot be answered with `postgresql.*` tables alone, omit it instead of referencing `mysql.*` or `polaris.*`.
If a `mysql` query cannot be answered with `mysql.*` tables alone, omit it instead of referencing other sources.
If a `polaris` query cannot be answered with `polaris.*` tables alone, omit it instead of referencing other sources.
If a source does not contain direct customer entities but does contain adjacent operational activity, you may return an analogous activity query for that source, but the rationale must clearly label it as an analogue or related operational perspective rather than direct customer activity.
If the question mentions backup, archived, snapshot, copied, or historical Polaris data, then any `polaris` query must use only `polaris.backups.*` tables.
If no relevant `polaris.backups.*` tables exist for a backup-oriented question, omit the Polaris query rather than using a live Polaris table.

Return structured output with a `queries` array. Do not wrap the answer in markdown fences.
"""

        try:
            structured_llm = self.llm.with_structured_output(OCIQueryBatch, method="json_schema")
            payload = structured_llm.invoke(
                [
                    ("system", system_prompt),
                    ("human", human_prompt),
                ]
            )
            queries = payload.queries
        except Exception as primary_error:
            try:
                response = self.llm.invoke(
                    [
                        ("system", system_prompt),
                        ("human", human_prompt),
                    ]
                )
                payload = self._parse_oci_payload(response)
                queries = payload.get("queries", [])
            except Exception as fallback_error:
                self.last_generation_error = (
                    "OCI generation failed: "
                    f"{primary_error.__class__.__name__}: {primary_error}. "
                    "Fallback parse also failed: "
                    f"{fallback_error.__class__.__name__}: {fallback_error}."
                )
                return None

        generated: list[GeneratedSQL] = []
        allowed_sources = set(sources)
        for item in queries:
            try:
                if isinstance(item, OCIQueryCandidate):
                    source = item.source
                    statement = item.statement
                    rationale = item.rationale
                else:
                    source = SourceKind(item["source"])
                    statement = item["statement"]
                    rationale = item.get("rationale", "Generated by OCI.")

                if source not in allowed_sources:
                    continue
                if (
                    backup_question
                    and source == SourceKind.polaris
                    and "polaris.backups." not in statement.lower()
                ):
                    continue
                generated.append(
                    GeneratedSQL(
                        source=source,
                        statement=statement,
                        rationale=rationale,
                    )
                )
            except Exception:
                continue

        if not generated:
            self.last_generation_error = (
                "OCI generation returned no valid source-scoped SQL statements."
            )

        return generated or None

    @staticmethod
    def _is_backup_question(question: str) -> bool:
        lowered = question.lower()
        return any(
            term in lowered
            for term in ("backup", "archived", "archive", "snapshot", "historical", "copied")
        )

    def _parse_oci_payload(self, response: Any) -> dict[str, Any]:
        if isinstance(response, dict):
            return response

        content = getattr(response, "content", response)
        if isinstance(content, dict):
            return content

        if isinstance(content, list):
            fragments: list[str] = []
            for item in content:
                if isinstance(item, str):
                    fragments.append(item)
                elif isinstance(item, dict):
                    text = item.get("text") or item.get("output") or item.get("content")
                    if isinstance(text, str):
                        fragments.append(text)
            content = "\n".join(fragment for fragment in fragments if fragment).strip()

        if not isinstance(content, str):
            content = str(content)

        text = content.strip()
        if not text:
            raise ValueError("OCI response content was empty.")

        candidates = [text]

        fenced_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
        if fenced_match:
            candidates.append(fenced_match.group(1).strip())

        first_brace = text.find("{")
        last_brace = text.rfind("}")
        if first_brace != -1 and last_brace != -1 and first_brace < last_brace:
            candidates.append(text[first_brace : last_brace + 1].strip())

        for candidate in candidates:
            try:
                payload = json.loads(candidate)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                return payload

        raise ValueError("OCI response was not valid JSON.")

    def _build_system_prompt(self) -> str:
        return (
            "You are an enterprise NL2SQL assistant.\n"
            "Generate only read-only SQL.\n"
            "Showcase cross-source analysis when the question is broad, ambiguous, or comparative.\n"
            "When multiple configured sources contain relevant business entities, prefer returning queries for more than one source instead of collapsing to a single source.\n"
            "For ambiguous business questions, return at least two relevant configured sources when possible.\n"
            "Use a single source only when the user explicitly names one source or when one source is clearly the only relevant source.\n"
            "If direct cross-source joins are not reliable, return one query per source rather than inventing a join.\n"
            "Use Spark-compatible SQL when possible.\n"
            "For Polaris tables, always use fully qualified three-part names starting with the catalog name `polaris`.\n"
            "For MySQL and PostgreSQL tables, use logical source-qualified names like `mysql.<table>` and `postgresql.<table>` in generated SQL.\n"
            "When joining tables, qualify every selected, grouped, ordered, and filtered column with a table alias or fully qualified table name.\n"
            "Never assume columns or join keys that are not present in the metadata.\n"
            "Do not include a source in the response unless its metadata clearly supports the requested analysis.\n"
            "If a source lacks the required tables or columns, omit it instead of explaining hypotheticals or writing placeholder SQL.\n"
            "Never reference tables from a different source than the query's declared source.\n"
            "If the user asks about backups, archives, snapshots, copied data, or historical Polaris data, use only `polaris.backups.*` tables for Polaris queries.\n"
            "For backup-oriented questions, never answer from live Polaris namespaces like `polaris.sales.*` unless the user explicitly asks for live Polaris data.\n"
            "If candidate sources are Polaris and PostgreSQL only, do not reference MySQL tables anywhere in the response.\n"
            "When a source only supports an analogous operational view rather than direct customer activity, say that explicitly in the rationale.\n"
            "Do not invent tables or columns.\n"
            "Use the metadata context below.\n\n"
            f"{self.metadata_service.prompt_context()}"
        )

    def _fallback_sql(
        self,
        request: QueryRequest,
        sources: list[SourceKind],
        reason: Optional[str] = None,
    ) -> list[GeneratedSQL]:
        question = request.question.lower()
        metadata = self.metadata_service.load().get("sources", {})
        statements: list[GeneratedSQL] = []

        for source in sources:
            if source == SourceKind.polaris:
                statement = self._fallback_polaris_sql(question, metadata)
                if statement is None:
                    continue
            elif source == SourceKind.mysql:
                statement = self._fallback_mysql_sql(question, metadata)
            elif source == SourceKind.postgresql:
                statement = self._fallback_postgresql_sql(question, metadata)
            else:
                statement = (
                    "SELECT invoice_id, account_id, invoice_total\n"
                    "FROM oracle.ar_invoices\n"
                    "ORDER BY invoice_id DESC"
                )

            if "count" in question:
                statement = f"SELECT COUNT(*) AS row_count\nFROM ({statement}) base_query"

            statements.append(
                GeneratedSQL(
                    source=source,
                    statement=statement,
                    rationale=(
                        f"Fallback SQL for {source.value}. "
                        f"{reason or 'OCI SQL generation was unavailable for this request.'}"
                    ),
                )
            )

        return statements

    def _fallback_polaris_sql(self, question: str, metadata: dict) -> Optional[str]:
        if self._is_backup_question(question):
            backup_tables = [
                str(table.get("name", ""))
                for table in metadata.get(SourceKind.polaris.value, [])
                if str(table.get("name", "")).startswith(f"{self.settings.polaris_catalog_name}.backups.")
            ]
            requested_terms = tuple(
                term
                for term in (
                    "order",
                    "shipment",
                    "product",
                    "inventory",
                    "customer",
                    "activity",
                    "history",
                )
                if term in question
            )
            preferred_backup = next(
                (
                    table_name
                    for table_name in backup_tables
                    if requested_terms and any(term in table_name.lower() for term in requested_terms)
                ),
                None,
            )
            if preferred_backup is None and not requested_terms and backup_tables:
                preferred_backup = backup_tables[0]
            if preferred_backup is None:
                return None

            return (
                "SELECT *\n"
                f"FROM {preferred_backup}\n"
                "LIMIT 100"
            )

        order_table = self._find_table_name(metadata, SourceKind.polaris, ("order",))
        customer_table = self._find_table_name(metadata, SourceKind.polaris, ("customer",))

        if "top" in question or "highest" in question:
            if customer_table != "unknown_table":
                return (
                    "SELECT c.customer_id, c.customer_name, SUM(o.order_total) AS total_order_amount\n"
                    f"FROM {customer_table} c\n"
                    f"JOIN {order_table} o ON o.customer_id = c.customer_id\n"
                    "GROUP BY c.customer_id, c.customer_name\n"
                    "ORDER BY total_order_amount DESC\n"
                    "LIMIT 10"
                )

            return (
                "SELECT customer_id, SUM(order_total) AS total_order_amount\n"
                f"FROM {order_table}\n"
                "GROUP BY customer_id\n"
                "ORDER BY total_order_amount DESC\n"
                "LIMIT 10"
            )

        return (
            "SELECT customer_id, order_total, order_ts\n"
            f"FROM {order_table}\n"
            "ORDER BY order_ts DESC"
        )

    def _fallback_mysql_sql(self, question: str, metadata: dict) -> str:
        customer_table = self._find_table_name(metadata, SourceKind.mysql, ("customer",))
        if "region" in question:
            return (
                "SELECT region, COUNT(*) AS customer_count\n"
                f"FROM {customer_table}\n"
                "GROUP BY region\n"
                "ORDER BY customer_count DESC, region"
            )

        return (
            "SELECT customer_id, customer_name, region\n"
            f"FROM {customer_table}\n"
            "ORDER BY customer_name"
        )

    def _fallback_postgresql_sql(self, question: str, metadata: dict) -> str:
        shipment_table = self._find_table_name(metadata, SourceKind.postgresql, ("shipment",))
        product_table = self._find_table_name(metadata, SourceKind.postgresql, ("product",))

        if "latest" in question:
            return (
                "SELECT shipment_id, product_id, shipped_at, quantity\n"
                f"FROM {shipment_table}\n"
                "ORDER BY shipped_at DESC"
            )

        return (
            "SELECT product_id, product_name, category\n"
            f"FROM {product_table}\n"
            "ORDER BY product_name"
        )

    @staticmethod
    def _find_table_name(
        metadata: dict,
        source: SourceKind,
        preferred_terms: tuple[str, ...],
    ) -> str:
        tables = metadata.get(source.value, [])
        for table in tables:
            table_name = table.get("name", "")
            lowered = table_name.lower()
            if any(term in lowered for term in preferred_terms):
                return table_name
        if tables:
            return tables[0].get("name", "")
        return {
            SourceKind.polaris: "sales.orders",
            SourceKind.mysql: "mysql.customers",
            SourceKind.postgresql: "postgresql.inventory_products",
        }.get(source, "unknown_table")
