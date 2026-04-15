# Architecture Notes

## Design Goals

- Use Spark as the common execution plane for heterogeneous data access.
- Use Polaris for Iceberg catalog access instead of modeling relational systems as Polaris-native sources.
- Keep the LLM focused on planning and SQL synthesis, not credential handling or unrestricted execution.
- Make the app safe-by-default with read-only SQL checks and explicit row limits.

## Logical Components

### Frontend

- Single-page web app with a chat-style query panel
- Displays:
  - user question
  - selected source strategy
  - generated SQL
  - guardrail decisions
  - returned rows

### Backend API

- `POST /api/v1/query` accepts natural-language questions
- `GET /api/v1/health` reports application readiness
- `GET /api/v1/sources` returns configured sources and sample metadata

### Agent Layer

- Builds a schema-aware system prompt
- Selects one or more candidate sources
- Produces SQL in a structured response
- Falls back to a deterministic stub mode when OCI is not configured
- Uses live Polaris, PostgreSQL, and MySQL metadata when Spark connectivity is available

### Execution Layer

- Creates a Spark session with Iceberg and Polaris-compatible catalog settings
- Provides extension points for:
  - Polaris catalog access via Iceberg REST catalog
  - JDBC-backed MySQL views
  - JDBC-backed PostgreSQL views
  - JDBC-backed Oracle views

### Metadata Layer

- Polaris metadata is discovered through Spark catalog inspection:
  - `SHOW NAMESPACES`
  - `SHOW TABLES`
  - `DESCRIBE TABLE`
- PostgreSQL and MySQL metadata are discovered through Spark JDBC queries against:
  - `information_schema.tables`
  - `information_schema.columns`
- A bundled sample catalog remains as a fallback when live systems are unavailable

## Recommended Production Enhancements

- Persist query traces and execution metrics
- Add semantic schema descriptions from your data catalog
- Introduce SQL linting and allowlists per source
- Add async job execution for large queries
- Add SSO and per-user authorization

## Source Strategy

- `polaris`: preferred for Iceberg tables surfaced through the Polaris REST catalog
- `mysql`: accessed through Spark JDBC readers or curated Spark views
- `postgresql`: accessed through Spark JDBC readers or curated Spark views
- `oracle`: accessed through Spark JDBC readers or curated Spark views

## Guardrail Strategy

- allow only read-only statements
- block DDL and DML keywords
- enforce `LIMIT`
- return generated SQL before execution so the UI can support approvals later
