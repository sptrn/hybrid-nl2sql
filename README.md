# Hybrid NL2SQL Starter

Starter repository for a web-based NL2SQL application that uses:

- `FastAPI` for the backend API
- `LangChain` plus `langchain-oci` for OCI Generative AI integration
- `Spark SQL` as the execution layer
- `Apache Polaris` as the Iceberg catalog entry point
- `React` + `Vite` for the frontend

The scaffold is intentionally opinionated:

- The backend is runnable before OCI or database credentials are configured.
- The Spark layer is modeled as the single SQL execution boundary.
- The code includes guardrails and extension points instead of pretending the database integrations are already complete.

## Architecture

The intended request flow is:

1. The frontend sends a natural-language question to the backend.
2. The backend loads schema metadata and builds an NL2SQL prompt.
3. An OCI-backed LangChain service generates SQL for one or more target sources.
4. Guardrails validate that the SQL is read-only and bounded.
5. Spark executes approved SQL against:
   - Polaris-backed Iceberg catalogs
   - JDBC-accessible MySQL, PostgreSQL, and Oracle sources
6. The backend returns generated SQL, execution status, and result rows to the UI.

More detail is in [docs/architecture.md](/home/opc/hybrid-nl2sql/docs/architecture.md).
For a live local JDBC metadata exercise, see [docs/local-metadata-lab.md](/home/opc/hybrid-nl2sql/docs/local-metadata-lab.md).

## Repo Layout

```text
backend/      FastAPI app, OCI/LangChain integration, Spark services
frontend/     React + Vite web interface
docs/         Architecture and implementation notes
```

## Quick Start

1. Copy `.env.example` to `.env` and fill in your OCI and data-source settings.
2. Start the stack:

```bash
docker compose up --build
```

3. Open the UI at `http://localhost:5173`.
4. Open the API docs at `http://localhost:8000/docs`.

## Development

Backend:

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn app.main:app --reload --port 8000
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

Local metadata lab:

```bash
cp .env.local-services.example .env.local-services
./scripts/podman-local-services-up.sh
python3 -m venv .venv
.venv/bin/pip install -e backend
./scripts/run-local-metadata-exercise.sh
```

## What Is Implemented

- A real project structure with backend and frontend apps
- API endpoints for health, source inspection, and NL2SQL query submission
- OCI model configuration hooks
- Spark session bootstrap with Polaris catalog settings
- JDBC source definitions for MySQL, PostgreSQL, and Oracle
- Read-only SQL guardrails
- Live metadata introspection for Polaris, MySQL, and PostgreSQL, with sample metadata fallback
- A simple web UI for entering questions and reviewing generated SQL

## What You Still Need To Wire

- OCI credentials and compartment details
- JDBC drivers for every source you enable beyond the bundled PostgreSQL and MySQL examples
- Spark package and driver jars for your exact environment
- Authentication, auditing, and query approval flows
- Production deployment settings

## Recommended First Milestones

1. Verify OCI chat connectivity with the configured model.
2. Confirm Spark can read your Polaris catalog.
3. Expand live metadata harvesting to Oracle after Polaris, PostgreSQL, and MySQL are validated.
4. Lock down source allowlists and row limits for production queries.
5. Add tests around SQL generation and guardrail behavior.
