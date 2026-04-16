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

More detail is in [docs/architecture.md](docs/architecture.md).
For a live local JDBC metadata exercise, see [docs/local-metadata-lab.md](docs/local-metadata-lab.md).

## Repo Layout

```text
backend/      FastAPI app, OCI/LangChain integration, Spark services
frontend/     React + Vite web interface
docs/         Architecture and implementation notes
```

## Installation Prerequisites

Before testing this stack in your own environment, make sure you have:

- A Linux, macOS, or Windows host that can run Docker Engine or Podman.
- At least 4 CPU cores and 8 GB RAM available for the containers, especially when Spark and Polaris are enabled.
- Network access from the host to any external systems you plan to query:
  `OCI Generative AI`, `Polaris`, `MySQL`, `PostgreSQL`, and `Oracle`.
- A valid OCI config file if you want to use OCI-backed SQL generation:
  `~/.oci/config` plus the matching API key material.
- A populated `.env` file with the connection details for the sources you want to enable.
- Open host ports for:
  `8000` for the backend API and `5173` for the frontend UI, or custom ports if you remap them.

Notes:

- If you only want to test the UI and API shell, you can leave OCI and database credentials unset and the app will still start in fallback mode.
- If you are using Podman instead of Docker, substitute `podman` and `podman compose` for the Docker commands below.

## Install From Prebuilt Docker Images

If your team publishes prebuilt images for this repo, users can test the stack without building from source.

1. Copy `.env.example` to `.env` and fill in the required OCI and source connection settings.
2. Make sure your OCI config is available on the host at `~/.oci`.
3. Pull the published images from your container registry:

```bash
docker pull <registry>/<namespace>/hybrid-nl2sql-backend:<tag>
docker pull <registry>/<namespace>/hybrid-nl2sql-frontend:<tag>
```

4. Create a compose file for image-based deployment, for example `docker-compose.images.yml`:

```yaml
services:
  backend:
    image: <registry>/<namespace>/hybrid-nl2sql-backend:<tag>
    env_file:
      - .env
    ports:
      - "${API_PORT:-8000}:8000"
    volumes:
      - ${HOME}/.oci:/root/.oci:ro

  frontend:
    image: <registry>/<namespace>/hybrid-nl2sql-frontend:<tag>
    env_file:
      - .env
    ports:
      - "${FRONTEND_PORT:-5173}:5173"
    depends_on:
      - backend
```

5. Start the stack:

```bash
docker compose -f docker-compose.images.yml up -d
```

6. Verify the installation:

- Open the UI at `http://localhost:5173`
- Open the API docs at `http://localhost:8000/docs`
- Check the health endpoint:

```bash
curl http://localhost:8000/api/v1/health
```

7. Stop the stack when you are done:

```bash
docker compose -f docker-compose.images.yml down
```

Recommended first tests after the containers are up:

- Open the `NL2SQL Dashboard` tab and submit a simple prompt such as `Show the latest orders from Polaris.`
- Open the `Backup To Iceberg` tab and verify that MySQL or PostgreSQL tables are discoverable before running a backup.
- Confirm that the `Runtime` card shows the expected `LLM mode` and `Spark` readiness.

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
/datastore/hybrid-nl2sql/scripts/start-backend.sh
```

If port `8000` is already in use, start the backend on another port:

```bash
BACKEND_PORT=8001 /datastore/hybrid-nl2sql/scripts/start-backend.sh
```

Frontend:

```bash
/datastore/hybrid-nl2sql/scripts/start-frontend.sh
```

If the backend is running on a non-default port, start the frontend with the same `BACKEND_PORT` so the Vite proxy points at the right API:

```bash
BACKEND_PORT=8001 /datastore/hybrid-nl2sql/scripts/start-frontend.sh
```

For access from another machine on the same network, the frontend dev server binds to `0.0.0.0:5173`. The backend should also be started with `--host 0.0.0.0` if you want the browser on another host to reach the API directly.

## Polaris Setup

To enable the `polaris` source in the backend, set these environment variables in `.env` or `.env.local-services`:

```bash
POLARIS_URI=https://<your-polaris-host>/api/catalog
POLARIS_WAREHOUSE=<polaris-catalog-name>
POLARIS_SCOPE=PRINCIPAL_ROLE:ALL
POLARIS_CREDENTIAL=<client-id>:<client-secret>
```

Notes:

- `POLARIS_WAREHOUSE` should be the catalog or warehouse name exposed by Polaris, not an object storage URI.
- The backend auto-adds the default Iceberg Spark runtime package when Polaris is enabled. Override `SPARK_ICEBERG_RUNTIME_PACKAGE` if your Spark build needs a different coordinate.
- If your Polaris deployment gives you a bearer token directly, use `POLARIS_TOKEN` instead of `POLARIS_CREDENTIAL`.
- For extra Iceberg REST catalog properties, use `POLARIS_CATALOG_OPTIONS` with `key=value` pairs separated by `;`.

Local metadata lab:

```bash
cp .env.local-services.example .env.local-services
./scripts/podman-local-services-up.sh
python3 -m venv .venv
.venv/bin/pip install -e backend
./scripts/seed-local-polaris.sh
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
