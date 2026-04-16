# Local Metadata Lab

This repo includes a small local lab for exercising live metadata introspection against:

- Polaris
- MySQL
- PostgreSQL

The lab uses Podman containers for the sample databases and the backend runs from the repo's Python virtual environment.
By default, the helper scripts use Podman storage under `/datastore/opc`.

## Files

- `infra/local-services/mysql/init.sql`
- `infra/local-services/postgres/init.sql`
- `scripts/podman-local-services-up.sh`
- `scripts/podman-local-services-down.sh`
- `scripts/bootstrap_local_polaris.py`
- `scripts/seed-local-polaris.sh`
- `scripts/seed_local_polaris_catalog.py`
- `scripts/run-local-metadata-exercise.sh`
- `.env.local-services.example`

## Quick Start

1. Copy `.env.local-services.example` to `.env.local-services`.
2. Start the sample services:

```bash
./scripts/podman-local-services-up.sh
```

3. Create the backend virtual environment if needed:

```bash
python3 -m venv .venv
.venv/bin/pip install -e backend
```

4. Seed the local Polaris catalog if you started services before the virtualenv existed:

```bash
./scripts/seed-local-polaris.sh
```

5. Run the metadata exercise:

```bash
./scripts/run-local-metadata-exercise.sh
```

The exercise script calls the real backend `/api/v1/health` and `/api/v1/sources` paths in-process and prints the JSON response.

## What This Proves

- Spark can start locally
- Spark can connect to a local Polaris catalog through the Iceberg REST catalog
- Spark can connect to MySQL through JDBC
- Spark can connect to PostgreSQL through JDBC
- The backend's metadata service merges live source metadata into the API response
