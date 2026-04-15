#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PODMAN_ROOT="${PODMAN_ROOT:-/datastore/opc/podman_storage/storage/graphroot}"
PODMAN_RUNROOT="${PODMAN_RUNROOT:-/datastore/opc/podman_storage/storage/runroot}"
PODMAN_TMPDIR="${PODMAN_TMPDIR:-/datastore/opc/podman_tmp}"
PODMAN=(podman --root "${PODMAN_ROOT}" --runroot "${PODMAN_RUNROOT}" --tmpdir "${PODMAN_TMPDIR}")
NETWORK_NAME="${NETWORK_NAME:-hybrid-nl2sql-lab}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-hybrid-nl2sql-mysql}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-hybrid-nl2sql-postgres}"
MYSQL_PORT="${MYSQL_PORT:-13306}"
POSTGRES_PORT="${POSTGRES_PORT:-15432}"

mkdir -p "${PODMAN_ROOT}" "${PODMAN_RUNROOT}" "${PODMAN_TMPDIR}"
export TMPDIR="${PODMAN_TMPDIR}"
export CONTAINERS_IMAGE_COPY_TMPDIR="${PODMAN_TMPDIR}"

if ! "${PODMAN[@]}" network exists "${NETWORK_NAME}"; then
  "${PODMAN[@]}" network create "${NETWORK_NAME}" >/dev/null
fi

"${PODMAN[@]}" run -d --replace \
  --name "${MYSQL_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  --security-opt label=disable \
  -p "${MYSQL_PORT}:3306" \
  -e MYSQL_ROOT_PASSWORD=rootpass \
  -e MYSQL_DATABASE=sampledb \
  -e MYSQL_USER=app \
  -e MYSQL_PASSWORD=change-me \
  -v "${ROOT_DIR}/infra/local-services/mysql/init.sql:/docker-entrypoint-initdb.d/001-init.sql:ro" \
  docker.io/mysql:8.4 >/dev/null

"${PODMAN[@]}" run -d --replace \
  --name "${POSTGRES_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  --security-opt label=disable \
  -p "${POSTGRES_PORT}:5432" \
  -e POSTGRES_DB=sampledb \
  -e POSTGRES_USER=app \
  -e POSTGRES_PASSWORD=change-me \
  -v "${ROOT_DIR}/infra/local-services/postgres/init.sql:/docker-entrypoint-initdb.d/001-init.sql:ro" \
  docker.io/postgres:16 >/dev/null

echo "Waiting for MySQL..."
until python3 - <<PY >/dev/null 2>&1
import socket
socket.create_connection(("127.0.0.1", ${MYSQL_PORT}), timeout=1).close()
PY
do
  sleep 2
done

echo "Waiting for PostgreSQL..."
until python3 - <<PY >/dev/null 2>&1
import socket
socket.create_connection(("127.0.0.1", ${POSTGRES_PORT}), timeout=1).close()
PY
do
  sleep 2
done

sleep 5

echo "Local metadata services are ready."
echo "MySQL:     127.0.0.1:${MYSQL_PORT}/sampledb"
echo "Postgres:  127.0.0.1:${POSTGRES_PORT}/sampledb"
