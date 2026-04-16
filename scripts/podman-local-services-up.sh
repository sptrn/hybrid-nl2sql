#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env.local-services}"
PODMAN_ROOT="${PODMAN_ROOT:-/datastore/opc/podman_storage/storage/graphroot}"
PODMAN_RUNROOT="${PODMAN_RUNROOT:-/datastore/opc/podman_storage/storage/runroot}"
PODMAN_TMPDIR="${PODMAN_TMPDIR:-/datastore/opc/podman_tmp}"
PODMAN=(podman --root "${PODMAN_ROOT}" --runroot "${PODMAN_RUNROOT}" --tmpdir "${PODMAN_TMPDIR}")
NETWORK_NAME="${NETWORK_NAME:-hybrid-nl2sql-lab}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-hybrid-nl2sql-mysql}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-hybrid-nl2sql-postgres}"
RUSTFS_CONTAINER="${RUSTFS_CONTAINER:-hybrid-nl2sql-rustfs}"
POLARIS_CONTAINER="${POLARIS_CONTAINER:-hybrid-nl2sql-polaris}"
MYSQL_PORT="${MYSQL_PORT:-13306}"
POSTGRES_PORT="${POSTGRES_PORT:-15432}"
POLARIS_PORT="${POLARIS_PORT:-18181}"
POLARIS_ADMIN_PORT="${POLARIS_ADMIN_PORT:-18182}"
RUSTFS_API_PORT="${RUSTFS_API_PORT:-19000}"
RUSTFS_CONSOLE_PORT="${RUSTFS_CONSOLE_PORT:-19001}"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
POLARIS_BOOTSTRAP_CLIENT_ID="${POLARIS_BOOTSTRAP_CLIENT_ID:-root}"
POLARIS_BOOTSTRAP_CLIENT_SECRET="${POLARIS_BOOTSTRAP_CLIENT_SECRET:-s3cr3t}"
POLARIS_DEFAULT_BASE_LOCATION="${POLARIS_DEFAULT_BASE_LOCATION:-s3://bucket123}"
POLARIS_STORAGE_ENDPOINT="${POLARIS_STORAGE_ENDPOINT:-http://127.0.0.1:${RUSTFS_API_PORT}}"
POLARIS_STORAGE_ENDPOINT_INTERNAL="${POLARIS_STORAGE_ENDPOINT_INTERNAL:-http://rustfs:9000}"
POLARIS_CATALOG_NAME="${POLARIS_WAREHOUSE:-quickstart_catalog}"
POLARIS_URI="${POLARIS_URI:-http://127.0.0.1:${POLARIS_PORT}/api/catalog}"
POLARIS_BUCKET_NAME="${POLARIS_DEFAULT_BASE_LOCATION#s3://}"
POLARIS_BUCKET_NAME="${POLARIS_BUCKET_NAME%%/*}"

mkdir -p "${PODMAN_ROOT}" "${PODMAN_RUNROOT}" "${PODMAN_TMPDIR}"
mkdir -p "${PODMAN_TMPDIR}/mc-config"
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

"${PODMAN[@]}" run -d --replace \
  --name "${RUSTFS_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  --network-alias rustfs \
  --security-opt label=disable \
  -p "${RUSTFS_API_PORT}:9000" \
  -p "${RUSTFS_CONSOLE_PORT}:9001" \
  -e RUSTFS_ACCESS_KEY=polaris_root \
  -e RUSTFS_SECRET_KEY=polaris_pass \
  -e RUSTFS_VOLUMES=/data \
  -e RUSTFS_ADDRESS=:9000 \
  -e RUSTFS_CONSOLE_ENABLE=true \
  -e RUSTFS_CONSOLE_ADDRESS=:9001 \
  docker.io/rustfs/rustfs:1.0.0-alpha.81 >/dev/null

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

echo "Waiting for RustFS..."
until python3 - <<PY >/dev/null 2>&1
import socket
socket.create_connection(("127.0.0.1", ${RUSTFS_API_PORT}), timeout=1).close()
socket.create_connection(("127.0.0.1", ${RUSTFS_CONSOLE_PORT}), timeout=1).close()
PY
do
  sleep 2
done

"${PODMAN[@]}" run --rm \
  --network "${NETWORK_NAME}" \
  -v "${PODMAN_TMPDIR}/mc-config:/root/.mc:Z" \
  docker.io/minio/mc:latest \
  alias set local http://rustfs:9000 polaris_root polaris_pass >/dev/null

"${PODMAN[@]}" run --rm \
  --network "${NETWORK_NAME}" \
  -v "${PODMAN_TMPDIR}/mc-config:/root/.mc:Z" \
  docker.io/minio/mc:latest \
  mb --ignore-existing "local/${POLARIS_BUCKET_NAME}" >/dev/null

"${PODMAN[@]}" run -d --replace \
  --name "${POLARIS_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  --security-opt label=disable \
  -p "${POLARIS_PORT}:8181" \
  -p "${POLARIS_ADMIN_PORT}:8182" \
  -e AWS_REGION=us-west-2 \
  -e AWS_ACCESS_KEY_ID=polaris_root \
  -e AWS_SECRET_ACCESS_KEY=polaris_pass \
  -e POLARIS_BOOTSTRAP_CREDENTIALS="${POLARIS_REALM},${POLARIS_BOOTSTRAP_CLIENT_ID},${POLARIS_BOOTSTRAP_CLIENT_SECRET}" \
  -e polaris.realm-context.realms="${POLARIS_REALM}" \
  -e quarkus.otel.sdk.disabled=true \
  docker.io/apache/polaris:latest >/dev/null

echo "Waiting for Polaris..."
until python3 - <<PY >/dev/null 2>&1
import urllib.request
urllib.request.urlopen("http://127.0.0.1:${POLARIS_ADMIN_PORT}/q/health", timeout=2).read()
PY
do
  sleep 2
done

POLARIS_URI="${POLARIS_URI}" \
POLARIS_REALM="${POLARIS_REALM}" \
POLARIS_BOOTSTRAP_CLIENT_ID="${POLARIS_BOOTSTRAP_CLIENT_ID}" \
POLARIS_BOOTSTRAP_CLIENT_SECRET="${POLARIS_BOOTSTRAP_CLIENT_SECRET}" \
POLARIS_WAREHOUSE="${POLARIS_CATALOG_NAME}" \
POLARIS_DEFAULT_BASE_LOCATION="${POLARIS_DEFAULT_BASE_LOCATION}" \
POLARIS_STORAGE_ENDPOINT="${POLARIS_STORAGE_ENDPOINT}" \
POLARIS_STORAGE_ENDPOINT_INTERNAL="${POLARIS_STORAGE_ENDPOINT_INTERNAL}" \
  python3 "${ROOT_DIR}/scripts/bootstrap_local_polaris.py"

sleep 5

if [[ -x "${ROOT_DIR}/.venv/bin/python" && -f "${ENV_FILE}" ]]; then
  "${ROOT_DIR}/scripts/seed-local-polaris.sh" "${ENV_FILE}" >/dev/null
fi

echo "Local metadata services are ready."
echo "MySQL:     127.0.0.1:${MYSQL_PORT}/sampledb"
echo "Postgres:  127.0.0.1:${POSTGRES_PORT}/sampledb"
echo "Polaris:   127.0.0.1:${POLARIS_PORT}/api/catalog"
echo "RustFS UI: http://127.0.0.1:${RUSTFS_CONSOLE_PORT}"
