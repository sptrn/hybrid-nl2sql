#!/usr/bin/env bash
set -euo pipefail

PODMAN_ROOT="${PODMAN_ROOT:-/datastore/opc/podman_storage/storage/graphroot}"
PODMAN_RUNROOT="${PODMAN_RUNROOT:-/datastore/opc/podman_storage/storage/runroot}"
PODMAN_TMPDIR="${PODMAN_TMPDIR:-/datastore/opc/podman_tmp}"
PODMAN=(podman --root "${PODMAN_ROOT}" --runroot "${PODMAN_RUNROOT}" --tmpdir "${PODMAN_TMPDIR}")
NETWORK_NAME="${NETWORK_NAME:-hybrid-nl2sql-lab}"
mkdir -p "${PODMAN_ROOT}" "${PODMAN_RUNROOT}" "${PODMAN_TMPDIR}"
export TMPDIR="${PODMAN_TMPDIR}"
export CONTAINERS_IMAGE_COPY_TMPDIR="${PODMAN_TMPDIR}"

for container in hybrid-nl2sql-mysql hybrid-nl2sql-postgres hybrid-nl2sql-rustfs hybrid-nl2sql-polaris; do
  if "${PODMAN[@]}" container exists "${container}"; then
    "${PODMAN[@]}" rm -f "${container}" >/dev/null
  fi
done

if "${PODMAN[@]}" network exists "${NETWORK_NAME}"; then
  "${PODMAN[@]}" network rm "${NETWORK_NAME}" >/dev/null
fi

echo "Local metadata services stopped."
