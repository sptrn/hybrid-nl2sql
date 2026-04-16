#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-${ROOT_DIR}/.env.local-services}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  exit 1
fi

if [[ ! -x "${ROOT_DIR}/.venv/bin/python" ]]; then
  echo "Missing virtualenv at ${ROOT_DIR}/.venv" >&2
  echo "Create it with: python3 -m venv .venv && .venv/bin/pip install -e backend" >&2
  exit 1
fi

APP_ENV_FILE="${ENV_FILE}" \
PYTHONPATH="${ROOT_DIR}/backend:${PYTHONPATH:-}" \
  "${ROOT_DIR}/.venv/bin/python" "${ROOT_DIR}/scripts/seed_local_polaris_catalog.py"
