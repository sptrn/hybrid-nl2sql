#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
EXPECTED_PYTHON="${VENV_DIR}/bin/python3"
BACKEND_HOST="${BACKEND_HOST:-0.0.0.0}"
BACKEND_PORT="${BACKEND_PORT:-8000}"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  python3 -m venv "${VENV_DIR}"
fi

if [[ -f "${VENV_DIR}/bin/pip" ]]; then
  PIP_SHEBANG="$(head -n 1 "${VENV_DIR}/bin/pip" || true)"
  if [[ "${PIP_SHEBANG}" != "#!${EXPECTED_PYTHON}" ]]; then
    python3 -m venv --clear "${VENV_DIR}"
  fi
fi

cd "${ROOT_DIR}"
"${VENV_DIR}/bin/python" -m pip install -e "${ROOT_DIR}/backend"

if ! "${VENV_DIR}/bin/python" - "${BACKEND_HOST}" "${BACKEND_PORT}" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((host, port))
    except OSError:
        raise SystemExit(1)
PY
then
  echo "Port ${BACKEND_PORT} is already in use on ${BACKEND_HOST}." >&2
  echo "Set BACKEND_PORT to a free port, for example:" >&2
  echo "  BACKEND_PORT=8001 ${0}" >&2
  exit 1
fi

exec "${VENV_DIR}/bin/uvicorn" app.main:app --app-dir "${ROOT_DIR}/backend" --reload --reload-dir "${ROOT_DIR}/backend" --host "${BACKEND_HOST}" --port "${BACKEND_PORT}"
