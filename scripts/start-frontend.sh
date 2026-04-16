#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRONTEND_DIR="${ROOT_DIR}/frontend"
FRONTEND_HOST="${FRONTEND_HOST:-0.0.0.0}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
BACKEND_PORT="${BACKEND_PORT:-8000}"

export VITE_DEV_API_PROXY_TARGET="${VITE_DEV_API_PROXY_TARGET:-http://127.0.0.1:${BACKEND_PORT}}"

cd "${FRONTEND_DIR}"
npm install
exec npm run dev -- --host "${FRONTEND_HOST}" --port "${FRONTEND_PORT}"
