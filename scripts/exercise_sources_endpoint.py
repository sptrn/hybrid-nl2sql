#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from fastapi.testclient import TestClient


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    env_file = root / (sys.argv[1] if len(sys.argv) > 1 else ".env.local-services")
    if not env_file.exists():
        print(f"Environment file not found: {env_file}", file=sys.stderr)
        return 1

    load_dotenv(env_file, override=True)

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from app.main import app

    client = TestClient(app)
    health = client.get("/api/v1/health")
    sources = client.get("/api/v1/sources")

    print("HEALTH")
    print(json.dumps(health.json(), indent=2))
    print("\nSOURCES")
    print(json.dumps(sources.json(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

