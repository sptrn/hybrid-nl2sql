#!/usr/bin/env python3
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def request_json(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    body: Optional[Dict[str, Any]] = None,
    form: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    payload = None
    request_headers = dict(headers or {})
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    elif form is not None:
        payload = urllib.parse.urlencode(form).encode("utf-8")
        request_headers["Content-Type"] = "application/x-www-form-urlencoded"

    request = urllib.request.Request(url, data=payload, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            content = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed with {exc.code}: {detail}") from exc

    if not content.strip():
        return {}
    return json.loads(content)


def main() -> None:
    catalog_api = require_env("POLARIS_URI").rstrip("/")
    realm = os.getenv("POLARIS_REALM", "POLARIS")
    client_id = os.getenv("POLARIS_BOOTSTRAP_CLIENT_ID", "root")
    client_secret = os.getenv("POLARIS_BOOTSTRAP_CLIENT_SECRET", "s3cr3t")
    catalog_name = require_env("POLARIS_WAREHOUSE")
    default_base_location = os.getenv("POLARIS_DEFAULT_BASE_LOCATION", "s3://bucket123")
    storage_endpoint = os.getenv("POLARIS_STORAGE_ENDPOINT", "http://127.0.0.1:19000")
    storage_endpoint_internal = os.getenv(
        "POLARIS_STORAGE_ENDPOINT_INTERNAL",
        "http://rustfs:9000",
    )

    management_api = catalog_api.replace("/api/catalog", "/api/management", 1)
    token_response = request_json(
        "POST",
        f"{catalog_api}/v1/oauth/tokens",
        form={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
    )
    token = token_response.get("access_token")
    if not token:
        raise SystemExit("Polaris bootstrap failed: access token was not returned.")

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Polaris-Realm": realm,
    }
    payload = {
        "catalog": {
            "name": catalog_name,
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {
                "default-base-location": default_base_location,
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [default_base_location],
                "endpoint": storage_endpoint,
                "endpointInternal": storage_endpoint_internal,
                "pathStyleAccess": True,
            },
        }
    }

    try:
        request_json(
            "POST",
            f"{management_api}/v1/catalogs",
            headers=headers,
            body=payload,
        )
        print(f"Created Polaris catalog '{catalog_name}'.")
    except RuntimeError as exc:
        if " 409:" in str(exc):
            print(f"Polaris catalog '{catalog_name}' already exists.")
        else:
            raise


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise
