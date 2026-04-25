import json
import os
import sys
import urllib.error
import urllib.request

CONNECT_URL = "http://connect:8083"
CONNECTOR_NAME = "cdc-connector"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": os.environ.get("PG_USER", "cdc_user"),
        "database.password": os.environ.get("PG_PASSWORD", "cdc_pass"),
        "database.dbname": os.environ.get("PG_DB", "sourcedb"),
        "topic.prefix": "dbserver1",
        "table.include.list": "public.customers,public.drivers",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
        "publication.name": "dbz_publication",
        "snapshot.mode": "initial",
        "tombstones.on.delete": "true",
    },
}

def _request(method, path, body=None):
    url = f"{CONNECT_URL}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())

def main():
    print(f"[health_check] Checking connector '{CONNECTOR_NAME}' at {CONNECT_URL}")
    status_code, _ = _request("GET", f"/connectors/{CONNECTOR_NAME}")
    if status_code == 404:
        print("[health_check] Connector not found — registering now...")
        code, resp = _request("POST", "/connectors", CONNECTOR_CONFIG)
        if code not in (200, 201):
            print(f"[health_check] ERROR: Failed to register connector: {resp}")
            sys.exit(1)
        print("[health_check] Connector registered successfully.")
    elif status_code != 200:
        print(f"[health_check] ERROR: Unexpected status {status_code}")
        sys.exit(1)
    _, status = _request("GET", f"/connectors/{CONNECTOR_NAME}/status")
    connector_state = status.get("connector", {}).get("state", "UNKNOWN")
    print(f"[health_check] Connector state: {connector_state}")
    if connector_state == "FAILED":
        print("[health_check] ERROR: Connector is in FAILED state.")
        sys.exit(1)
    print("[health_check] OK — connector is healthy.")
    sys.exit(0)

if __name__ == "__main__":
    main()