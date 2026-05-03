import os
import time

import requests


CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083")
CONNECTOR_NAME = os.environ.get("CONNECTOR_NAME", "cdc-connector")
TOPIC_PREFIX = os.environ.get("CDC_TOPIC_PREFIX", "dbserver1")

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.environ.get("PG_HOST", "postgres"),
        "database.port": os.environ.get("PG_PORT", "5432"),
        "database.user": os.environ.get("PG_USER", "cdc_user"),
        "database.password": os.environ.get("PG_PASSWORD", "cdc_pass"),
        "database.dbname": os.environ.get("PG_DB", "sourcedb"),
        "topic.prefix": TOPIC_PREFIX,
        "table.include.list": "public.customers,public.drivers",
        "plugin.name": "pgoutput",
        "slot.name": os.environ.get("DEBEZIUM_SLOT", "debezium_slot_project3"),
        "publication.name": os.environ.get("DEBEZIUM_PUBLICATION", "dbz_project3_publication"),
        "publication.autocreate.mode": "filtered",
        "snapshot.mode": "initial",
        "tombstones.on.delete": "true",
        "include.schema.changes": "false",
    },
}


def request_json(method, path, **kwargs):
    url = f"{CONNECT_URL}{path}"
    response = requests.request(method, url, timeout=15, **kwargs)
    if response.text:
        try:
            payload = response.json()
        except ValueError:
            payload = response.text
    else:
        payload = None
    return response.status_code, payload


def connector_exists():
    status, _ = request_json("GET", f"/connectors/{CONNECTOR_NAME}")
    return status == 200


def current_config():
    status, payload = request_json("GET", f"/connectors/{CONNECTOR_NAME}/config")
    if status == 404:
        return None
    if status != 200:
        raise RuntimeError(f"Could not read connector config. HTTP {status}: {payload}")
    return payload


def register_connector():
    status, payload = request_json("POST", "/connectors", json=CONNECTOR_CONFIG)
    if status not in (200, 201):
        raise RuntimeError(f"Failed to register connector. HTTP {status}: {payload}")
    print(f"Registered Debezium connector '{CONNECTOR_NAME}' with topic.prefix={TOPIC_PREFIX}.")


def recreate_if_config_drifted():
    config = current_config()
    if config is None:
        register_connector()
        return

    expected = CONNECTOR_CONFIG["config"]
    drifted_keys = [
        key for key in (
            "topic.prefix",
            "table.include.list",
            "plugin.name",
            "snapshot.mode",
            "tombstones.on.delete",
        )
        if str(config.get(key)) != str(expected.get(key))
    ]

    if not drifted_keys:
        return

    print(f"Connector config drifted in {drifted_keys}; recreating connector.")
    status, payload = request_json("DELETE", f"/connectors/{CONNECTOR_NAME}")
    if status not in (200, 202, 204):
        raise RuntimeError(f"Failed to delete old connector. HTTP {status}: {payload}")
    time.sleep(3)
    register_connector()


def wait_until_running():
    status_url = f"/connectors/{CONNECTOR_NAME}/status"
    deadline = time.time() + 90
    last_payload = None

    while time.time() < deadline:
        status, payload = request_json("GET", status_url)
        last_payload = payload
        if status == 404:
            time.sleep(2)
            continue
        if status != 200:
            raise RuntimeError(f"Could not reach connector status endpoint. HTTP {status}: {payload}")

        connector_state = payload.get("connector", {}).get("state")
        task_states = [task.get("state") for task in payload.get("tasks", [])]
        print(f"Connector state: {connector_state}; task states: {task_states}")

        if connector_state == "RUNNING" and all(state == "RUNNING" for state in task_states):
            print("Debezium connector is healthy.")
            return

        if connector_state == "FAILED" or any(state == "FAILED" for state in task_states):
            raise RuntimeError(f"Connector failed: {payload}")

        time.sleep(5)

    raise RuntimeError(f"Connector did not become RUNNING before timeout. Last status: {last_payload}")


def main():
    print(f"Checking Debezium connector '{CONNECTOR_NAME}' at {CONNECT_URL}")
    recreate_if_config_drifted()
    wait_until_running()


if __name__ == "__main__":
    main()
