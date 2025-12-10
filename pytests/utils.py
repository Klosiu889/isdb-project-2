import time

import requests
from config import BASE_URL


def create_dummy_table(name):
    data = {
        "name": name,
        "columns": [
            {"name": "col1", "type": "INT64"},
            {"name": "col2", "type": "VARCHAR"},
        ],
    }
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 200
    return resp.json()


def wait_for_status(query_id, target_statuses, timeout=5):
    start = time.time()
    while time.time() - start < timeout:
        resp = requests.get(f"{BASE_URL}/query/{query_id}")
        assert resp.status_code == 200
        current_status = resp.json()["status"]
        if current_status in target_statuses:
            return current_status
        time.sleep(0.1)
    raise TimeoutError(
        f"Query {query_id} did not reach status {target_statuses} in {timeout}s"
    )
