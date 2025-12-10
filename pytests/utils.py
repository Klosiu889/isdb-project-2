import time
from typing import List, Literal, Tuple, TypedDict

import requests
from config import BASE_URL

QueryStatus = Literal["CREATED", "PLANNING", "RUNNING", "COMPLETED", "FAILED"]


class Column(TypedDict):
    name: str
    type: Literal["INT64", "VARCHAR"]


class Table(TypedDict):
    name: str
    columns: List[Column]


def create_dummy_table(name) -> Tuple[str, Table]:
    data: Table = {
        "name": name,
        "columns": [
            {"name": "col1", "type": "INT64"},
            {"name": "col2", "type": "VARCHAR"},
        ],
    }
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 200
    table_id = resp.json()
    return (table_id, data)


def create_table(name, columns) -> str:
    data = {"name": name, "columns": columns}
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 200
    table_id = resp.json()
    return table_id


def wait_for_status(query_id, target_statuses, timeout=5) -> QueryStatus:
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


def wait_for_final_status(query_id) -> QueryStatus:
    return wait_for_status(query_id, ["COMPLETED", "FAILED"])


def get_error_message(query_id):
    resp = requests.get(f"{BASE_URL}/error/{query_id}")
    assert resp.status_code == 200
    body = resp.json()
    return body["problems"][0]["error"]
