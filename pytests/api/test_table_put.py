import requests
from config import BASE_URL


def test_put_table(server):
    data = {
        "name": "test_put_table",
        "columns": [
            {"name": "col1", "type": "INT64"},
            {"name": "col2", "type": "VARCHAR"},
        ],
    }
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 200


def test_put_table_with_existing_name(server):
    data = {"name": "test_put_table_with_existing_name", "columns": []}
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 200

    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 400

    body = resp.json()
    assert body["problems"]
    assert len(body["problems"]) == 1
    assert body["problems"][0]["error"] == "Table with given name already exists"


def test_put_table_with_duplicate_column_names(server):
    data = {
        "name": "test_put_table_with_duplicate_column_names",
        "columns": [
            {"name": "col1", "type": "INT64"},
            {"name": "col1", "type": "INT64"},
        ],
    }
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 400

    body = resp.json()
    assert body["problems"]
    assert len(body["problems"]) == 1
    assert body["problems"][0]["error"] == "Two columns have identical names"
    assert body["problems"][0]["context"] == "col1"


def test_put_table_with_multiple_errors(server):
    data = {"name": "test_put_table_with_multiple_errors", "columns": []}
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 200

    data = {
        "name": "test_put_table_with_multiple_errors",
        "columns": [
            {"name": "col1", "type": "VARCHAR"},
            {"name": "col1", "type": "VARCHAR"},
            {"name": "col2", "type": "INT64"},
            {"name": "col2", "type": "INT64"},
            {"name": "col3", "type": "INT64"},
            {"name": "col3", "type": "VARCHAR"},
        ],
    }
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 400

    body = resp.json()
    assert sorted(
        body["problems"], key=lambda x: (x["error"], x.get("context", ""))
    ) == sorted(
        [
            {"error": "Table with given name already exists"},
            {"error": "Two columns have identical names", "context": "col1"},
            {"error": "Two columns have identical names", "context": "col2"},
            {"error": "Two columns have identical names", "context": "col3"},
        ],
        key=lambda x: (x["error"], x.get("context", "")),
    )
