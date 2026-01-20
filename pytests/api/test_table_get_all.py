import requests
from config import BASE_URL


def test_list_tables_empty(server):
    resp = requests.get(f"{BASE_URL}/tables")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_tables(server):
    data = {"name": "test_list_tables", "columns": [
        {"name": "col1", "type": "VARCHAR"},
    ]}
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 200

    resp = requests.get(f"{BASE_URL}/tables")
    assert resp.status_code == 200

    body = resp.json()
    assert body[0]["tableId"]
    assert body[0]["name"] == "test_list_tables"
