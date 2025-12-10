import requests
from config import BASE_URL


def test_get_table(server):
    data = {
        "name": "test_get_table",
        "columns": [
            {"name": "col1", "type": "VARCHAR"},
            {"name": "col2", "type": "INT64"},
        ],
    }
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 200
    id = resp.json()

    resp = requests.get(f"{BASE_URL}/table/{id}")
    assert resp.status_code == 200

    body = resp.json()
    assert body["name"] == data["name"]
    assert sorted(body["columns"], key=lambda x: (x["name"], x["type"])) == sorted(
        data["columns"], key=lambda x: (x["name"], x["type"])
    )


def test_get_non_existence_table(server):
    id = "test_get_non_existence_table"
    resp = requests.get(f"{BASE_URL}/table/{id}")
    assert resp.status_code == 404
    assert resp.json() == {"message": "Couldn't find a table of given ID"}


def test_get_table_after_delete(server):
    data = {"name": "test_get_table_after_delete", "columns": []}
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 200
    id = resp.json()

    resp = requests.delete(f"{BASE_URL}/table/{id}")
    assert resp.status_code == 200

    resp = requests.get(f"{BASE_URL}/table/{id}")
    assert resp.status_code == 404
    assert resp.json() == {"message": "Couldn't find a table of given ID"}
