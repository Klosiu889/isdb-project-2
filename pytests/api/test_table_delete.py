import requests
from config import BASE_URL


def test_delete_table(server):
    data = {"name": "test_delete_table", "columns": []}
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 200
    id = resp.json()

    resp = requests.delete(f"{BASE_URL}/table/{id}")
    assert resp.status_code == 200


def test_delete_non_existence_table(server):
    id = "test_delete_non_existence_table"
    resp = requests.delete(f"{BASE_URL}/table/{id}")
    assert resp.status_code == 404
    assert resp.json() == {"message": "Couldn't find a table of given ID"}
