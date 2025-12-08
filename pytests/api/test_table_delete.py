import requests


def test_delete_table(server, base_url):
    data = {
        "name": "test_delete_table",
        "columns": []
    }
    resp = requests.put(f"{base_url}/table", json=data)
    assert resp.status_code == 200
    id = resp.json()

    resp = requests.delete(f"{base_url}/table/{id}")
    assert resp.status_code == 200


def test_delete_non_existence_table(server, base_url):
    id = "test_delete_non_existence_table"
    resp = requests.delete(f"{base_url}/table/{id}")
    assert resp.status_code == 404
    assert resp.json() == { "message": "Couldn't find a table of given ID" }
