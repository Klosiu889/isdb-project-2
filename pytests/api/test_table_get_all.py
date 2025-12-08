import requests 


def test_list_tables_empty(server, base_url):
    resp = requests.get(f"{base_url}/tables")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_tables(server, base_url):
    data = {
        "name": "test_list_tables",
        "columns": []
    }
    resp = requests.put(f"{base_url}/table", json=data)
    assert resp.status_code == 200

    resp = requests.get(f"{base_url}/tables")
    assert resp.status_code == 200

    body = resp.json()
    assert body[0]["tableId"]
    assert body[0]["name"] == "test_list_tables"
