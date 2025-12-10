import requests
from config import BASE_URL, QUERY_STATUSES


def test_list_queries_empty(server):
    resp = requests.get(f"{BASE_URL}/queries")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_queries(server):
    data = {"name": "test_list_queries", "columns": []}
    resp = requests.put(f"{BASE_URL}/table", json=data)
    assert resp.status_code == 200

    data = {"queryDefinition": {"tableName": "test_list_queries"}}
    resp = requests.post(f"{BASE_URL}/query", json=data)
    assert resp.status_code == 200
    query_id = resp.json()

    resp = requests.get(f"{BASE_URL}/queries")
    assert resp.status_code == 200

    body = resp.json()
    assert len(body) == 1
    assert body[0]["queryId"] == query_id
    assert body[0]["status"] in QUERY_STATUSES
