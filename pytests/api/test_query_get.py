import requests
from config import BASE_URL, QUERY_STATUSES
from utils import create_dummy_table


def test_get_query_select(server):
    table_name = "test_get_query_select"
    create_dummy_table(table_name)

    data = {"queryDefinition": {"tableName": table_name}}
    resp = requests.post(f"{BASE_URL}/query", json=data)
    assert resp.status_code == 200
    query_id = resp.json()

    resp = requests.get(f"{BASE_URL}/query/{query_id}")
    assert resp.status_code == 200

    body = resp.json()
    assert body["queryId"]
    assert body["status"] in QUERY_STATUSES
    assert body["queryDefinition"]["tableName"] == table_name


def test_get_non_existence_query(server):
    queryid = "test_get_non_existence_query"
    resp = requests.get(f"{BASE_URL}/query/{queryid}")
    assert resp.status_code == 404

    body = resp.json()
    assert body["message"] == "Couldn't find a query of given ID"
