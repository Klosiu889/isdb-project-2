import requests
from config import BASE_URL
from utils import create_dummy_table, wait_for_status


def test_get_query_result_success(server):
    table_name = "test_get_query_result_success"
    (_, table_data) = create_dummy_table(table_name)

    data = {"queryDefinition": {"tableName": table_name}}
    resp = requests.post(f"{BASE_URL}/query", json=data)
    query_id = resp.json()

    final_status = wait_for_status(query_id, ["COMPLETED"])
    assert final_status == "COMPLETED"

    resp = requests.get(f"{BASE_URL}/result/{query_id}")
    assert resp.status_code == 200

    body = resp.json()
    assert len(body) == 1
    assert body[0]["rowCount"] == 0
    assert len(body[0]["columns"]) == len(table_data["columns"])


def test_get_result_non_existent(server):
    id = "test_get_result_non_existent"
    resp = requests.get(f"{BASE_URL}/result/{id}")
    assert resp.status_code == 404
