import requests
from config import BASE_URL
from utils import create_dummy_table


def test_delete_table(server):
    table_name = "test_delete_table"
    (table_id, _) = create_dummy_table(table_name)

    resp = requests.delete(f"{BASE_URL}/table/{table_id}")
    assert resp.status_code == 200


def test_delete_non_existence_table(server):
    id = "test_delete_non_existence_table"
    resp = requests.delete(f"{BASE_URL}/table/{id}")
    assert resp.status_code == 404
    assert resp.json() == {"message": "Couldn't find a table of given ID"}
