import csv
import os

import pytest
import requests
from config import BASE_URL
from utils import create_table, wait_for_final_status


@pytest.fixture
def test_csv_path():
    file_path = os.path.join(os.getcwd(), "data", "test_copy_fails.csv")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    data = [["10", "abc", "20"], ["30", "def", "40"]]

    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)

    return file_path


def test_copy_success_trivial(server, test_csv_path):
    table_name = "copy_success"
    create_table(
        table_name,
        [
            {"name": "c1", "type": "INT64"},
            {"name": "c2", "type": "VARCHAR"},
            {"name": "c3", "type": "INT64"},
        ],
    )

    data = {
        "queryDefinition": {
            "sourceFilepath": test_csv_path,
            "destinationTableName": table_name,
        }
    }
    resp = requests.post(f"{BASE_URL}/query", json=data)
    assert resp.status_code == 200

    status = wait_for_final_status(resp.json())
    assert status == "COMPLETED"


def test_fail_file_not_found(server):
    create_table("file_fail_table", [{"name": "c1", "type": "INT64"}])

    data = {
        "queryDefinition": {
            "sourceFilepath": "ghost_file.csv",
            "destinationTableName": "file_fail_table",
        }
    }
    resp = requests.post(f"{BASE_URL}/query", json=data)

    assert resp.status_code == 400
    body = resp.json()
    assert body["problems"]
    assert len(body["problems"]) == 1
    assert body["problems"][0]["error"] == "File does not exist"
    assert body["problems"][0]["context"] == "ghost_file.csv"
