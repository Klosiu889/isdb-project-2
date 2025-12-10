import csv
import os

import pytest
import requests
from config import BASE_URL
from utils import create_table, get_error_message, wait_for_final_status


@pytest.fixture
def test_csv_path():
    file_path = os.path.join(os.getcwd(), "data", "test_copy_fails.csv")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    data = [["10", "abc", "20"], ["30", "def", "40"]]

    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)

    return file_path


def test_fail_column_count_mismatch_no_mapping(server, test_csv_path):
    table_name = "width_mismatch"
    create_table(
        table_name,
        [{"name": "c1", "type": "INT64"}, {"name": "c2", "type": "VARCHAR"}],
    )

    data = {
        "queryDefinition": {
            "sourceFilepath": test_csv_path,
            "destinationTableName": table_name,
        }
    }
    resp = requests.post(f"{BASE_URL}/query", json=data)
    query_id = resp.json()

    status = wait_for_final_status(query_id)
    assert status == "FAILED"

    err = get_error_message(query_id)
    assert (
        err
        == "Mismatch: Table has 2 columns, but CSV has 3. Without mapping, counts must match exactly."
    )


def test_fail_mapping_bad_column_name(server, test_csv_path):
    table_name = "bad_map_name"
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
            "destinationColumns": ["c1", "ghost_col", "c3"],
        }
    }
    resp = requests.post(f"{BASE_URL}/query", json=data)
    query_id = resp.json()

    status = wait_for_final_status(query_id)
    assert status == "FAILED"

    err = get_error_message(query_id)
    assert err == "Mapping references column 'ghost_col', which does not exist in table"


def test_fail_mapping_length_mismatch(server, test_csv_path):
    table_name = "bad_map_len"
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
            "destinationColumns": ["c1", "c2"],
        }
    }
    resp = requests.post(f"{BASE_URL}/query", json=data)
    query_id = resp.json()

    status = wait_for_final_status(query_id)
    assert status == "FAILED"

    err = get_error_message(query_id)
    assert err == "Mapping have different number of rows then destination table"


def test_fail_type_mismatch(server, test_csv_path):
    table_name = "type_fail"
    create_table(
        table_name,
        [
            {"name": "c1", "type": "INT64"},
            {"name": "c2", "type": "INT64"},
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
    query_id = resp.json()

    status = wait_for_final_status(query_id)
    assert status == "FAILED"

    err = get_error_message(query_id)
    assert err == "Type Error at Row 1, Column 'c2': Expected INT64, got 'abc'"


def test_fail_csv_too_narrow(server, test_csv_path):
    tiny_csv = os.path.join(os.getcwd(), "data", "tiny.csv")
    with open(tiny_csv, "w") as f:
        f.write("100\n200\n")

    table_name = "csv_narrow"
    create_table(
        table_name, [{"name": "c1", "type": "INT64"}, {"name": "c2", "type": "INT64"}]
    )

    data = {
        "queryDefinition": {
            "sourceFilepath": tiny_csv,
            "destinationTableName": table_name,
            "destinationColumns": ["c1", "c2"],
        }
    }
    resp = requests.post(f"{BASE_URL}/query", json=data)
    query_id = resp.json()

    status = wait_for_final_status(query_id)
    assert status == "FAILED"

    err = get_error_message(query_id)
    assert err == "CSV too narrow: Mapping requires 2 columns, but CSV only has 1."
