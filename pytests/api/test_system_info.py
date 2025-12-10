import time

import requests
from config import BASE_URL


def test_system_info(server):
    time.sleep(1)
    resp = requests.get(f"{BASE_URL}/system/info")
    assert resp.status_code == 200

    body = resp.json()
    assert body["version"]
    assert body["interfaceVersion"]
    assert body["author"] == "Jakub Kłos"
    assert body["uptime"] > 0
