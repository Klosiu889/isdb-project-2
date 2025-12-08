import requests
import time


def test_system_info(server, base_url):
    time.sleep(1)
    resp = requests.get(f"{base_url}/system/info")
    assert resp.status_code == 200

    body = resp.json()
    assert body["version"]
    assert body["interfaceVersion"]
    assert body["author"] == "Jakub Kłos"
    assert body["uptime"] > 0
