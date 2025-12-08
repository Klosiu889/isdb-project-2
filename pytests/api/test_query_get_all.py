import requests 


def test_list_queries_empty(server, base_url):
    resp = requests.get(f"{base_url}/queries")
    assert resp.status_code == 200
    assert resp.json() == []
