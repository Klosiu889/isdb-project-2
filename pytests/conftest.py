import pytest
import time
import subprocess


@pytest.fixture
def base_url():
    return "http://127.0.0.1:8080"


@pytest.fixture(scope="module")
def server():
    proc = subprocess.Popen(["cargo", "run"])

    time.sleep(1)

    yield proc

    proc.terminate()
    proc.wait()

