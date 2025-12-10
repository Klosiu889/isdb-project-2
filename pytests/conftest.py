import os
import pytest
import time
import sys
import subprocess

sys.path.append(os.path.dirname(__file__))

@pytest.fixture(scope="module")
def server():
    proc = subprocess.Popen(["cargo", "run"])

    time.sleep(1)

    yield proc

    proc.terminate()
    proc.wait()

