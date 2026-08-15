"""A live DTaaS stack for the integration tests.

Session-scoped fixtures bring up, on loopback:

- an ORBIT broker hosting the `dt` plugin (`--plugins default,dt`),
- a co-located rhapsody endpoint with `backends=['concurrent']` and the
  notification window at 0 (P2 -- otherwise every sequential task pays
  250 ms),
- a consumer runtime the tests get `DTClient`s from.

Everything is skipped when the broker cannot be started (no certs, no
token, port taken), so the suite stays runnable without a deployment.
"""

import os
import shutil
import socket
import subprocess
import sys
import time
import uuid

from pathlib import Path

import pytest

try:
    import httpx

    from radical.orbit import EndpointRuntime
except ImportError:  # no ORBIT installed: there is nothing here to run
    collect_ignore_glob = ["*"]

REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "src"

BROKER_HOST = "127.0.0.1"
BROKER_PORT = int(os.environ.get("DT_TEST_BROKER_PORT", "8031"))
BROKER_URL = f"https://{BROKER_HOST}:{BROKER_PORT}"

TASK_ENDPOINT = "dt_test_task_ep"
DT_ENDPOINT = "dt_test_dt_ep"  # endpoint-hosted `dt`, for the smoke test

STARTUP_TIMEOUT = 60.0
LOGS = Path(os.environ.get("DT_TEST_LOG_DIR", "/tmp")) / "dt-integration-logs"

# engine wiring every test uses: the co-located endpoint, concurrent backend
ENGINES = {
    "engines": {
        "task": {"endpoint_name": TASK_ENDPOINT, "backends": ["concurrent"]}
    }
}


def _port_free(port: int) -> bool:
    with socket.socket() as sock:
        sock.settimeout(0.5)
        return sock.connect_ex((BROKER_HOST, port)) != 0


def _child_env(**extra: str) -> dict:
    """Environment for a broker / endpoint child process.

    `src` goes first on PYTHONPATH so the children run the working tree,
    not whatever `digitaltwin` happens to be installed.
    """

    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(SRC), *filter(None, [env.get("PYTHONPATH")])]
    )
    env["RADICAL_ORBIT_BROKER_URL"] = BROKER_URL
    env.update(extra)

    return env


def _spawn(name: str, argv: list, **env: str) -> subprocess.Popen:
    LOGS.mkdir(parents=True, exist_ok=True)
    log = (LOGS / f"{name}.log").open("w")

    return subprocess.Popen(
        argv, env=_child_env(**env), stdout=log, stderr=subprocess.STDOUT
    )


def _terminate(proc: subprocess.Popen, timeout: float = 15.0) -> None:
    if proc.poll() is not None:
        return

    proc.terminate()
    try:
        proc.wait(timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(5)


def _orbit_script(name: str) -> list:
    """Locate an ORBIT entry script (installed, or in a source checkout)."""

    import radical.orbit

    candidates = [
        Path(sys.executable).parent / name,  # this interpreter's env
        # a source checkout: .../site-packages/radical/orbit -> ../../../bin
        Path(radical.orbit.__file__).resolve().parents[3] / "bin" / name,
    ]
    for script in candidates:
        if script.exists():
            return [sys.executable, str(script)]

    installed = shutil.which(name)
    if installed:
        return [installed]

    pytest.skip(f"cannot locate {name}")


@pytest.fixture(scope="session")
def broker():
    """An ORBIT broker on a non-default loopback port, hosting `dt`."""

    if not _port_free(BROKER_PORT):
        pytest.skip(f"port {BROKER_PORT} is busy")

    argv = _orbit_script("radical-orbit-broker.py") + [
        "--host", BROKER_HOST,
        "--port", str(BROKER_PORT),
        "--plugins", "default,dt",
    ]
    proc = _spawn("broker", argv)

    try:
        _await_broker(proc)
        yield BROKER_URL
    finally:
        _terminate(proc)


def _await_broker(proc: subprocess.Popen) -> None:
    deadline = time.time() + STARTUP_TIMEOUT

    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.skip(f"broker exited early -- see {LOGS / 'broker.log'}")
        try:
            httpx.get(BROKER_URL + "/", verify=False, timeout=2)
            return
        except Exception:
            time.sleep(0.25)

    _terminate(proc)
    pytest.skip(f"broker did not come up -- see {LOGS / 'broker.log'}")


@pytest.fixture(scope="session")
def task_endpoint(broker):
    """A co-located rhapsody endpoint: where the twins' tasks execute."""

    argv = _orbit_script("radical-orbit-endpoint.py") + [
        "-n", TASK_ENDPOINT, "-u", broker, "-p", "default",
    ]
    proc = _spawn(
        TASK_ENDPOINT,
        argv,
        RADICAL_ORBIT_RHAPSODY_BACKEND="concurrent",
        RADICAL_ORBIT_RHAPSODY_NOTIFY_WINDOW="0",
    )

    try:
        _await_plugin(TASK_ENDPOINT, "rhapsody", proc)
        yield TASK_ENDPOINT
    finally:
        _terminate(proc)


@pytest.fixture(scope="session")
def dt_endpoint(broker):
    """An endpoint hosting the `dt` plugin itself (endpoint-hosted mode).

    It deliberately does *not* load rhapsody: its twins' tasks go to
    `task_endpoint`, exactly as in the broker-hosted deployment.
    """

    argv = _orbit_script("radical-orbit-endpoint.py") + [
        "-n", DT_ENDPOINT, "-u", broker, "-p", "dt",
    ]
    proc = _spawn(DT_ENDPOINT, argv)

    try:
        _await_plugin(DT_ENDPOINT, "dt", proc)
        yield DT_ENDPOINT
    finally:
        _terminate(proc)


def _await_plugin(endpoint: str, plugin: str, proc: subprocess.Popen) -> None:
    """Wait until `endpoint` advertises `plugin` in the broker topology."""

    runtime = EndpointRuntime(broker_url=BROKER_URL)
    runtime.start(wait=True)

    try:
        deadline = time.time() + STARTUP_TIMEOUT
        while time.time() < deadline:
            if proc.poll() is not None:
                pytest.skip(f"{endpoint} exited -- see {LOGS / endpoint}.log")

            info = runtime.topology().get(endpoint) or {}
            if plugin in (info.get("plugins") or {}):
                return

            time.sleep(0.25)

    finally:
        runtime.stop()

    _terminate(proc)
    pytest.skip(f"{endpoint} never advertised {plugin!r}")


@pytest.fixture(scope="session")
def stack(broker, task_endpoint):
    """The full broker + endpoint stack; returns the broker URL."""

    return broker


@pytest.fixture
def runtime(stack):
    """A fresh consumer runtime -- one per test, like a real client."""

    rt = EndpointRuntime(broker_url=stack)
    rt.start(wait=True)
    try:
        yield rt
    finally:
        rt.stop()


@pytest.fixture
def dt(runtime):
    """A `DTClient` on a fresh session, torn down with the test."""

    client = runtime.get_plugin("broker", "dt", config=ENGINES)
    try:
        yield client
    finally:
        _drop_session(client)


def _drop_session(client) -> None:
    """Close every twin, then unregister -- sessions are immortal."""

    try:
        for twin in client.twin_list():
            client.twin_close(twin["twin_id"])
        client.unregister_session()
    except Exception as exc:  # a test may have closed it already
        print(f"session teardown: {exc}")


@pytest.fixture
def twin_id():
    """A fresh client-supplied twin uuid."""

    return str(uuid.uuid4())
