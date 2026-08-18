"""Unit tests for the `dt` plugin: policy, wire format, guards.

No ORBIT broker involved -- routes are exercised over Starlette's
`TestClient` (plugin route registration is dual), sessions and the
stream-broker supervisor directly.
"""

import asyncio

from typing import Optional

import pytest

pytest.importorskip("radical.orbit")

from fastapi import FastAPI, HTTPException  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from digitaltwin.components import UtilityTask  # noqa: E402
from digitaltwin.service.plugin import PluginDT  # noqa: E402
from digitaltwin.service.session import DTSession, TwinInstance  # noqa: E402
from digitaltwin.service.wire import (  # noqa: E402
    MAX_PAYLOAD,
    Package,
    check_versions,
    decode,
    encode,
    encode_checked,
    version_stamp,
)


@pytest.fixture
def plugin():
    """A `PluginDT` on a bare app -- no broker, no engines."""

    return PluginDT(FastAPI())


@pytest.fixture
def client(plugin):
    return TestClient(plugin._app)


# ---------------------------------------------------------------------------
# session policy
# ---------------------------------------------------------------------------

def test_sessions_are_forced_persistent(plugin, client):
    """Whatever the client asks for, twins must outlive it."""

    for body in ({}, {"lifetime": "ephemeral"}, {"lifetime": "ttl", "ttl": 5}):
        sid = client.post("/dt/register_session", json=body).json()["sid"]
        record = plugin._records[sid]

        assert record.lifetime == "persistent"
        assert record.ttl is None


def test_sid_is_a_bearer_capability(plugin, client):
    """A reconnecting client is a different participant; it must still
    get its own twins back."""

    resp = client.post("/dt/register_session", json={},
                       headers={"x-orbit-src": "client.1"})
    sid = resp.json()["sid"]
    assert plugin._records[sid].owner == "client.1"

    again = client.post("/dt/register_session", json={"sid": sid},
                        headers={"x-orbit-src": "client.2"})

    assert again.status_code == 200
    assert again.json() == {"sid": sid, "reattached": True}


def test_register_session_rejects_a_bad_config(client):
    resp = client.post("/dt/register_session", json={"config": "nope"})

    assert resp.status_code == 400
    assert "config" in resp.text


def test_admin_sessions_reports_policy_and_broker(plugin, client):
    sid = client.post("/dt/register_session", json={}).json()["sid"]
    listing = client.get("/dt/admin/sessions").json()

    entry = next(s for s in listing["sessions"] if s["sid"] == sid)
    assert entry["lifetime"] == "persistent"
    assert entry["twins"] == []
    assert entry["engines"] == []

    # nothing needed the stream broker yet, so none was started
    assert listing["stream_broker"] == {"addresses": None, "alive": False}


# ---------------------------------------------------------------------------
# routes and validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("twin_id", ["", "with/slash", "with space", None, 42])
def test_twin_create_rejects_bad_ids(client, twin_id):
    """A twin id is a stream namespace: a separator would let two twins
    alias each other's topics."""

    sid = client.post("/dt/register_session", json={}).json()["sid"]
    resp = client.post(f"/dt/twin_create/{sid}", json={"twin_id": twin_id})

    assert resp.status_code == 400


def test_twin_call_rejects_unknown_verbs(client):
    sid = client.post("/dt/register_session", json={}).json()["sid"]
    resp = client.post(f"/dt/twin_call/{sid}/t1",
                       json={"verb": "rm_rf", "payload": "", "client": {}})

    assert resp.status_code == 400
    assert "unknown verb" in resp.text


def test_twin_call_rejects_version_skew(client):
    sid = client.post("/dt/register_session", json={}).json()["sid"]
    resp = client.post(
        f"/dt/twin_call/{sid}/t1",
        json={"verb": "start", "payload": "",
              "client": {"python": "2.7", "cloudpickle": "0.1"}},
    )

    assert resp.status_code == 400
    assert "version skew" in resp.text


def test_twin_close_is_idempotent_for_unknown_twins(client):
    sid = client.post("/dt/register_session", json={}).json()["sid"]
    resp = client.post(f"/dt/twin_close/{sid}/never-existed")

    assert resp.status_code == 200
    assert resp.json()["state"] == "closed"


def test_unknown_session_is_404(client):
    assert client.get("/dt/twin_list/session.nope").status_code == 404


def test_an_unknown_session_never_unpickles_the_payload(client):
    """Decoding is arbitrary code execution: an unroutable verb must be
    turned away before its payload reaches cloudpickle.

    The payload here is not decodable at all -- a 400 would mean the
    service tried."""

    resp = client.post(
        "/dt/twin_call/session.nope/t1",
        json={"verb": "start", "payload": "!! not base64 !!",
              "client": version_stamp()},
    )

    assert resp.status_code == 404


async def test_a_call_on_a_closed_session_is_410():
    """Not a 500: a client racing its own `unregister_session` has
    simply outlived its session."""

    session = DTSession("s1")
    await session.close()

    with pytest.raises(HTTPException) as raised:
        await session.twin_call("t1", "start")

    assert raised.value.status_code == 410


async def test_a_malformed_call_is_a_client_error():
    """A hand-built payload with the wrong arity is a bad request, not a
    service fault."""

    session = DTSession("s1")
    session.twins["t1"] = _twin_with(_FakeFlow())

    with pytest.raises(HTTPException) as raised:
        await session.twin_call("t1", "start",
                                encode({"args": (1, 2, 3)}), version_stamp())

    assert raised.value.status_code == 409
    assert "TypeError" in raised.value.detail


# ---------------------------------------------------------------------------
# wire format
# ---------------------------------------------------------------------------

def test_encode_roundtrip():
    payload = {"args": (1, "two", [3]), "kwargs": {"k": {"nested": True}}}

    assert decode(encode(payload)) == payload


def test_size_check_refuses_oversized_payloads():
    with pytest.raises(ValueError, match="frame cap"):
        encode_checked(bytearray(MAX_PAYLOAD), "test payload")


def test_size_check_passes_a_normal_payload():
    assert encode_checked({"args": (1, 2)}, "test payload")


def test_version_stamp_matches_itself():
    check_versions(version_stamp())


@pytest.mark.parametrize("key, value", [
    ("python", "2.7"),
    ("cloudpickle", "0.1"),
    # by-reference pickling of component classes: any difference counts
    ("digitaltwin", version_stamp()["digitaltwin"] + ".dev1"),
])
def test_version_skew_is_rejected(key, value):
    with pytest.raises(ValueError, match=f"{key} version skew"):
        check_versions({**version_stamp(), key: value})


@pytest.mark.parametrize("missing", ["python", "cloudpickle", "digitaltwin"])
def test_a_missing_version_is_rejected(missing):
    stamp = {k: v for k, v in version_stamp().items() if k != missing}

    with pytest.raises(ValueError, match=f"did not report its {missing}"):
        check_versions(stamp)


def test_the_stamp_pins_digitaltwin_too():
    assert "digitaltwin" in version_stamp()


def test_package_instantiates_with_the_injected_engine():
    class Component:
        def __init__(self, flow, a, b=0):
            self.flow, self.a, self.b = flow, a, b

    component = Package(Component, (1,), {"b": 2}).instantiate("engine")

    assert (component.flow, component.a, component.b) == ("engine", 1, 2)


# ---------------------------------------------------------------------------
# the persistent-component guard
# ---------------------------------------------------------------------------

class _FakeFlow:
    """Just enough engine for `_instantiate` and teardown."""

    def __init__(self):
        self.registered = []
        self.is_shut_down = False

    def function_task(self, func):
        self.registered.append(func)
        return func

    async def shutdown(self):
        self.is_shut_down = True


class _Persistent(UtilityTask):
    def __init__(self, flow):
        super().__init__(flow)

        @flow.function_task
        async def body():
            return 1

        self.body = body


class _Plain(UtilityTask):
    pass


def _twin_with(flow):
    twin = TwinInstance("t1")
    twin.runtime = type("R", (), {"flow": flow})()

    return twin


def test_persistent_function_task_warns(caplog):
    flow = _FakeFlow()
    session = DTSession("s1")

    with caplog.at_level("WARNING"):
        session._instantiate(Package(_Persistent), _twin_with(flow),
                             is_persistent=True)

    assert "registered 1 function_task" in caplog.text
    # the engine is handed back unpatched
    assert flow.function_task.__func__ is _FakeFlow.function_task


def test_non_persistent_function_task_is_fine(caplog):
    session = DTSession("s1")

    with caplog.at_level("WARNING"):
        session._instantiate(Package(_Persistent), _twin_with(_FakeFlow()))

    assert "function_task" not in caplog.text


def test_persistent_without_function_tasks_is_fine(caplog):
    session = DTSession("s1")

    with caplog.at_level("WARNING"):
        session._instantiate(Package(_Plain), _twin_with(_FakeFlow()),
                             is_persistent=True)

    assert "function_task" not in caplog.text


def test_instantiate_rejects_a_non_package():
    with pytest.raises(ValueError, match="package"):
        DTSession("s1")._instantiate(_Plain, _twin_with(_FakeFlow()))


# ---------------------------------------------------------------------------
# engines
# ---------------------------------------------------------------------------

def _slow_build(session, delay: float, started: Optional[asyncio.Event] = None):
    """Stand in for a real (up to 150 s) backend build."""

    built = []

    async def build(name):
        if started is not None:
            started.set()
        await asyncio.sleep(delay)
        flow = _FakeFlow()
        built.append(flow)
        return flow

    session._create_engine = build

    return built


async def test_one_engine_is_shared_by_every_caller():
    session = DTSession("s1")
    built = _slow_build(session, 0.05)

    first, second = await asyncio.gather(session.engine(), session.engine())

    assert first is second
    assert len(built) == 1


async def test_an_engine_build_survives_a_cancelled_caller():
    """A twin whose initialization is cancelled halfway must not take the
    build with it -- that would strand a live backend nobody holds."""

    session = DTSession("s1")
    started = asyncio.Event()
    built = _slow_build(session, 0.2, started)

    caller = asyncio.create_task(session.engine())
    await started.wait()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller

    flow = await session.engine()

    assert built == [flow]
    assert session._engines == {"task": flow}


async def test_an_engine_landing_after_close_disposes_of_itself():
    """Nothing will ever own it, so it must not outlive its own build."""

    session = DTSession("s1")
    built = _slow_build(session, 0.2)

    caller = asyncio.create_task(session.engine())
    await asyncio.sleep(0.05)

    await session.close()

    with pytest.raises((RuntimeError, asyncio.CancelledError)):
        await caller

    assert len(built) == 1
    assert built[0].is_shut_down
    assert session._engines == {}


async def test_close_shuts_down_a_built_engine():
    session = DTSession("s1")
    _slow_build(session, 0)

    flow = await session.engine()
    await session.close()

    assert flow.is_shut_down
    assert session._engines == {}


# ---------------------------------------------------------------------------
# the embedded stream broker and its supervisor
# ---------------------------------------------------------------------------

async def test_stream_broker_starts_on_loopback_once(plugin):
    """One broker per plugin, shared by every twin, bound to loopback."""

    try:
        first = await plugin.stream_addresses()
        second = await plugin.stream_addresses()

        assert first == second
        assert all(addr.startswith("tcp://127.0.0.1:") for addr in first)
        assert plugin._stream_broker.is_alive()

    finally:
        await plugin.shutdown()


async def test_supervisor_respawns_on_the_same_addresses(plugin, monkeypatch):
    """A silently dead stream broker would stall every twin's stream."""

    monkeypatch.setattr("digitaltwin.service.plugin.BROKER_WATCH_INTERVAL", 0.1)

    try:
        addrs = await plugin.stream_addresses()
        first_pid = plugin._stream_broker._proc.pid

        plugin._stream_broker._proc.kill()
        await asyncio.wait_for(_respawned(plugin, first_pid), 20)

        assert plugin._stream_broker.get_connection_str() == addrs
        assert plugin._stream_broker.is_alive()

    finally:
        await plugin.shutdown()


async def _respawned(plugin, old_pid):
    """Wait for a live successor.  Observed under the plugin's own lock,
    so a respawn in progress is never seen half-started."""

    while True:
        async with plugin._stream_lock:
            proc = plugin._stream_broker._proc
            if proc is not None and proc.pid != old_pid and proc.is_alive():
                return
        await asyncio.sleep(0.05)


async def test_shutdown_stops_the_broker_and_the_supervisor(plugin):
    await plugin.stream_addresses()
    broker, supervisor = plugin._stream_broker, plugin._supervisor

    await plugin.shutdown()

    assert not broker.is_alive()
    assert supervisor.done()
    assert plugin._stream_broker is None
