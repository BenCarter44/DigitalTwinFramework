"""M0.1 -- teardown: terminal/idempotent stop, failure routing, no leaks."""

import asyncio

import pytest

from digitaltwin import (
    NULL_DTYPE,
    TRUTHY,
    Barrier,
    DTRuntime,
    DataType,
    RuntimeState,
    UtilityTask,
)

TICK = DataType("tick")

# DTRuntime never calls into the engine itself -- components do.  The unit
# tests use components which do not submit any work, so no engine is needed.
NO_FLOW = None


class Forever(UtilityTask):
    """A persistent component that never returns."""

    async def main_loop(self, runtime, in_data):
        while True:
            await asyncio.sleep(0.01)


class SlowToCancel(UtilityTask):
    """A component that does not settle when cancelled -- stop() must
    abandon it instead of waiting for it."""

    def __init__(self, flow, release: asyncio.Event):
        super().__init__(flow)
        self.release = release

    async def main_loop(self, runtime, in_data):
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await self.release.wait()


class Boom(UtilityTask):
    async def main_loop(self, runtime, in_data):
        raise ValueError("component exploded")


async def test_stop_is_terminal_and_idempotent(stream_clients):
    runtime = DTRuntime(NO_FLOW, await stream_clients("twin-a"))
    runtime.add_task(Forever(NO_FLOW), TRUTHY, TICK, is_persistent=True)

    assert runtime.state is RuntimeState.READY
    runtime.start()
    assert runtime.state is RuntimeState.RUNNING

    await asyncio.sleep(0.1)
    await runtime.stop()
    assert runtime.state is RuntimeState.STOPPED

    await runtime.stop()  # idempotent
    assert runtime.state is RuntimeState.STOPPED

    with pytest.raises(RuntimeError):
        runtime.start()


async def test_stop_cancels_tasks_and_tears_down_the_stream(
    stream_clients, no_task_leaks
):
    client = await stream_clients("twin-a")
    runtime = DTRuntime(NO_FLOW, client)

    barrier = Barrier("b")
    barrier.add_dtype(TICK)
    runtime.add_barrier(barrier)

    runtime.add_task(Forever(NO_FLOW), TRUTHY, TICK, is_persistent=True)
    runtime.start()

    # the persistent component subscribed the runtime to its output dtype
    await asyncio.sleep(0.3)
    assert runtime.running_tasks
    assert TICK in client.subscriptions

    await runtime.stop()

    # no lingering tasks, subscriptions, sockets or contexts
    assert runtime.running_tasks == set()
    assert client.subscriptions == set()
    assert client._backend.pub_soc is None
    assert client._backend.sub_soc is None
    assert client._backend._ctx.closed


async def test_stop_abandons_tasks_that_ignore_cancellation(
    stream_clients, no_task_leaks
):
    release = asyncio.Event()
    runtime = DTRuntime(NO_FLOW, await stream_clients("twin-a"))
    runtime.add_task(SlowToCancel(NO_FLOW, release), TRUTHY, TICK, is_persistent=True)
    runtime.start()
    await asyncio.sleep(0.1)

    # bounded: stop returns even though the component does not settle
    await asyncio.wait_for(runtime.stop(timeout=0.2), timeout=2.0)
    assert runtime.state is RuntimeState.STOPPED

    # the abandoned task is no longer owned by the runtime
    assert runtime.running_tasks == set()

    release.set()
    await asyncio.sleep(0.05)


async def test_component_failure_sets_failed_state(stream_clients):
    runtime = DTRuntime(NO_FLOW, await stream_clients("twin-a"))
    runtime.add_task(Boom(NO_FLOW), TRUTHY, NULL_DTYPE)
    runtime.start()

    await asyncio.sleep(0.1)
    assert runtime.state is RuntimeState.FAILED
    assert runtime.last_error == "ValueError: component exploded"

    # stop still works on a failed twin, and keeps the error
    await runtime.stop()
    assert runtime.state is RuntimeState.STOPPED
    assert runtime.last_error == "ValueError: component exploded"


async def test_stopped_runtime_starts_no_new_work(stream_clients):
    runtime = DTRuntime(NO_FLOW, await stream_clients("twin-a"))
    runtime.start()
    await runtime.stop()

    assert runtime._to_asyncio_task(asyncio.sleep, 0) is None
    assert runtime.running_tasks == set()


async def test_describe_is_serializable(stream_clients):
    import json

    runtime = DTRuntime(NO_FLOW, await stream_clients("twin-a"))
    inference = DataType("inference")

    runtime.add_task(Forever(NO_FLOW), TRUTHY, TICK, is_persistent=True)
    runtime.add_task(Forever(NO_FLOW), inference, NULL_DTYPE)

    barrier = Barrier("b", hard=False)
    barrier.add_dtype(TICK)
    runtime.add_barrier(barrier)

    info = runtime.describe()
    assert json.loads(json.dumps(info)) == info

    assert info["namespace"] == "twin-a"
    assert info["state"] == "ready"
    assert info["last_error"] is None
    assert sorted(info["dtypes"]) == ["NULL", "TRUE", "inference", "tick"]
    assert info["barriers"] == {"tick": [{"name": "b", "hard": False}]}
    assert info["components"] == [
        {
            "component": "Forever",
            "input_dtype": "TRUE",
            "output_dtype": "tick",
            "is_persistent": True,
        },
        {
            "component": "Forever",
            "input_dtype": "inference",
            "output_dtype": "NULL",
            "is_persistent": False,
        },
    ]

    # print_graph is a rendering of describe()
    assert "Forever" in runtime.print_graph()

    await runtime.stop()
