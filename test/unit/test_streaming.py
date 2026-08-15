"""M0.2 -- topic namespacing and stream client teardown."""

import asyncio

import pytest

from digitaltwin import DataType, PubSubClient

SENSOR = DataType("sensor")


async def _drain(queue: asyncio.Queue, timeout=5.0):
    return await asyncio.wait_for(queue.get(), timeout)


def test_topics_are_namespaced_and_terminated():
    a = PubSubClient(backend=None, namespace="twin-a")
    b = PubSubClient(backend=None, namespace="twin-b")

    assert a.topic(SENSOR) == "dt/twin-a/dtypes/sensor\x00"
    assert a.topic(SENSOR) != b.topic(SENSOR)

    # the terminator keeps a label from prefix-matching a longer one
    assert not a.topic(DataType("sen")).startswith(a.topic(SENSOR)[:-1])
    assert not a.topic(SENSOR).startswith(a.topic(DataType("sen")))


async def test_identical_dtype_labels_do_not_cross_subscribe(stream_clients):
    """The multi-tenancy correctness target: same dtype label, two
    namespaces, one broker -- no crosstalk."""

    twin_a = await stream_clients("twin-a")
    twin_b = await stream_clients("twin-b")

    queue_a: asyncio.Queue = asyncio.Queue()
    queue_b: asyncio.Queue = asyncio.Queue()

    await twin_a.subscribe_to_dtype(SENSOR, queue_a)
    await twin_b.subscribe_to_dtype(SENSOR, queue_b)
    await asyncio.sleep(0.2)  # let the subscriptions reach the broker

    for i in range(3):
        await twin_a.publish(SENSOR, f"a-{i}")

    received = await _drain(queue_a)
    assert received.dtype == SENSOR
    assert received.data == "a-0"

    await asyncio.sleep(0.2)
    assert queue_b.empty()


async def test_unsubscribe_dtype_stops_delivery(stream_clients):
    twin = await stream_clients("twin-a")
    queue: asyncio.Queue = asyncio.Queue()

    await twin.subscribe_to_dtype(SENSOR, queue)
    await asyncio.sleep(0.2)

    await twin.publish(SENSOR, "first")
    assert (await _drain(queue)).data == "first"

    await twin.unsubscribe_dtype(SENSOR)
    assert SENSOR not in twin.subscriptions
    await asyncio.sleep(0.2)

    await twin.publish(SENSOR, "second")
    await asyncio.sleep(0.2)
    assert queue.empty()

    # re-subscription is possible after unsubscribe
    await twin.subscribe_to_dtype(SENSOR, queue)


def test_namespace_must_not_alias_topics():
    for namespace in ("", "twin/a", "twin\x00a"):
        with pytest.raises(ValueError):
            PubSubClient(backend=None, namespace=namespace)


async def test_receive_loop_survives_bad_payloads_and_callbacks(stream_clients):
    """A malformed message or a raising callback must not stall the stream."""

    twin = await stream_clients("twin-a")
    backend = twin._backend
    queue: asyncio.Queue = asyncio.Queue()

    async def raises(message):
        raise ValueError("bad callback")

    await backend.subscribe(twin.topic(SENSOR), raises)
    await twin.subscribe_to_dtype(SENSOR, queue)
    await asyncio.sleep(0.2)

    # not a pickle
    await backend.pub_soc.send_multipart([twin.topic(SENSOR).encode(), b"garbage"])
    await asyncio.sleep(0.2)

    # the raising callback fires on this one, the queue still gets it
    await twin.publish(SENSOR, "good")
    assert (await _drain(queue)).data == "good"

    assert not backend._task.done()


async def test_unexpected_receive_loop_exit_is_reported(stream_clients):
    twin = await stream_clients("twin-a")
    backend = twin._backend

    seen: list[BaseException] = []

    def report(exc):
        seen.append(exc)

    twin.on_error = report
    assert backend.on_error is report

    # a receive loop which ends without close() means a silent stall
    ended = asyncio.create_task(asyncio.sleep(0))
    await ended
    backend._run_done(ended)

    assert len(seen) == 1
    assert isinstance(seen[0], RuntimeError)

    # ... but the loop ending as part of close() is not an error
    await twin.close()
    backend._run_done(ended)
    assert len(seen) == 1


async def test_close_releases_task_sockets_and_context(broker, no_task_leaks):
    from digitaltwin import connect_stream_client

    twin = await connect_stream_client("twin-a", *broker.get_connection_str())
    backend = twin._backend

    await twin.subscribe_to_dtype(SENSOR, asyncio.Queue())

    await twin.close()

    assert twin.subscriptions == set()
    assert backend.topics == {}
    assert backend.pub_soc is None and backend.sub_soc is None
    assert backend._ctx.closed

    # idempotent, and a closed client refuses further use
    await twin.close()
    with pytest.raises(RuntimeError):
        await twin.publish(SENSOR, "nope")
