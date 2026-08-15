## Streaming Handler

## Actual streaming backend is pluggable.

import asyncio
import contextlib
import logging
import multiprocessing

from abc import ABC, abstractmethod
from typing import Callable, Optional

import cloudpickle
import zmq
import zmq.asyncio

from zmq.utils.monitor import recv_monitor_message

from .components import DataType, TypedData
from .config import RANDOM_PUB_ADDR, RANDOM_SUB_ADDR, stream_addresses

logger = logging.getLogger(__name__)

# bounded waits: no teardown path may hang the host event loop
BROKER_START_TIMEOUT = 30.0
BROKER_STOP_TIMEOUT = 5.0


class PubSubBackend(ABC):
    label = "generic"

    def __init__(self):
        # asynchronous failures (a dead receive loop above all) are reported
        # here -- see PubSubClient.on_error.  A silently stalled stream is
        # the failure mode this exists to prevent.
        self.on_error: Optional[Callable[[BaseException], None]] = None

    def _report_error(self, exc: BaseException):
        logger.error("stream backend failed: %s", exc, exc_info=exc)

        if self.on_error is not None:
            self.on_error(exc)

    @abstractmethod
    async def connect(self, *args, **kwargs):
        pass

    @abstractmethod
    async def publish(self, topic, message, **kwargs):
        pass

    @abstractmethod
    async def subscribe(self, topic, callback, **kwargs):
        pass

    @abstractmethod
    async def unsubscribe(self, topic):
        pass

    @abstractmethod
    async def close(self):
        """Release all resources.  Idempotent."""

    def __str__(self):
        return f"{self.label}"


# Use ZMQ for the broker
class ZMQ_Broker:
    """XSUB/XPUB proxy.  `run()` blocks -- it is meant to own its process.

    Addresses default to a random port on loopback (see `config`); the
    actually bound addresses are available from `get_connection_str()`
    once `bind()` ran.
    """

    def __init__(
        self, publish_addr: Optional[str] = None, subscribe_addr: Optional[str] = None
    ):
        self.publish_addr = publish_addr or RANDOM_PUB_ADDR
        self.subscribe_addr = subscribe_addr or RANDOM_SUB_ADDR

        self.ctx: Optional[zmq.Context] = None
        self.pub_recv: Optional[zmq.Socket] = None
        self.sub_send: Optional[zmq.Socket] = None

    def bind(self) -> tuple[str, str]:
        """Create the sockets and bind them.  Returns the bound addresses.

        Must run in the process that will run the proxy -- a ZMQ context
        does not survive a fork/spawn.
        """

        self.ctx = zmq.Context()
        self.pub_recv = self.ctx.socket(zmq.XSUB)
        self.sub_send = self.ctx.socket(zmq.XPUB)

        self.pub_recv.bind(self.publish_addr)
        self.sub_send.bind(self.subscribe_addr)

        # resolve wildcard ports to what the OS actually handed out
        self.publish_addr = self.pub_recv.getsockopt_string(zmq.LAST_ENDPOINT)
        self.subscribe_addr = self.sub_send.getsockopt_string(zmq.LAST_ENDPOINT)

        return self.get_connection_str()

    def run(self):
        if self.ctx is None:
            self.bind()

        try:
            zmq.proxy(self.pub_recv, self.sub_send)
        except zmq.ContextTerminated:
            pass
        finally:
            self.pub_recv.close(linger=0)
            self.sub_send.close(linger=0)
            self.ctx.term()

    def get_connection_str(self) -> tuple[str, str]:
        return self.publish_addr, self.subscribe_addr


def _broker_main(publish_addr, subscribe_addr, conn):
    """Entry point of the broker subprocess (must be importable for spawn)."""

    broker = ZMQ_Broker(publish_addr, subscribe_addr)
    conn.send(broker.bind())
    conn.close()
    broker.run()


class ZMQ_BrokerProcess:
    """A `ZMQ_Broker` embedded as a spawn-context subprocess.

    The subprocess boundary is the stop path: `zmq.proxy()` has none of
    its own.  The child binds (random port by default), reports the bound
    addresses back to the parent, and is stopped by terminate/join.
    """

    def __init__(
        self, publish_addr: Optional[str] = None, subscribe_addr: Optional[str] = None
    ):
        self._addrs = (
            publish_addr or RANDOM_PUB_ADDR,
            subscribe_addr or RANDOM_SUB_ADDR,
        )
        self._proc: Optional[multiprocessing.process.BaseProcess] = None

        # serializes start/stop: concurrent starts must not spawn two
        # brokers, and must not observe a half-started one
        self._lock = asyncio.Lock()

        self.publish_addr: Optional[str] = None
        self.subscribe_addr: Optional[str] = None

    async def start(self, timeout: float = BROKER_START_TIMEOUT) -> tuple[str, str]:
        """Spawn the broker and return its bound (publish, subscribe) pair."""

        async with self._lock:
            if self._proc is not None:
                return self.get_connection_str()

            # spawn + pipe read are blocking: keep them off the event loop
            addrs = await asyncio.to_thread(self._start, timeout)
            self.publish_addr, self.subscribe_addr = addrs
            logger.info("stream broker at %s / %s", *addrs)

            return addrs

    async def stop(self, timeout: float = BROKER_STOP_TIMEOUT):
        """Terminate the broker subprocess.  Idempotent."""

        async with self._lock:
            if self._proc is None:
                return

            await asyncio.to_thread(self._stop, timeout)
            self.publish_addr = self.subscribe_addr = None

    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.is_alive()

    def get_connection_str(self) -> tuple[str, str]:
        return self.publish_addr, self.subscribe_addr

    def _start(self, timeout):
        ctx = multiprocessing.get_context("spawn")
        recv_conn, send_conn = ctx.Pipe(duplex=False)

        self._proc = ctx.Process(
            target=_broker_main, args=(*self._addrs, send_conn), daemon=True
        )
        try:
            self._proc.start()
            send_conn.close()  # only the child keeps the write end

            if not recv_conn.poll(timeout):
                raise TimeoutError(f"stream broker did not bind within {timeout}s")

            return recv_conn.recv()

        except BaseException:
            self._stop(BROKER_STOP_TIMEOUT)
            raise

        finally:
            recv_conn.close()

    def _stop(self, timeout):
        proc, self._proc = self._proc, None

        if proc.pid is not None:  # None if start() itself failed
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout)

            if proc.is_alive():
                logger.warning("stream broker ignored terminate -- killing")
                proc.kill()
                proc.join(timeout)

        proc.close()


class ZMQ_PS_Client(PubSubBackend):
    label = "local"

    def __init__(self, pub_addr: Optional[str] = None, sub_addr: Optional[str] = None):
        super().__init__()

        self.pub_addr = pub_addr
        self.sub_addr = sub_addr

        self._ctx = zmq.asyncio.Context()
        self.pub_soc: Optional[zmq.asyncio.Socket] = (
            self._ctx.socket(zmq.PUB) if pub_addr is not None else None
        )
        self.sub_soc: Optional[zmq.asyncio.Socket] = (
            self._ctx.socket(zmq.SUB) if sub_addr is not None else None
        )

        # subscribe: store the callback for the topic
        # publish: send a message to each of the callbacks.

        self.topics: dict[str, list[Callable]] = {}

        self._task: Optional[asyncio.Task] = None
        self._closed = False
        self.is_running = asyncio.Event()

    async def _connect_socket(self, sock, addr):
        """Connect `sock` and wait until the connection is established.

        The monitor is attached *before* connecting: it only reports events
        which happen after it was attached.
        """

        monitor = sock.get_monitor_socket()
        try:
            sock.connect(addr)
            while True:
                event = await recv_monitor_message(monitor)
                if event["event"] == zmq.EVENT_CONNECTED:
                    return
        finally:
            # also runs on cancellation.  disable_monitor() only detaches
            # the socket -- leaving it open would block ctx.term()
            sock.disable_monitor()
            monitor.close(linger=0)

    async def connect(self):
        self._check_open()

        if self.sub_soc is not None:
            logger.info("Waiting to connect to ZMQ broker...")
            await self._connect_socket(self.sub_soc, self.sub_addr)

        if self.pub_soc is not None:
            await self._connect_socket(self.pub_soc, self.pub_addr)

        if self.sub_soc is not None:
            self._task = asyncio.create_task(self._run())
            self._task.add_done_callback(self._run_done)

        self.is_running.set()

    async def publish(self, topic, message):
        self._check_open()
        if self.pub_soc is None:
            raise ValueError("Publishing endpoint not connected")

        if not (self.is_running.is_set()):
            logger.warning("Requesting publish before connecting to broker. Waiting")
            await self.is_running.wait()

        topic_b = topic.encode("utf-8")
        message_b = cloudpickle.dumps(message)
        await self.pub_soc.send_multipart([topic_b, message_b])

    async def subscribe(self, topic, callback, **backend_params):
        self._check_open()
        if self.sub_soc is None:
            raise ValueError("Subscribe endpoint not connected")

        if not (self.is_running.is_set()):
            logger.warning("Requesting subscribe before connecting to broker. Waiting")
            await self.is_running.wait()
        self.sub_soc.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))

        self.topics.setdefault(topic, []).append(callback)

    async def unsubscribe(self, topic):
        if self._closed or self.sub_soc is None:
            return

        if topic in self.topics:
            self.sub_soc.setsockopt(zmq.UNSUBSCRIBE, topic.encode("utf-8"))
            del self.topics[topic]

    async def close(self):
        """Cancel the receive loop, close all sockets, terminate the context.

        Idempotent.
        """

        if self._closed:
            return
        self._closed = True

        task, self._task = self._task, None
        try:
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        finally:
            # the guard above makes close() a one-shot, so the sockets and
            # the context have to go even if the await was cancelled
            self.topics.clear()

            for sock in (self.pub_soc, self.sub_soc):
                if sock is not None:
                    sock.close(linger=0)
            self.pub_soc = self.sub_soc = None

            # all sockets are closed, so this returns immediately
            self._ctx.term()
            self.is_running.clear()

    def _check_open(self):
        if self._closed:
            raise RuntimeError("stream client is closed")

    async def _run(self):
        """Receive loop.  A single bad payload or a raising callback must
        not take the stream down -- it is dropped and logged."""

        while True:
            frames = await self.sub_soc.recv_multipart()

            try:
                topic, message = frames
                item = topic.decode("utf-8")
                data = cloudpickle.loads(message)
            except Exception:
                logger.exception("dropping malformed message: %r", frames[:1])
                continue

            for task in self.topics.get(item, []):
                # one failing subscriber must not starve its siblings
                # (CancelledError is not an Exception: close() still wins)
                try:
                    await task(data)
                except Exception:
                    logger.exception("subscriber failed on topic %r", item)

    def _run_done(self, task: asyncio.Task):
        """The receive loop only ends on close().  Any other exit means the
        twin stopped receiving -- report it instead of stalling silently."""

        if self._closed or task.cancelled():
            return

        exc = task.exception() or RuntimeError("stream receive loop exited")
        self._report_error(exc)


# The pubsub client abstracts away the specifics of the pub / sub
# implementation. It rather adds the DataType wrapper / connects with the
# runtime


class PubSubClient:
    """Namespaced, dtype-aware view on a pubsub backend.

    Topics are `dt/<namespace>/dtypes/<dtype label>`.  The namespace keeps
    twins that use identical dtype labels apart on a shared broker, so one
    client per twin is required (also because `subscribe_to_dtype` holds
    one queue per dtype).
    """

    # ZMQ SUBSCRIBE is prefix matching: the terminator keeps a label from
    # matching every label it is a prefix of.  Hygiene, not correctness --
    # delivery is filtered by exact topic lookup.  A control character is
    # used because a dtype label may contain any printable character.
    TOPIC_TERMINATOR = "\x00"

    # For now, only support one backend. Future TODO: Add support for multiple backends

    def __init__(self, backend: PubSubBackend, namespace: str):
        # a namespace carrying a separator would let two twins alias each
        # other's topics -- the one thing the namespace exists to prevent
        if not namespace or "/" in namespace or self.TOPIC_TERMINATOR in namespace:
            raise ValueError(f"invalid stream namespace: {namespace!r}")

        self._backend = backend
        self.namespace = namespace

        # so I don't repeat
        self.subscriptions: set[DataType] = set()

    @property
    def on_error(self) -> Optional[Callable[[BaseException], None]]:
        """Hook for asynchronous stream failures (see `PubSubBackend`)."""

        return self._backend.on_error

    @on_error.setter
    def on_error(self, callback: Optional[Callable[[BaseException], None]]):
        self._backend.on_error = callback

    def topic(self, dtype: DataType) -> str:
        return f"dt/{self.namespace}/dtypes/{dtype.name}{self.TOPIC_TERMINATOR}"

    # for runtime use only!!!
    async def subscribe_to_dtype(
        self,
        dtype: DataType,
        queue: asyncio.Queue,
        backend_params={},
    ):

        if dtype in self.subscriptions:
            return
        self.subscriptions.add(dtype)

        # add message to queue
        async def receive_data(message):
            td = TypedData(dtype, message)
            await queue.put(td)

        await self._backend.subscribe(
            topic=self.topic(dtype), callback=receive_data, **backend_params
        )

    async def unsubscribe_dtype(self, dtype: DataType):
        if dtype not in self.subscriptions:
            return
        self.subscriptions.discard(dtype)

        await self._backend.unsubscribe(self.topic(dtype))

    async def publish(self, dtype: DataType, message, backend_params={}):
        # Convert dtype to a topic
        await self._backend.publish(
            topic=self.topic(dtype), message=message, **backend_params
        )

    async def close(self):
        """Drop all subscriptions and close the backend.  Idempotent.

        The client owns its backend: one client per twin, torn down with it.
        """

        for dtype in list(self.subscriptions):
            await self.unsubscribe_dtype(dtype)

        await self._backend.close()


async def connect_stream_client(
    namespace: str, pub_addr: Optional[str] = None, sub_addr: Optional[str] = None
) -> PubSubClient:
    """Build and connect a namespaced stream client from configuration."""

    pub_addr, sub_addr = stream_addresses(pub_addr, sub_addr)

    backend = ZMQ_PS_Client(pub_addr, sub_addr)
    await backend.connect()

    return PubSubClient(backend, namespace)
