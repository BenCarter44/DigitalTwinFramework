# src/digitaltwin/streaming.py
"""Streaming facilities for the digital-twin runtime.

The digital twin framework itself has an abstract :class:`PubSubClient` that
translates DT terms into basic topics that a PubSubBackend can use.

The PubSubBackend is the actual implementation of a PubSub transport. This
distinction places the PubSubBackend outside of the DT architecture
intentionally.

Typical usage is::

    backend = ZMQ_PS_Client("tcp://127.0.0.1:5000", "tcp://127.0.0.1:5001")
    await backend.connect()
    ps = PubSubClient(backend)
    await ps.subscribe_to_dtype(DataType("hello"), queue)
    await ps.publish(DataType("hello"), "world")

The client accepts an arbitrary backend; the default is the abstract
:class:`PubSubBackend`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import asyncio
import logging
from typing import Any, Callable, Dict, Optional

import cloudpickle
import zmq
import zmq.asyncio
from zmq.utils.monitor import recv_monitor_message

from .components import DataType, TypedData

logger = logging.getLogger(__name__)


class PubSubBackend(ABC):
    """Abstract base class for publish/subscribe backends.

    Subclasses must implement the asynchronous ``connect`` method and the
    core publish/subscribe operations.
    """

    label: str = "generic"

    def __init__(self) -> None:
        self.label = "PubSubBackend"

    @abstractmethod
    async def connect(self, *args: Any, **kwargs: Any) -> None:
        """Connect the backend to the message broker.

        Args:
            *args (Any): Positional arguments specific to the backend.
            **kwargs (Any): Keyword arguments specific to the backend.
        """

    @abstractmethod
    async def publish(self, topic: str, message: Any, **kwargs: Any) -> None:
        """Publish *message* to *topic*.

        Args:
            topic (str): Destination topic.
            message (Any): Payload to send.
            **kwargs (Any): Additional backend specific parameters.
        """

    @abstractmethod
    async def subscribe(
        self, topic: str, callback: Callable[[Any], Any], **kwargs: Any
    ) -> None:
        """Subscribe *callback* to *topic*.

        Args:
            topic (str): Topic to subscribe to.
            callback (Callable[[Any], Any]): Coroutine that receives the
                deserialised message.
            **kwargs (Any): Other backend‑specific arguments.
        """

    @abstractmethod
    async def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from *topic*.

        Args:
            topic (str): Topic to stop receiving.
        """

    def __str__(self) -> str:
        return f"{self.label}"


class MQTTBackend(PubSubBackend):
    """Placeholder for an MQTT‑based backend (not yet implemented)."""

    label: str = "mqtt"

    # The concrete implementation will inherit from :class:`PubSubBackend`.
    def __init__(self) -> None:
        super().__init__()
        self.label = "MQTTBackend"


# ---------------------------------------------------------------------------
# ZMQ broker and client
# ---------------------------------------------------------------------------
class ZMQ_Broker:
    """Thin wrapper that creates a ZMQ XPUB/XSUB proxy.

    Args:
        publish_addr (str): Address on which the broker will bind the XSUB
            socket.
        subscribe_addr (str): Address on which the broker will bind the XPUB
            socket.
    """

    def __init__(self, publish_addr: str, subscribe_addr: str) -> None:
        self.ctx = zmq.Context()
        self.pub_recv = zmq.Socket(self.ctx, zmq.XSUB)
        self.sub_send = zmq.Socket(self.ctx, zmq.XPUB)

        self.publish_addr = publish_addr
        self.subscribe_addr = subscribe_addr

    def run(self) -> None:
        """Start the proxy loop forever.

        Raises:
            zmq.ZMQError
                If binding to the provided addresses fails.
        """
        self.pub_recv.bind(self.publish_addr)
        self.sub_send.bind(self.subscribe_addr)
        zmq.proxy(self.pub_recv, self.sub_send)
        self.pub_recv.close()
        self.sub_send.close()
        self.ctx.term()

    def get_connection_str(self) -> tuple[str, str]:
        """Return the publish and subscribe URLs used by this broker."""
        return self.publish_addr, self.subscribe_addr


class ZMQ_PS_Client(PubSubBackend):
    """Pub/Sub client that talks to a ZMQ broker.

    The client manages a pair of asynchronous sockets (PUB/SUB) and
    keeps a mapping from topics to user callbacks.
    """

    label: str = "local"

    def __init__(
        self, pub_addr: Optional[str] = None, sub_addr: Optional[str] = None
    ) -> None:
        super().__init__()
        self.pub_addr = pub_addr
        self.sub_addr = sub_addr

        ctx = zmq.asyncio.Context()
        if self.pub_addr is not None:
            self.pub_soc: Optional[zmq.asyncio.Socket] = ctx.socket(zmq.PUB)
        else:
            self.pub_soc = None

        if self.sub_addr is not None:
            self.sub_soc: Optional[zmq.asyncio.Socket] = ctx.socket(zmq.SUB)
        else:
            self.sub_soc = None

        # subscribe: store the callback for the topic
        # publish: send a message to each of the callbacks.

        self.topics: dict[str, list[Callable]] = {}

        self.loop: Optional[asyncio.BaseEventLoop] = None
        self.is_running = asyncio.Event()

    async def _wait_for_connect(self, sock: zmq.asyncio.Socket) -> None:
        """Block until *sock* reports a ``EVENT_CONNECTED``.

        Args:
            sock (zmq.asyncio.Socket): Socket to monitor.
        """
        monitor = sock.get_monitor_socket()
        while True:
            event = await recv_monitor_message(monitor)
            if event["event"] == zmq.EVENT_CONNECTED:
                break

    async def connect(self) -> None:
        """Connect the client to the broker and start the read loop.

        The method blocks until the sockets are connected.
        """
        if self.sub_addr is not None and self.sub_soc is not None:
            self.sub_soc.connect(self.sub_addr)
            logger.info("Waiting to connect to ZMQ broker…")
            await self._wait_for_connect(self.sub_soc)

        if self.pub_addr is not None and self.pub_soc is not None:
            self.pub_soc.connect(self.pub_addr)
            await self._wait_for_connect(self.pub_soc)

        self.task = asyncio.create_task(self._run())
        await asyncio.sleep(0.1)
        self.is_running.set()

    async def publish(self, topic: str, message: Any, **kwargs) -> None:
        """Publish *message* under *topic*.

        The method waits for the socket to be in a running state if the
        connection has not yet finished.
        """
        if self.pub_soc is None:
            raise ValueError("Publishing endpoint not connected")

        if not self.is_running.is_set():
            logger.warning("Requesting publish before connecting to broker. Waiting")
            await self.is_running.wait()

        topic_b = topic.encode("utf-8")
        message_b = cloudpickle.dumps(message)
        await self.pub_soc.send_multipart([topic_b, message_b])

    async def subscribe(
        self, topic: str, callback: Callable[[Any], Any], **backend_params: Any
    ) -> None:
        """Subscribe *callback* to *topic*.

        If *topic* is new a subscription is created, otherwise the
        callback is appended to the existing topic list.
        """
        if self.sub_soc is None:
            raise ValueError("Subscribe endpoint not connected")

        if not self.is_running.is_set():
            logger.warning("Requesting subscribe before connecting to broker. Waiting")
            await self.is_running.wait()

        self.sub_soc.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))

        if topic not in self.topics:
            self.topics[topic] = [callback]
        else:
            self.topics[topic].append(callback)

    async def _run(self) -> None:
        if self.sub_soc is None:
            return
        while True:
            topic, message = await self.sub_soc.recv_multipart()
            item = topic.decode("utf-8")
            data = cloudpickle.loads(message)
            for task in self.topics.get(item, []):
                await task(data)

    async def unsubscribe(self, topic: str) -> None:
        """Remove *topic* from the subscription list and unsubscribe.

        Args:
            topic (str): Topic to stop receiving.
        """
        if self.sub_soc is None:
            raise ValueError("Subscribe endpoint not connected")

        if topic in self.topics:
            self.sub_soc.setsockopt(zmq.UNSUBSCRIBE, topic.encode("utf-8"))
            del self.topics[topic]


# The pubsub client abstracts away the specifics of the pub / sub
# implementation. It rather adds the DataType wrapper / connects with the
# runtime


class PubSubClient:
    """Convenient helper to publish/subscribe ``TypedData``.

    The client internally translates raw ZMQ multipart messages into
    :class:`TypedData`; consumers usually interact with an
    :class:`asyncio.Queue` object.
    """

    # Topics used by the runtime.
    RUNTIME_INFO_TOPIC: str = "runtime/info"
    RUNTIME_DTYPES: str = "runtime/dtypes/<dtype_label>"

    # For now, only support one backend. Future TODO: Add support for multiple backends

    def __init__(self, backend=PubSubBackend) -> None:
        self._backend = backend

        # so I don't repeat
        self.subscriptions: set[DataType] = set()

    # for runtime use only!!!
    async def subscribe_to_dtype(
        self,
        dtype: DataType,
        queue: asyncio.Queue[TypedData],
        backend_params: dict[str, Any] | None = None,
    ) -> None:
        """Subscribe *queue* to all messages of *dtype*.

        The subscription creates a topic like
        ``runtime/dtypes/<dtype_label>`` and pushes received payloads
        wrapped in :class:`TypedData` onto *queue*.
        """
        if dtype in self.subscriptions:
            return
        self.subscriptions.add(dtype)

        # add message to queue
        async def receive_data(message: Any) -> None:
            td = TypedData(dtype, message)
            await queue.put(td)

        topic = self.RUNTIME_DTYPES.replace("<dtype_label>", dtype.name)
        await self._backend.subscribe(
            topic=topic, callback=receive_data, **(backend_params or {})
        )

    async def publish(
        self,
        dtype: DataType,
        message: Any,
        backend_params: dict[str, Any] | None = None,
    ) -> None:
        """Publish *message* to the topic that represents *dtype*.

        The payload is sent over the underlying backend.
        """
        topic = self.RUNTIME_DTYPES.replace("<dtype_label>", dtype.name)
        await self._backend.publish(
            topic=topic, message=message, **(backend_params or {})
        )


if __name__ == "__main__":

    # PubSub client is a lightweight caller of pub sub backends.
    # It also operates via queues. Subscriptions take a queue, and the pubsub
    # client adds items to the queue as they arrive. It also supports multiple
    # backends.
    #
    # Future: specifying pub/sub topics by backend.

    pub_addr = "tcp://127.0.0.1:5000"
    sub_addr = "tcp://127.0.0.1:5001"

    # Run ZMQ Broker in another process

    q: asyncio.Queue[TypedData] = asyncio.Queue()

    async def hello_world() -> None:
        while True:
            item = await q.get()
            print(f"Hello World! I got: {item.data}")

    async def main() -> None:

        zmq_backend = ZMQ_PS_Client(pub_addr, sub_addr)
        await zmq_backend.connect()

        ps_client = PubSubClient(zmq_backend)

        # Test PubSub

        worker = asyncio.create_task(hello_world())

        await ps_client.subscribe_to_dtype(DataType("hello"), q)

        for i in range(10):
            await ps_client.publish(DataType("hello"), f"message {i}")
            await asyncio.sleep(1)

    asyncio.run(main())
