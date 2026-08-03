## Streaming Handler

## Actual streaming backend is pluggable.

from abc import ABC, abstractmethod
import asyncio
from typing import Callable, Optional

import cloudpickle
from .components import DataType, TypedData
import zmq
import zmq.asyncio
from zmq.utils.monitor import recv_monitor_message
import logging

logger = logging.getLogger(__name__)


class PubSubBackend(ABC):
    label = "generic"

    def __init__(self):
        self.label = "PubSubBackend"

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

    def __str__(self):
        return f"{self.label}"


class MQTTBackend:
    label = "mqtt"
    pass


# Use ZMQ for the broker
class ZMQ_Broker:
    def __init__(self, publish_addr: str, subscribe_addr: str):
        self.ctx = zmq.Context()
        self.pub_recv = zmq.Socket(self.ctx, zmq.XSUB)
        self.sub_send = zmq.Socket(self.ctx, zmq.XPUB)

        self.publish_addr = publish_addr
        self.subscribe_addr = subscribe_addr

    def run(self):

        self.pub_recv.bind(self.publish_addr)
        self.sub_send.bind(self.subscribe_addr)

        zmq.proxy(self.pub_recv, self.sub_send)

        self.pub_recv.close()
        self.sub_send.close()
        self.ctx.term()

    def get_connection_str(self):
        return self.publish_addr, self.subscribe_addr


class ZMQ_PS_Client(PubSubBackend):
    label = "local"

    def __init__(self, pub_addr: Optional[str] = None, sub_addr: Optional[str] = None):
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

        self.topics: dict[str, Callable] = {}

        self.loop: Optional[asyncio.BaseEventLoop] = None
        self.is_running = asyncio.Event()

    async def _wait_for_connect(self, sock):
        monitor = sock.get_monitor_socket()
        while True:
            event = await recv_monitor_message(monitor)
            if event["event"] == zmq.EVENT_CONNECTED:
                break

    async def connect(self):
        if self.sub_addr is not None:
            self.sub_soc.connect(self.sub_addr)
            await self._wait_for_connect(self.sub_soc)

        if self.pub_addr is not None:
            self.pub_soc.connect(self.pub_addr)
            await self._wait_for_connect(self.pub_soc)
        self.task = asyncio.create_task(self._run())
        await asyncio.sleep(0.1)
        self.is_running.set()

    async def publish(self, topic, message):
        if self.pub_soc is None:
            raise ValueError("Publishing endpoint not connected")

        if not (self.is_running.is_set()):
            logger.warning("Requesting publish before connecting to broker. Waiting")
            await self.is_running.wait()

        topic_b = topic.encode("utf-8")
        message_b = cloudpickle.dumps(message)
        await self.pub_soc.send_multipart([topic_b, message_b])

    async def subscribe(self, topic, callback, **backend_params):
        if self.sub_soc is None:
            raise ValueError("Subscribe endpoint not connected")

        if not (self.is_running.is_set()):
            logger.warning("Requesting subscribe before connecting to broker. Waiting")
            await self.is_running.wait()
        self.sub_soc.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))

        if topic not in self.topics:
            self.topics[topic] = [callback]
        else:
            self.topics[topic].append(callback)

    async def _run(self):
        if self.sub_soc is None:
            return
        while True:
            topic, message = await self.sub_soc.recv_multipart()
            item = topic.decode("utf-8")
            data = cloudpickle.loads(message)
            for task in self.topics.get(item, []):
                await task(data)

    async def unsubscribe(self, topic):
        if self.sub_soc is None:
            raise ValueError("Subscribe endpoint not connected")

        if topic in self.topics:
            self.sub_soc.setsockopt(zmq.UNSUBSCRIBE, topic.encode("utf-8"))
            del self.topics[topic]


# The pubsub client abstracts away the specifics of the pub / sub
# implementation. It rather adds the DataType wrapper / connects with the
# runtime


class PubSubClient:
    # Topics:
    # runtime/info/
    # runtime/dtypes/<dtype_label>
    RUNTIME_INFO_TOPIC = "runtime/info"
    RUNTIME_DTYPES = "runtime/dtypes/<dtype_label>"

    # For now, only support one backend. Future TODO: Add support for multiple backends

    def __init__(self, backend=PubSubBackend):
        self._backend = backend

        # so I don't repeat
        self.subscriptions: set[DataType] = set()

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

        topic = self.RUNTIME_DTYPES.replace("<dtype_label>", dtype.name)
        await self._backend.subscribe(
            topic=topic, callback=receive_data, **backend_params
        )

    # Can only be run by persistent tasks!
    async def publish(self, dtype, message, backend_params={}):
        # Convert dtype to a topic
        topic = self.RUNTIME_DTYPES.replace("<dtype_label>", dtype.name)
        await self._backend.publish(topic=topic, message=message, **backend_params)


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

    async def hello_world():
        while True:
            item = await q.get()
            print(f"Hello World! I got: {item.data}")

    async def main():

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
