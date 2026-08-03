import asyncio

from radical.asyncflow import WorkflowEngine
from rhapsody.backends import ConcurrentExecutionBackend
from concurrent.futures import ProcessPoolExecutor

from digitaltwin.components import *
from digitaltwin.runtime import DTRuntime
from digitaltwin.streaming import PubSubClient, ZMQ_PS_Client

from radical.asyncflow.logging import init_default_logger

import logging

logger = logging.getLogger(__name__)


# Simply start and then stop the runtime.


# Globals:

ZMQ_PS_BROKER_PUB = "tcp://127.0.0.1:5000"
ZMQ_PS_BROKER_SUB = "tcp://127.0.0.1:5001"


if __name__ == "__main__":

    async def main():
        init_default_logger(logging.INFO)
        logging.getLogger("radical.asyncflow").setLevel(logging.WARNING)
        logging.getLogger("rhapsody").setLevel(logging.WARNING)

        exe = await ConcurrentExecutionBackend(ProcessPoolExecutor())
        flow = await WorkflowEngine.create(backend=exe)

        stream_backend = ZMQ_PS_Client(ZMQ_PS_BROKER_PUB, ZMQ_PS_BROKER_SUB)
        await stream_backend.connect()
        pubsub_client = PubSubClient(stream_backend)

        runtime = DTRuntime(flow, pubsub_client)

        runtime.print_graph()
        runtime.start()

        # let it run....
        print("Sleeping...")
        await asyncio.sleep(10)
        await flow.shutdown()

    asyncio.run(main())
