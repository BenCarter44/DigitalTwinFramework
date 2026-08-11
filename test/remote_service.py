import asyncio
from concurrent.futures import ProcessPoolExecutor
from radical.asyncflow import WorkflowEngine
from rhapsody.backends import ConcurrentExecutionBackend
from digitaltwin.streaming import PubSubClient, ZMQ_PS_Client

from digitaltwin.remote.remote_service import RemoteDTService

from radical.asyncflow.logging import init_default_logger
import logging

logger = logging.getLogger(__name__)


ZMQ_PS_BROKER_PUB = "tcp://127.0.0.1:5000"
ZMQ_PS_BROKER_SUB = "tcp://127.0.0.1:5001"
SERVICE = "tcp://127.0.0.1:5555"

# put it all together
# sensor --> model --> data_sink


async def main():
    init_default_logger(logging.INFO)
    logging.getLogger("radical.asyncflow").setLevel(logging.WARNING)
    logging.getLogger("rhapsody").setLevel(logging.WARNING)

    # create engine
    exe = await ConcurrentExecutionBackend(ProcessPoolExecutor())
    flow = await WorkflowEngine.create(backend=exe)

    # create pubsub backend client
    stream_backend = ZMQ_PS_Client(ZMQ_PS_BROKER_PUB, ZMQ_PS_BROKER_SUB)
    await stream_backend.connect()
    pubsub_client = PubSubClient(stream_backend)

    service = RemoteDTService(flow, SERVICE, pubsub_client)
    logger.info(f"Starting RemoteDTService on {SERVICE}")
    await service.serve()

    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
