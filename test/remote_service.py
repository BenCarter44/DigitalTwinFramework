import asyncio
from concurrent.futures import ProcessPoolExecutor
from radical.asyncflow import WorkflowEngine
from rhapsody.backends import ConcurrentExecutionBackend
from digitaltwin.streaming import connect_stream_client

from digitaltwin.remote.remote_service import RemoteDTService

from radical.asyncflow.logging import init_default_logger
import logging
import os

logger = logging.getLogger(__name__)


SERVICE = os.environ.get("DT_REMOTE_SERVICE_ADDR", "tcp://127.0.0.1:5555")

# put it all together
# sensor --> model --> data_sink


async def main():
    init_default_logger(logging.INFO)
    logging.getLogger("radical.asyncflow").setLevel(logging.WARNING)
    logging.getLogger("rhapsody").setLevel(logging.WARNING)

    # create engine
    exe = await ConcurrentExecutionBackend(ProcessPoolExecutor())
    flow = await WorkflowEngine.create(backend=exe)

    # create the twin's namespaced stream client (this prototype hosts a
    # single session, so a single namespace will do)
    pubsub_client = await connect_stream_client("remote-service")

    service = RemoteDTService(flow, SERVICE, pubsub_client)
    logger.info(f"Starting RemoteDTService on {SERVICE}")
    await service.serve()

    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
