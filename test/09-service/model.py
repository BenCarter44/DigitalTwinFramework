import asyncio
import logging

from radical.asyncflow import WorkflowEngine
from digitaltwin.components import ModelInvestigator, TypedData
from digitaltwin.runtime import RuntimeAPI

from dtypes import *

logger = logging.getLogger(__name__)


class MyModel(ModelInvestigator):
    """No learning -- just inference, whose result changes with the
    published model arguments.

    The compute runs on the engine (and therefore on a rhapsody
    endpoint); the `TypedData` wrapping happens here, in the service.
    Function task *arguments* are cloudpickled, but return values only
    survive the ORBIT rhapsody plugin if they are JSON-safe or bytes --
    so tasks return plain values.
    """

    def __init__(self, flow: WorkflowEngine, *args, **kwargs):
        super().__init__(flow)
        self.flow = flow

        @self.flow.function_task
        async def compute(in_data: TypedData, offset=1):
            return offset - in_data.data

        self.compute = compute

    async def main_loop(self, runtime: RuntimeAPI):
        async def do_inference(in_data: TypedData, offset=1):
            return TypedData(INFERENCE_DTYPE, await self.compute(in_data,
                                                                 offset=offset))

        runtime.set_inference_task(do_inference)

        offset = 2
        while True:
            runtime.publish_new_model({"offset": offset})
            offset += 1
            await asyncio.sleep(5)
