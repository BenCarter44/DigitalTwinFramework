import asyncio
import os
import sys
import time

from radical.asyncflow import WorkflowEngine
from digitaltwin.components import UtilityTask
from digitaltwin.runtime import RuntimeAPI
from digitaltwin.streaming import PubSubClient, PubSubConfig
from dtypes import *
import random

import logging

logger = logging.getLogger(__name__)


class MySensor(UtilityTask):
    def __init__(self, flow: WorkflowEngine):
        super().__init__(flow)
        self.flow = flow

        @self.flow.function_task
        async def test(ps_config: PubSubConfig):
            ps = await PubSubClient.from_config(ps_config)
            for i in range(30):
                # val = random.random()
                val = time.monotonic_ns()
                print(f"Sensor val: {val} - {i}")
                await ps.publish(SENSOR_DTYPE, val)
                await asyncio.sleep(1)

        self.task = test

    async def main_loop(self, runtime: RuntimeAPI, in_data):
        await self.task(runtime.get_stream_config())
