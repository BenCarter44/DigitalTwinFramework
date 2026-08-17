import asyncio
import os
import sys

from radical.asyncflow import WorkflowEngine
from digitaltwin.components import UtilityTask
from digitaltwin.streaming import PubSubClient
from dtypes import *
import random

import logging

logger = logging.getLogger(__name__)


class MySensor(UtilityTask):
    def __init__(self, flow: WorkflowEngine, *args, **kwargs):
        super().__init__(flow)
        self.flow = flow

    async def main_loop(self, runtime, in_data):

        ps = await PubSubClient.from_config(runtime.get_stream_config())
        for i in range(10):
            await asyncio.sleep(1)
            val = random.random()
            print(f"Sensor val: {val}")
            await ps.publish(SENSOR_DTYPE, val)
