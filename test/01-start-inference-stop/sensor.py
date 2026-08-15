import asyncio
import os
import sys
import time

from radical.asyncflow import WorkflowEngine
from digitaltwin.components import UtilityTask
from dtypes import *
import random

import logging

logger = logging.getLogger(__name__)


class MySensor(UtilityTask):
    def __init__(self, flow: WorkflowEngine):
        super().__init__(flow)
        self.flow = flow

    async def main_loop(self, runtime, in_data):
        for i in range(30):
            # val = random.random()
            val = time.monotonic_ns()
            print(f"Sensor val: {val} - {i}")
            await runtime.stream.publish(SENSOR_DTYPE, val)
            await asyncio.sleep(0.5)
