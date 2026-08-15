import asyncio
import datetime
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

        f = open("sensor.out", "w")
        f.write("SENSOR MEASUREMENTS ========================= \n")

        for i in range(30):
            f.write(f"[{datetime.datetime.now()}] Publish: {i} \n")
            f.flush()

            await runtime.stream.publish(SENSOR_DTYPE, i)
            await asyncio.sleep(1)

        f.close()
