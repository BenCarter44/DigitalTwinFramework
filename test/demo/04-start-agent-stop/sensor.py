import asyncio
import datetime
import os
import sys
import time

from radical.asyncflow import WorkflowEngine
from digitaltwin.streaming import ZMQ_PS_Client, PubSubClient
from digitaltwin.components import UtilityTask
from dtypes import *
import random


import logging

logger = logging.getLogger(__name__)


class MySensor(UtilityTask):
    def __init__(self, flow: WorkflowEngine):
        super().__init__(flow)
        self.flow = flow

        @self.flow.function_task(service=True)
        async def task():

            f = open("sensor.out", "w")
            f.write("SENSOR MEASUREMENTS ========================= \n")

            ps_backend = ZMQ_PS_Client(ZMQ_PS_BROKER_PUB)
            await ps_backend.connect()
            pclient = PubSubClient(ps_backend)

            for i in range(30):
                f.write(f"[{datetime.datetime.now()}] Publish: {i} \n")
                await pclient.publish(SENSOR_DTYPE, i)
                f.flush()
                await asyncio.sleep(1)

            f.close()

        self.task = task

    async def main_loop(self, runtime, in_data):
        await self.task()
