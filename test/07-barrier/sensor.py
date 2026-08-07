import asyncio
import os
import sys
import time

from radical.asyncflow import WorkflowEngine
from digitaltwin.streaming import ZMQ_PS_Client, PubSubClient
from digitaltwin.components import DataType, UtilityTask

ZMQ_PS_BROKER_PUB = "tcp://127.0.0.1:5000"
ZMQ_PS_BROKER_SUB = "tcp://127.0.0.1:5001"

import random

import logging

logger = logging.getLogger(__name__)


class MySensor(UtilityTask):
    def __init__(self, flow: WorkflowEngine, delay, output_dt: DataType):
        super().__init__(flow)
        self.flow = flow

        @self.flow.function_task
        async def task():
            ps_backend = ZMQ_PS_Client(ZMQ_PS_BROKER_PUB)
            await ps_backend.connect()
            pclient = PubSubClient(ps_backend)

            for i in range(30):
                print(f"Publish {output_dt}. Val: {i}")
                await pclient.publish(output_dt, i)
                await asyncio.sleep(delay)

        self.task = task

    async def main_loop(self, runtime, in_data):
        await self.task()
