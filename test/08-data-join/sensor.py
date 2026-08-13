import asyncio
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


class NumberSensor(UtilityTask):
    def __init__(self, flow: WorkflowEngine):
        super().__init__(flow)
        self.flow = flow

        @self.flow.function_task
        async def task():
            ps_backend = ZMQ_PS_Client(ZMQ_PS_BROKER_PUB)
            await ps_backend.connect()
            pclient = PubSubClient(ps_backend)

            for i in range(35):
                # val = random.random()
                await pclient.publish(NUMBER_SENSOR_DTYPE, i)
                await asyncio.sleep(1)

        self.task = task

    async def main_loop(self, runtime, in_data):
        await self.task()


class LetterSensor(UtilityTask):
    def __init__(self, flow: WorkflowEngine):
        super().__init__(flow)
        self.flow = flow

        @self.flow.function_task
        async def task():
            ps_backend = ZMQ_PS_Client(ZMQ_PS_BROKER_PUB)
            await ps_backend.connect()
            pclient = PubSubClient(ps_backend)

            alphabet = "abcdefghijklmnopqrstuvwxyz"
            for i in range(26):
                await pclient.publish(LETTER_SENSOR_DTYPE, alphabet[i])
                await asyncio.sleep(1)

        self.task = task

    async def main_loop(self, runtime, in_data):
        await self.task()
