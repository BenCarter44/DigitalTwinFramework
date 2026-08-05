import asyncio
import os
import sys

from radical.asyncflow import WorkflowEngine
from digitaltwin.streaming import ZMQ_PS_Client, PubSubClient
from digitaltwin.components import UtilityTask
from dtypes import *
import random

import logging

logger = logging.getLogger(__name__)


class Timer(UtilityTask):
    def __init__(self, flow: WorkflowEngine):
        super().__init__(flow)
        self.flow = flow

        @self.flow.function_task
        async def task():
            ps_backend = ZMQ_PS_Client(ZMQ_PS_BROKER_PUB)
            await ps_backend.connect()
            pclient = PubSubClient(ps_backend)

            counter = 0
            while True:
                await pclient.publish(TIMER_TRIGGER_DTYPE, counter)
                await asyncio.sleep(1)

        self.task = task

    async def main_loop(self, runtime, in_data):
        await self.task()
