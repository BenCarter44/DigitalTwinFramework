import asyncio
import os
import sys

from radical.asyncflow import WorkflowEngine
from digitaltwin.components import UtilityTask
from dtypes import *
import random

import logging

logger = logging.getLogger(__name__)


class Timer(UtilityTask):
    def __init__(self, flow: WorkflowEngine):
        super().__init__(flow)
        self.flow = flow

    async def main_loop(self, runtime, in_data):
        counter = 0
        while True:
            await runtime.stream.publish(TIMER_TRIGGER_DTYPE, counter)
            await asyncio.sleep(1)
