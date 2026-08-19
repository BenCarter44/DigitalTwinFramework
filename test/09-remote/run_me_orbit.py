import os
import time

## Caveats prior to running:
# - The digital twin src directory needs to be symlinked into the ORBIT
#   repository (ORBIT currently looks for plugins only in its src directory)
#
# - I had to add an override for signal handling to get rid of the
#   "RuntimeError" of set_wakeup_fd only works in main thread of the main
#   interpreter in AsyncFlow
#
# - I had to force ORBIT underneath to use ConcurrentExecutionBackend instead of
#   Dragon. It should work on dragon, but didn't test.


from digitaltwin.components import TRUTHY, NULL_DTYPE
from digitaltwin.remote.client import *
from radical.orbit import EndpointRuntime

from dtypes import *
from sensor import MySensor
from agent import MyAgent
from data_sink import MySink
from split import DummySplit

# include all user code for registration!!!
import dtypes
import sensor
import agent
import model
import data_sink
import split

register_user_modules([dtypes, sensor, agent, data_sink, model, split])

from radical.asyncflow.logging import init_default_logger

import logging

logger = logging.getLogger(__name__)

# put it all together
# sensor --> model --> data_sink


def main():
    init_default_logger(logging.INFO)

    rt = EndpointRuntime()
    rt.start(wait=True)

    # Find an endpoint that serves the math plugin
    eids = [
        e_name
        for e_name, e_info in rt.topology().items()
        if e_info and "dt" in (e_info.get("plugins") or {})
    ]
    if not eids:
        print(
            "no endpoint serves the 'dt' plugin - start one with "
            "'--plugins default,math'"
        )
        rt.stop()
        return

    runtime = rt.get_plugin(eids[0], "dt")  # also registers a session

    sensor_pkg = runtime.package(MySensor, 0, kwargs={})
    agent_pkg = runtime.package(MyAgent, 0, kwargs={})
    split_pkg = runtime.package(DummySplit)
    sink_pkg = runtime.package(MySink, 0, kwargs={})

    # test barrier - as it is only on one dtype, it does nothing
    barrier = Barrier("test")
    barrier.add_dtype(SENSOR_DTYPE)
    runtime.add_barrier(barrier)

    # test data join
    join = JoinDataType([INFERENCE_DTYPE])
    runtime.add_data_join(join)

    runtime.add_task(sensor_pkg, TRUTHY, SENSOR_DTYPE, is_persistent=True)
    runtime.add_agent(agent_pkg, SENSOR_DTYPE, INFERENCE_DTYPE)
    runtime.add_data_split_task(split_pkg, join, [INFERENCE_POST_SPLIT_DTYPE])
    runtime.add_task(sink_pkg, INFERENCE_POST_SPLIT_DTYPE, NULL_DTYPE)

    out = runtime.print_graph()
    print(out)

    runtime.start()

    time.sleep(20)

    runtime.end()
    runtime.close()


if __name__ == "__main__":
    main()
