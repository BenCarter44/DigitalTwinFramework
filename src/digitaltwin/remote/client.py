import zmq
import base64
import json
import cloudpickle

from ..components import Barrier, DataType, JoinDataType
import logging

logger = logging.getLogger(__name__)

###
# See test/09-remote on how this works!
###


def register_user_modules(modules: list) -> None:
    for module in modules:
        cloudpickle.register_pickle_by_value(module)


class RemoteDTRuntime:
    """Proxy client for a :class:`RemoteDTService`.

    The client serializes all arguments with ``cloudpickle`` and sends them
    over a ZeroMQ REQ socket.  Responses are deserialized and returned.
    It mirrors the public API of :class:`~digitaltwin.runtime.DTRuntime`.
    """

    def __init__(self, address: str) -> None:
        self.ctx = zmq.Context.instance()
        self.socket = self.ctx.socket(zmq.REQ)
        self.socket.connect(address)

        self._call("_new")

    def package(self, module, *args, **kwargs):
        cp_class = base64.b64encode(cloudpickle.dumps(module)).decode("ascii")
        args_out = []
        for a in args:
            args_out.append(base64.b64encode(cloudpickle.dumps(a)).decode("ascii"))

        kwargs_out = {}
        for k, a in kwargs.items():
            kwargs_out[k] = base64.b64encode(cloudpickle.dumps(a)).decode("ascii")

        payload = {
            "class": cp_class,
            "args": args_out,
            "kwargs": kwargs_out,
            "pkg": "pkg",
        }
        # logger.debug(f"Package payload: {payload}")
        identifier = self._register_artifact(payload)
        return identifier

    def _register_artifact(self, pkg):
        identifier = self._call("_register_artifact", pkg)
        return identifier

    def _call(self, method: str, *args, **kwargs):
        args_out = []
        for a in args:
            args_out.append(base64.b64encode(cloudpickle.dumps(a)).decode("ascii"))

        kwargs_out = {}
        for k, a in kwargs.items():
            kwargs_out[k] = base64.b64encode(cloudpickle.dumps(a)).decode("ascii")

        payload = json.dumps(
            {"method": method, "args": args_out, "kwargs": kwargs_out}
        ).encode("utf-8")
        self.socket.send(payload)
        resp = self.socket.recv()
        return cloudpickle.loads(resp)

    # --------------------------------------------------------------
    # Wrapped methods – all sync to match the real DTRuntime API.
    # --------------------------------------------------------------

    def start(self):
        return self._call("start")

    def add_task(
        self,
        task_pkg,
        input_dtype: DataType,
        output_dtype: DataType,
        is_persistent: bool = False,
    ):
        return self._call(
            "add_task", task_pkg, input_dtype, output_dtype, is_persistent
        )

    def add_investigator(
        self, inv_pkg, input_dtype: DataType, output_dtype: DataType, *args, **kwargs
    ):
        return self._call(
            "add_investigator", inv_pkg, input_dtype, output_dtype, *args, **kwargs
        )

    def add_agent(
        self, agent_pkg, input_dtype: DataType, output_dtype: DataType, *args, **kwargs
    ):
        return self._call(
            "add_agent", agent_pkg, input_dtype, output_dtype, *args, **kwargs
        )

    def add_barrier(self, barrier: Barrier):
        return self._call("add_barrier", barrier)

    def add_data_join(self, join_dtype: JoinDataType):
        return self._call("add_data_join", join_dtype)

    def add_data_split_task(
        self, task_pkg, input_dtype: DataType, output_dtypes: tuple[DataType]
    ):
        return self._call("add_data_split_task", task_pkg, input_dtype, output_dtypes)

    def print_graph(self):
        return self._call("print_graph")

    def end(self) -> None:
        # nop currently
        self._call("_end")

    def close(self) -> None:
        self.socket.close()


# Lightweight orchestrator that can create new RemoteDTRuntime sessions.
class RemoteDTOrchestrator:
    """Creates and manages RemoteDTRuntime sessions.

    For now it simply instantiates a new :class:`RemoteDTRuntime` with a
    configurable address.  The class can be extended to handle session
    pooling, authentication, or reconnection logic.
    """

    def __init__(self, address: str) -> None:
        self.address = address

    def new_session(self) -> RemoteDTRuntime:
        return RemoteDTRuntime(self.address)


# NOTE: No additional logic is required – the real RemoteDTService
# performs the round-trip and forwards to the local DTRuntime.
