"""Super simple DT plugin. Really a port of old remote_service.

The plugin is not part of any default plugin set; load it explicitly::

    ./bin/radical-orbit-endpoint-wrapper.sh --plugins default,math
"""

# I took the "math" example plugin, and ported over the remote_service to it.
# It is supposed to be a simple-as-possible demo of a service.

import base64
import json
import logging

import cloudpickle
from fastapi import FastAPI
from radical.asyncflow import WorkflowEngine
import rhapsody
from starlette.requests import Request


from radical.orbit.plugin_base import Plugin
from radical.orbit.plugin_session_base import PluginSession
from radical.orbit.client import PluginClient
from radical.orbit.utils import host_role

from digitaltwin.components import _TwinComponent, Barrier, DataType, JoinDataType
from digitaltwin.config import stream_addresses
from digitaltwin.runtime import DTRuntime
from digitaltwin.streaming import PubSubClient, ZMQ_PS_Client

log = logging.getLogger("radical.orbit")


# ------------------------------------------------------------------------
#
class DTClient(PluginClient):
    """
    Client-side interface for the Math plugin.

    Obtained via ``runtime.get_plugin(endpoint_name, 'math')``, which also
    registers a session.
    """

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

        payload = {"method": method, "args": args_out, "kwargs": kwargs_out}

        self._require_session()
        resp = self._request("POST", self._url(f"passthrough/{self.sid}"), json=payload)
        self._raise(resp, f"Call: {payload['method']}")
        return resp.json()["result"]

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
        pass


# ------------------------------------------------------------------------
#
class DTSession(PluginSession):
    """
    Math session (service side).

    Holds the per-session operation history — the tutorial's stand-in for
    real per-client state (jobs, tasks, backend connections).
    """

    def __init__(self, sid: str):
        super().__init__(sid)
        self._history = []  # [{"op", "a", "b", "result"}, ...]

    async def _process_call(
        self, req: dict, rt: DTRuntime, artifacts: list[_TwinComponent], flow
    ):
        method = req["method"]
        args_raw = req.get("args", [])
        kwargs_raw = req.get("kwargs", [])

        args = [cloudpickle.loads(base64.b64decode(arg)) for arg in args_raw]
        kwargs = {
            k: cloudpickle.loads(base64.b64decode(kwargs_raw[k])) for k in kwargs_raw
        }

        if method == "start":
            return rt.start()

        # for all subclasses of _TwinComponent
        if method == "_register_artifact":
            # try unpickling
            package = args[0]
            assert package["pkg"] == "pkg"

            args_out = []
            for a in package["args"]:
                args_out.append(cloudpickle.loads(base64.b64decode(a)))

            kwargs_out = {}
            for k, a in package["kwargs"].items():
                kwargs_out[k] = cloudpickle.loads(base64.b64decode(a))

            class_comp = cloudpickle.loads(base64.b64decode(package["class"]))
            obj = class_comp(flow, *args_out, **kwargs_out)

            artifacts.append(obj)
            return len(artifacts) - 1

        if method == "print_graph":
            return rt.print_graph()

        if method == "add_barrier":
            return rt.add_barrier(args[0])

        if method == "add_data_join":
            return rt.add_data_join(args[0])

        # others require unpickle package
        pkg = args[0]
        print(pkg)

        if not hasattr(rt, method):
            return f"unknown method {method}"
        fn = getattr(rt, method)

        # execute!

        fn(artifacts[pkg], *args[1:], **kwargs)
        return {"result": "ok"}

    async def handle(
        self, art: list[_TwinComponent], rt: DTRuntime, request: Request, flow
    ) -> dict:
        msg = await request.body()
        req = json.loads(msg.decode("utf-8"))

        if req["method"] == "_end":
            await rt.stop()
            self.notify("result", "ok")
            return {"result": "ok"}

        output = await self._process_call(req, rt, art, flow)

        self.notify("result", output)
        return {"result": output}

    async def history(self) -> dict:
        """
        Return the operations recorded in this session.

        Returns:
            {"count": int, "ops": [{"op", "a", "b", "result"}, ...]}
        """
        self._check_active()
        return {"count": len(self._history), "ops": list(self._history)}

    async def close(self) -> dict:
        """Release per-session state."""
        self._history = []
        return await super().close()


# ------------------------------------------------------------------------
#
class DTPlugin(Plugin):
    """
    Math plugin for ORBIT — the Plugin Writer's Tutorial example.

    Serves the four basic arithmetic operations plus a per-session
    operation history.
    """

    plugin_name = "dt"
    session_class = DTSession
    client_class = DTClient
    version = "0.1.0"

    ui_config = {
        "icon": "DT",
        "title": "Digital Twin",
        "description": "Digital Twin Framework as a Service",
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        """Return whether to load: broker hosts only.

        The dispatcher owns the global pool/pilot/task state, observes topology
        events directly, and proxies psij calls out to login-node endpoints.
        """
        return host_role(app)["role"] == "broker"

    def __init__(self, app: FastAPI, instance_name: str = "dt"):
        """
        Initialize the DT plugin.
        """
        super().__init__(app, instance_name)

        self.runtimes: dict[int, DTRuntime] = {}
        self.add_route_post("passthrough/{sid}", self.passthrough)
        self.artifacts: dict[int, list[_TwinComponent]] = {}

        self.is_start = False
        self.stream_backend = ZMQ_PS_Client(*stream_addresses())
        self.streamers: dict[int, PubSubClient] = {}

        # required for broker host
        self._broker_caller = getattr(app.state, "broker_caller", None)
        self._broker_tap = getattr(app.state, "broker_tap", None)
        self._untap = None

    async def create_runtime(self, sid: int):
        if not self.is_start:
            # later switch to task dispatcher plugin.
            self.backend = rhapsody.get_backend("orbit")
            self.engine = await self.backend
            self.flow = await WorkflowEngine.create(self.engine)
            await self.stream_backend.connect()

        self.streamers[sid] = PubSubClient(self.stream_backend, f"dt_{sid}")
        self.runtimes[sid] = DTRuntime(self.flow, self.streamers[sid])
        self.is_start = True

        self.artifacts[sid] = []

    async def passthrough(self, request: Request) -> dict:
        sid = request.path_params["sid"]

        if sid not in self.runtimes:
            await self.create_runtime(sid)

        return await self._forward(
            sid,
            DTSession.handle,
            rt=self.runtimes[sid],
            art=self.artifacts[sid],
            request=request,
            flow=self.flow,
        )
