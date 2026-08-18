import asyncio
import base64
import json
import zmq
import zmq.asyncio
import cloudpickle
from radical.asyncflow import WorkflowEngine

from digitaltwin.components import _TwinComponent  # type: ignore
from ..runtime import DTRuntime
from ..streaming import PubSubClient

import logging

logger = logging.getLogger(__name__)

###
# See test/09-remote on how this works!
###


class RemoteDTService:
    """ZeroMQ based service that forwards **synchronously** to a local
    :class:`~digitaltwin.runtime.DTRuntime` instance.

    All calls receive base64-encoded cloudpickle payloads.  Errors are
    returned as a pickled string describing the exception.
    """

    def __init__(
        self, flow: WorkflowEngine, bind_addr: str, streamer: PubSubClient
    ) -> None:
        self.bind_addr = bind_addr
        self.ctx = zmq.asyncio.Context.instance()
        self.socket = self.ctx.socket(zmq.REP)
        self.socket.bind(self.bind_addr)

        # support just one session / runtime right now...
        self.runtime: DTRuntime = None  # type: ignore

        self.flow = flow
        self.streamer = streamer
        self.artifacts: list[_TwinComponent] = []  # will actually be various subclasses

    def _process_call(self, req: dict):
        method = req["method"]
        args_raw = req.get("args", [])
        kwargs_raw = req.get("kwargs", [])

        args = [cloudpickle.loads(base64.b64decode(arg)) for arg in args_raw]
        kwargs = {
            k: cloudpickle.loads(base64.b64decode(kwargs_raw[k])) for k in kwargs_raw
        }

        if method == "_new":
            self.runtime = DTRuntime(self.flow, self.streamer)
            return "ok"

        if method == "start":
            return self.runtime.start()

        if method == "_end":
            return False

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
            logger.info(f"Instance creation: {class_comp}, {args_out}, {kwargs_out}")
            obj = class_comp(self.flow, *args_out, **kwargs_out)

            self.artifacts.append(obj)
            return len(self.artifacts) - 1

        if method == "print_graph":
            return self.runtime.print_graph()

        if method == "add_barrier":
            return self.runtime.add_barrier(args[0])

        if method == "add_data_join":
            return self.runtime.add_data_join(args[0])

        # others require unpickle package
        pkg = args[0]
        logger.debug(f"Call: {method}: {self.artifacts[pkg]}, {args[1:]}, {kwargs}")

        if not hasattr(self.runtime, method):
            return f"unknown method {method}"
        fn = getattr(self.runtime, method)

        # execute!
        fn(self.artifacts[pkg], *args[1:], **kwargs)
        return "ok"

    def _handle_request(self, msg: bytes) -> bytes | bool:
        try:
            req = json.loads(msg.decode("utf-8"))
        except Exception as exc:  # bad framing
            return cloudpickle.dumps(f"bad request: {exc}")

        if req["method"] == "_end":
            return False

        output = self._process_call(req)

        return cloudpickle.dumps(output)

    async def serve(self) -> None:
        while True:
            msg = await self.socket.recv()
            assert isinstance(msg, bytes)
            resp = self._handle_request(msg)
            if resp == False:
                await self.runtime.stop()
                await self.socket.send(cloudpickle.dumps("ok"))
                break
            else:
                await self.socket.send(resp)
