import asyncio
import base64
import json
import zmq
import zmq.asyncio
import cloudpickle

class RemoteDTService:
    """ZeroMQ based service that forwards **synchronously** to a local
    :class:`~digitaltwin.runtime.DTRuntime` instance.

    All calls receive base64‑encoded cloudpickle payloads.  Errors are
    returned as a pickled string describing the exception.
    """

    def __init__(self, runtime, bind_addr: str = "tcp://*:5555"):
        self.runtime = runtime
        self.bind_addr = bind_addr
        self.ctx = zmq.asyncio.Context.instance()
        self.socket = self.ctx.socket(zmq.REP)
        self.socket.bind(self.bind_addr)

    async def _handle_request(self, msg: bytes) -> bytes:
        try:
            req = json.loads(msg.decode("utf-8"))
            method = req["method"]
            params_b64 = req.get("params", [])
            params = [cloudpickle.loads(base64.b64decode(p)) for p in params_b64]
        except Exception as exc:  # bad framing
            return cloudpickle.dumps(f"bad request: {exc}")

        if not hasattr(self.runtime, method):
            return cloudpickle.dumps(f"unknown method {method}")
        fn = getattr(self.runtime, method)
        try:
            if asyncio.iscoroutinefunction(fn):
                await fn(*params)
            else:
                fn(*params)
            # Success – protocol says we return an empty payload
            return cloudpickle.dumps(None)
        except Exception as exc:
            return cloudpickle.dumps(f"runtime error: {exc}")

    async def serve(self):
        while True:
            msg = await self.socket.recv()
            resp = await self._handle_request(msg)
            await self.socket.send(resp)

    def run(self):
        asyncio.run(self.serve())

# Simple usage:
# from digitaltwin.runtime import DTRuntime
# from digitaltwin.streaming import LocalBackend
# runtime = DTRuntime(...)
# service = RemoteDTService(runtime)
# service.run()
