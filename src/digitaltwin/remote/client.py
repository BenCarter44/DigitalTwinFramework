import asyncio
import base64
import json
import zmq
import zmq.asyncio
import cloudpickle

class RemoteDTRuntime:
    """Proxy client for a :class:`RemoteDTService`.

    The client serialises all arguments with ``cloudpickle`` and sends them
    over a ZeroMQ REQ socket.  Responses are deserialised and returned.
    It mirrors the public API of :class:`~digitaltwin.runtime.DTRuntime`.
    """

    def __init__(self, address: str = "tcp://localhost:5555"):
        self.ctx = zmq.asyncio.Context.instance()
        self.socket = self.ctx.socket(zmq.REQ)
        self.socket.connect(address)

    async def _call(self, method: str, *args):
        payload = json.dumps(
            {
                "method": method,
                "params": [
                    base64.b64encode(cloudpickle.dumps(a)).decode("ascii") for a in args
                ],
            }
        ).encode("utf-8")
        await self.socket.send(payload)
        resp = await self.socket.recv()
        return cloudpickle.loads(resp)

    # ------------------------------------------------------------------
    # Wrapped methods – all async to match the real DTRuntime API.
    # ------------------------------------------------------------------

    async def start(self):
        return await self._call("start")

    async def add_task(self, task, input_dtype, output_dtype, is_persistent: bool = False):
        return await self._call("add_task", task, input_dtype, output_dtype, is_persistent)

    async def add_investigator(self, investigator, input_dtype, output_dtype, *args, **kwargs):
        return await self._call(
            "add_investigator", investigator, input_dtype, output_dtype, *args, **kwargs
        )

    async def add_agent(self, agent, input_dtype, output_dtype, *args, **kwargs):
        return await self._call("add_agent", agent, input_dtype, output_dtype, *args, **kwargs)

# NOTE: No additional logic is required – the real RemoteDTService
# performs the round‑trip and forwards to the local DTRuntime.
