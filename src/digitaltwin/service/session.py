"""Service-side session and twin instances.

A `DTSession` belongs to one client and hosts many independent twins.
It owns the session-shared execution engines (M1: exactly one, `'task'`);
each `TwinInstance` owns a `DTRuntime` plus its own namespaced stream
client.  Twin teardown never disturbs its siblings or the engines.
"""

import asyncio
import contextlib
import logging
import time

from typing import Any, Optional

from fastapi import HTTPException
from radical.asyncflow import WorkflowEngine  # type: ignore
from radical.orbit.plugin_session_base import PluginSession
from rhapsody.backends.execution.orbit import OrbitExecutionBackend  # type: ignore

from ..components import DataType, TypedData
from ..runtime import DTRuntime
from ..streaming import PubSubClient, connect_stream_client
from .wire import Package, encode

log = logging.getLogger("radical.orbit")

# the one engine M1 knows about; M2 adds 'exsitu' as a config addition
TASK_ENGINE = "task"

# co-located-demo default -- 'dragon_v3' (the rhapsody default) would
# break every demo on a laptop
DEFAULT_BACKENDS = ["concurrent"]

# bounded waits.  This runs for days on a shared event loop: nothing here
# may block it and nothing may hang.
TWIN_INIT_TIMEOUT = 300.0  # engine (<=150s) + stream connect + slack
STREAM_CONNECT_TIMEOUT = 30.0
TWIN_STOP_TIMEOUT = 10.0
ENGINE_SHUTDOWN_TIMEOUT = 30.0

# get_inference is the only potentially-long verb; the client may pick its
# own bound.  [P0-interim] large but finite -- see the plan's section 3.
DEFAULT_INFERENCE_TIMEOUT = 600.0

STATE_INITIALIZING = "initializing"
STATE_FAILED = "failed"
STATE_CLOSED = "closed"

# exactly one of these per twin_call
VERBS = (
    "add_task",
    "add_investigator",
    "add_agent",
    "start",
    "stop",
    "describe",
    "get_inference",
)


class TwinInstance:
    """One twin: a `DTRuntime`, its stream client, and a state machine.

    `initializing -> ready -> running -> stopped | failed`.  The first
    state is owned here (the twin has no runtime yet); from `ready` on,
    the runtime's own state machine is the truth.
    """

    def __init__(self, twin_id: str, config: Optional[dict] = None):
        self.twin_id = twin_id
        self.config = config or {}
        self.created = time.time()

        self.runtime: Optional[DTRuntime] = None
        self.stream: Optional[PubSubClient] = None

        self._state = STATE_INITIALIZING
        self._last_error: Optional[str] = None

        self._init_task: Optional[asyncio.Task] = None
        # in-flight get_inference calls: they are awaited by request
        # handlers, not by the runtime, so teardown has to cancel them
        # explicitly or a closing twin would leave a caller hanging
        self._inflight: set[asyncio.Task] = set()

    @property
    def state(self) -> str:
        return self._state if self.runtime is None else str(self.runtime.state)

    @property
    def last_error(self) -> Optional[str]:
        if self.runtime is not None and self.runtime.last_error:
            return self.runtime.last_error
        return self._last_error

    def summary(self) -> dict:
        """The twin's entry in `twin_list` / `admin/sessions`."""

        return {
            "twin_id": self.twin_id,
            "state": self.state,
            "last_error": self.last_error,
            "age": round(time.time() - self.created, 3),
            "config": self.config,
        }

    def ready(self, runtime: DTRuntime, stream: PubSubClient) -> None:
        self.runtime = runtime
        self.stream = stream

    def fail(self, error: BaseException | str) -> None:
        self._state = STATE_FAILED
        self._last_error = (
            error if isinstance(error, str) else f"{type(error).__name__}: {error}"
        )
        log.error("[dt] twin %s failed: %s", self.twin_id, self._last_error)

    def track(self, task: asyncio.Task) -> asyncio.Task:
        """Register an in-flight request-side task for teardown."""

        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

        return task

    async def close(self, timeout: float = TWIN_STOP_TIMEOUT) -> None:
        """Tear the twin down.  Best-effort, bounded, never raises."""

        pending = [t for t in (self._init_task, *self._inflight) if t is not None]
        for task in pending:
            task.cancel()

        if pending:
            with contextlib.suppress(Exception):
                await asyncio.wait(pending, timeout=timeout)

        try:
            if self.runtime is not None:
                # stop() also closes the twin's stream client
                await self.runtime.stop(timeout)
            elif self.stream is not None:
                await asyncio.wait_for(self.stream.close(), timeout)

        except Exception as exc:
            log.warning("[dt] twin %s teardown: %s", self.twin_id, exc)

        self._state = STATE_CLOSED


class DTSession(PluginSession):
    """Per-client session: n twins plus the engines they share."""

    def __init__(self, sid: str, config: Optional[dict] = None):
        super().__init__(sid)

        self.config = config or {}
        self.created = time.time()

        self.twins: dict[str, TwinInstance] = {}
        self._engines: dict[str, WorkflowEngine] = {}
        self._engine_lock = asyncio.Lock()

    # -- twin lifecycle -----------------------------------------------------

    async def twin_create(self, twin_id: str, config: Optional[dict] = None) -> dict:
        """Register a twin and kick its initialization in the background.

        Returns immediately -- engine and stream setup can take minutes and
        must never be paid inside a held request.  Re-creating a live twin
        id is a no-op reporting the current state, which is what makes a
        retried `twin_create` safe.
        """

        self._check_active()

        twin = self.twins.get(twin_id)
        if twin is None:
            twin = TwinInstance(twin_id, config)
            self.twins[twin_id] = twin
            twin._init_task = asyncio.create_task(self._init_twin(twin))
            log.info("[dt] session %s created twin %s", self.sid, twin_id)

        return self._twin_state(twin)

    async def twin_list(self) -> dict:
        """The only observation mechanism in v1: every twin, every state."""

        self._check_active()

        return {"sid": self.sid, "twins": [t.summary() for t in self.twins.values()]}

    async def twin_close(self, twin_id: str) -> dict:
        """Stop and forget a twin.  Idempotent: closing an unknown (already
        closed) twin reports `closed` rather than failing a retry."""

        self._check_active()

        twin = self.twins.pop(twin_id, None)
        if twin is None:
            return {"twin_id": twin_id, "state": STATE_CLOSED}

        await twin.close()
        log.info("[dt] session %s closed twin %s", self.sid, twin_id)

        return self._twin_state(twin)

    async def twin_call(
        self, twin_id: str, verb: str, args: tuple = (), kwargs: Optional[dict] = None
    ) -> dict:
        """Apply exactly one graph verb to one twin."""

        self._check_active()

        twin = self._live_twin(twin_id)
        handler = getattr(self, f"_verb_{verb}")

        try:
            extra = await handler(twin, *args, **(kwargs or {}))

        except HTTPException:
            raise
        except asyncio.CancelledError:
            # the twin was closed under an in-flight call
            raise HTTPException(
                status_code=409, detail=f"twin {twin_id} was closed during {verb}"
            ) from None
        except TimeoutError:
            raise HTTPException(
                status_code=504, detail=f"twin {twin_id}: {verb} timed out"
            ) from None
        except (RuntimeError, ValueError, AssertionError) as exc:
            # the runtime's own refusals (stopped graph, start-after-stop,
            # bad dtypes) are client errors, not service faults
            raise HTTPException(
                status_code=409, detail=f"twin {twin_id}: {verb}: {exc}"
            ) from exc

        return {**self._twin_state(twin), **(extra or {})}

    # -- verbs --------------------------------------------------------------

    async def _verb_add_task(
        self,
        twin: TwinInstance,
        package: Package,
        input_dtype: DataType,
        output_dtype: DataType,
        is_persistent: bool = False,
    ) -> None:
        component = self._instantiate(package, twin, is_persistent)
        twin.runtime.add_task(component, input_dtype, output_dtype, is_persistent)

    async def _verb_add_investigator(
        self,
        twin: TwinInstance,
        package: Package,
        input_dtype: DataType,
        output_dtype: DataType,
        *args,
        **kwargs,
    ) -> None:
        component = self._instantiate(package, twin)
        twin.runtime.add_investigator(
            component, input_dtype, output_dtype, *args, **kwargs
        )

    async def _verb_add_agent(
        self,
        twin: TwinInstance,
        package: Package,
        input_dtype: DataType,
        output_dtype: DataType,
        *args,
        **kwargs,
    ) -> None:
        component = self._instantiate(package, twin)
        twin.runtime.add_agent(component, input_dtype, output_dtype, *args, **kwargs)

    async def _verb_start(self, twin: TwinInstance) -> None:
        # a running twin is left running: an idempotent retry, not an error
        twin.runtime.start()

    async def _verb_stop(self, twin: TwinInstance) -> None:
        # terminal and idempotent in the runtime itself
        await twin.runtime.stop()

    async def _verb_describe(self, twin: TwinInstance) -> dict:
        return {"graph": twin.runtime.describe()}

    async def _verb_get_inference(
        self,
        twin: TwinInstance,
        in_data: TypedData,
        output_dtype: DataType,
        timeout: Optional[float] = DEFAULT_INFERENCE_TIMEOUT,
    ) -> dict:
        # run it as a tracked task so `twin_close` can cancel it -- an
        # in-flight inference must not outlive (or hold up) its twin
        task = twin.track(
            asyncio.create_task(twin.runtime.get_inference(in_data, output_dtype))
        )

        return {"inference": encode(await asyncio.wait_for(task, timeout))}

    # -- engines ------------------------------------------------------------

    async def engine(self, name: str = TASK_ENGINE) -> WorkflowEngine:
        """The session-shared engine `name`, created on first use.

        One engine per name per session, never per twin.  M1 only ever
        asks for `'task'`.
        """

        async with self._engine_lock:
            flow = self._engines.get(name)
            if flow is None:
                flow = self._engines[name] = await self._create_engine(name)
            return flow

    async def _create_engine(self, name: str) -> WorkflowEngine:
        cfg = (self.config.get("engines") or {}).get(name) or {}

        log.info(
            "[dt] session %s building engine %r on endpoint %s",
            self.sid,
            name,
            cfg.get("endpoint_name") or "<auto>",
        )

        backend = await OrbitExecutionBackend(
            broker_url=self.broker_url,
            endpoint_name=cfg.get("endpoint_name"),
            backends=cfg.get("backends") or DEFAULT_BACKENDS,
            batch_window=0,  # per-call latency beats batching for in-situ
        )

        return await WorkflowEngine.create(backend=backend)

    @property
    def broker_url(self) -> Optional[str]:
        """Plugin-level broker URL (`None` lets ORBIT resolve it)."""

        return getattr(self._plugin, "broker_url", None)

    # -- teardown -----------------------------------------------------------

    async def close(self) -> dict:
        """Stop every twin, then shut the engines down.

        Engine shutdown is `wait_for`-bounded: asyncflow's own shutdown is
        an unbounded gather, so a bare await could park the host loop.
        """

        for twin in list(self.twins.values()):
            await twin.close()
        self.twins.clear()

        for name, flow in self._engines.items():
            try:
                await asyncio.wait_for(flow.shutdown(), ENGINE_SHUTDOWN_TIMEOUT)
            except Exception as exc:
                log.warning("[dt] session %s: engine %r shutdown: %s",
                            self.sid, name, exc)
        self._engines.clear()

        return await super().close()

    def summary(self) -> dict:
        """This session's entry in the `admin/sessions` listing."""

        return {
            "sid": self.sid,
            "active": self.is_active,
            "age": round(time.time() - self.created, 3),
            "engines": sorted(self._engines),
            "twins": [twin.summary() for twin in self.twins.values()],
        }

    # -- internals ----------------------------------------------------------

    async def _init_twin(self, twin: TwinInstance) -> None:
        """Background phase of `twin_create`: engine, stream, runtime.

        Every failure lands in `failed` + a last error -- a twin must
        never be left sitting in `initializing`.
        """

        try:
            if self._plugin is None:
                raise RuntimeError("session is not attached to a dt plugin")

            async with asyncio.timeout(TWIN_INIT_TIMEOUT):
                flow = await self.engine(TASK_ENGINE)
                pub_addr, sub_addr = await self._plugin.stream_addresses()
                stream = await connect_stream_client(
                    twin.twin_id, pub_addr, sub_addr, STREAM_CONNECT_TIMEOUT
                )
                twin.ready(DTRuntime(flow, stream), stream)

            log.info("[dt] twin %s ready", twin.twin_id)

        except asyncio.CancelledError:
            twin.fail("twin initialization was cancelled")
            raise
        except TimeoutError:
            twin.fail(f"twin initialization exceeded {TWIN_INIT_TIMEOUT}s")
        except BaseException as exc:
            twin.fail(exc)

    def _live_twin(self, twin_id: str) -> TwinInstance:
        """Resolve a twin that can take a verb, or fail fast.

        Graph verbs never wait for an initializing twin -- the client
        polls `twin_list` for that.
        """

        twin = self.twins.get(twin_id)
        if twin is None:
            raise HTTPException(status_code=404, detail=f"unknown twin: {twin_id}")

        if twin.runtime is None:
            raise HTTPException(
                status_code=409,
                detail=f"twin {twin_id} is {twin.state}"
                + (f": {twin.last_error}" if twin.last_error else ""),
            )

        return twin

    def _instantiate(
        self, package: Any, twin: TwinInstance, is_persistent: bool = False
    ) -> Any:
        """Build a component from a shipped class, injecting the engine.

        Also the home of the persistent-component guard: a persistent
        `main_loop` runs inline on the host loop, so any `function_task`
        it registered would be cloudpickled to the endpoint and occupy a
        backend slot for the twin's lifetime.  Instantiation and
        `is_persistent` are only visible together here, which is why the
        check lives service-side.
        """

        if not isinstance(package, Package):
            raise ValueError(
                "component argument must be a DTClient.package() result,"
                f" got {type(package).__name__}"
            )

        flow = twin.runtime.flow
        registered = 0
        original = flow.function_task

        def counting(*args, **kwargs):
            nonlocal registered
            registered += 1
            return original(*args, **kwargs)

        flow.function_task = counting
        try:
            component = package.instantiate(flow)
        finally:
            flow.function_task = original

        if is_persistent and registered:
            log.warning(
                "[dt] twin %s: persistent component %s registered %d"
                " function_task(s) -- persistent main_loops run inline on"
                " the service loop, so those tasks would occupy backend"
                " slots for the twin's lifetime.  Use plain async code and"
                " runtime.stream instead.",
                twin.twin_id,
                type(component).__name__,
                registered,
            )

        return component

    @staticmethod
    def _twin_state(twin: TwinInstance) -> dict:
        return {
            "twin_id": twin.twin_id,
            "state": twin.state,
            "last_error": twin.last_error,
        }
