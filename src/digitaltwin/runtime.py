import asyncio
import logging

from collections import defaultdict
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Callable, Optional

from radical.asyncflow import WorkflowEngine  # type: ignore

from .components import (
    NULL_DTYPE,
    TRUTHY,
    Barrier,
    DataType,
    ModelInvestigator,
    SciAgent,
    SplitTask,
    TypedData,
    UtilityTask,
    _TwinComponent,
)
from .streaming import PubSubClient, PubSubConfig

logger = logging.getLogger(__name__)

# bounded wait for in-flight tasks to settle on stop()
STOP_TIMEOUT = 10.0


class RuntimeState(StrEnum):
    """Lifecycle of a twin runtime.

    `stopped` and `failed` are both terminal: a twin which fails tears
    itself down (see `_record_error`), it just reports the error rather
    than a clean stop.
    """

    READY = "ready"
    RUNNING = "running"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass
class _AnnotatedComponent:
    component: _TwinComponent
    input_dtype: DataType = NULL_DTYPE
    output_dtype: DataType = NULL_DTYPE
    is_persistent: bool = False
    subscriptions: dict[str, list[Callable]] = field(
        default_factory=lambda: defaultdict(list)
    )
    model_kwargs: dict[str, Any] = field(default_factory=dict)
    accuracy_kwargs: dict[str, Any] = field(default_factory=dict)
    inference_task: Optional[Callable] = None
    investigators: dict[int, "_AnnotatedComponent"] = field(default_factory=dict)
    model_select_task: Optional[Callable] = None

    model_select_args: tuple = tuple()
    model_select_kwargs: dict = field(default_factory=dict)

    has_published_model: asyncio.Event = field(default_factory=lambda: asyncio.Event())
    has_published_selector: asyncio.Event = field(
        default_factory=lambda: asyncio.Event()
    )
    model_publish_cb = None


class RuntimeAPI:
    """What a twin component sees of its runtime."""

    ON_INPUT = "runtime/ON_INPUT"
    ON_OUTPUT = "runtime/ON_OUTPUT"
    ON_MODEL_PUBLISH = "runtime/ON_PUBLISH"
    ON_FILTERED_INPUT = "runtime/ON_FILTER_INPUT"
    ON_FILTERED_OUTPUT = "runtime/ON_FILTER_OUTPUT"

    def __init__(self, runtime: "DTRuntime", ant: _AnnotatedComponent):
        self._runtime = runtime
        self._ant = ant
        self._internal_add_investigator: Optional[Callable] = None

    @property
    def stream(self) -> PubSubClient:
        """The twin's namespaced, connected stream client.

        Persistent components publish their output through it (the runtime
        subscribes to that dtype and feeds the graph with it).  Components
        never build their own transport clients and never see addresses.

        This is the in-process convenience: the same endpoint `stream_config`
        describes, already open.  Code which does not run on the host loop
        needs the config instead -- see below.
        """

        return self._runtime.streamer

    @property
    def stream_config(self) -> PubSubConfig:
        """The twin's stream endpoint as plain data.

        Ship *this* to code which runs outside the host process (a task in
        another process or on another host) and let it open its own client:
        the live client above holds sockets, a receive loop and subscriber
        queues, none of which can travel.
        """

        return self._runtime.stream_config

    def subscribe_to_topic(self, topic: str, task: Callable):
        self._ant.subscriptions[topic].append(task)

    def publish_new_model(self, model_kwargs={}, acc_kwargs={}):
        self._ant.model_kwargs = model_kwargs
        self._ant.accuracy_kwargs = acc_kwargs
        self._ant.has_published_model.set()
        if self._ant.model_publish_cb is not None:
            self._runtime._to_asyncio_task(
                self._ant.model_publish_cb,
                self._ant.component,
                model_kwargs,
                acc_kwargs,
            )

    def set_inference_task(self, task: Callable):
        self._ant.inference_task = task

    def start_investigator(self, investigator: ModelInvestigator):
        if self._internal_add_investigator is None:
            raise ValueError("Only can start an investigator inside a SciAgent")

        count = len(self._ant.investigators)

        new = _AnnotatedComponent(
            investigator,
            input_dtype=self._ant.input_dtype,
            output_dtype=self._ant.output_dtype,
            is_persistent=False,
        )
        new.model_publish_cb = self._ant.component.model_publish_cb
        investigator.runtime_id = count
        self._ant.investigators[count] = new
        self._internal_add_investigator(new)  # calls the loop

    # receives TypedData. Outputs investigator ID.
    def set_model_selection_task(self, task: Callable):
        self._ant.model_select_task = task

    def update_model_selector(self, *args, **kwargs):
        self._ant.model_select_args = args
        self._ant.model_select_kwargs = kwargs
        self._ant.has_published_selector.set()

    # the learner can request inference from other agents --- is blocking
    async def get_inference(
        self, input_d: TypedData, output_dtype: DataType
    ) -> TypedData:
        return await self._runtime._internal_agent_inference(input_d, output_dtype)


class DTRuntime:
    """Workflow builder / dynamic manager.

    Method Types:
    - DEF           start
    - DEF           add_task
    - DEF           add_investigator
    - ASYNC DEF     _run_component
    - FLOW BLOCK    _dtype_consumer
    - DEF           _put_to_dtype_queue
    - ASYNC DEF     _launch_consumer

    User callables:
    - ASYNC OR FLOW TASK    inference_task
        (_runtime_component calls directly)


    - ASYNC DEF             main_loop
            (_add_investigator shoots off ASYNCTASK or
                _runtime_component calls directly or
                _runtime_component shoots off ASYNCTASK if persistent)

    - ASYNC OR FLOW TASK    callbacks
             (_runtime_component shoots off ASYNCTASK, then CALL_AWAIT )

    Conversions:
    _to_asyncio_task     ASYNC_DEF
    _call_await          resolves FLOW_BLOCK, FLOW_TASK, and ASYNC)



    On dtype creation:
    - _launch_consumer runs in separate ASYNC TASK
        (calls _dtype_consumer as FLOW BLOCK)

    - _dtype_consumer (calls _run_component as tasks)
        - awaits all.

    """

    def __init__(self, flow: WorkflowEngine, streamer: PubSubClient):
        super().__init__()

        self.flow = flow
        self.streamer = streamer

        # A digital twin workflow has nodes and edges:
        #  - nodes: the actual DTypes
        #  - edges: Investigators, Utility Tasks

        # TODO: special aggregator and split tasks.....

        self.dtype_queues: dict[DataType, asyncio.Queue] = {}

        # the tasks (edges). Defined by the input data type
        self.components: dict[DataType, list[_AnnotatedComponent]] = defaultdict(list)

        # list of barriers: order of PRODUCE --> CONSUME
        self.barriers: dict[DataType, list[Barrier]] = defaultdict(list)

        self.running_tasks: set[asyncio.Task] = set()

        self.is_start = asyncio.Event()

        self.state = RuntimeState.READY
        self.last_error: Optional[str] = None

        # the one teardown, whichever door started it: stop() or a failure.
        # Its presence is also what closes the twin for new work.
        self._stop_task: Optional[asyncio.Task] = None

        # a stalled stream is a twin failure, not a log line
        streamer.on_error = self._record_error

    @property
    def stream_config(self) -> PubSubConfig:
        """This twin's stream endpoint as plain data (see `PubSubConfig`).

        Derived from the injected client, so it cannot go stale.
        """

        return self.streamer.config

    def start(self):
        if self.state is RuntimeState.STOPPED:
            raise RuntimeError("stop() is terminal - this twin cannot be restarted")

        if self.state is RuntimeState.FAILED:
            raise RuntimeError(
                f"twin has failed and cannot be started: {self.last_error}"
            )

        self.state = RuntimeState.RUNNING
        self.is_start.set()

    async def stop(self, timeout: float = STOP_TIMEOUT):
        """Tear this twin down.  Terminal, idempotent, per-twin.

        Idempotent by joining: concurrent and repeated calls await the one
        teardown, so `stop()` only ever returns once the twin is down.

        Cancels every task the runtime owns (component main loops,
        callbacks, dtype consumers, barrier loops), then drops the twin's
        stream subscriptions and closes its stream client.  The execution
        engine is shared and never touched here.

        In-flight backend tasks: cancelling the task that awaits one
        propagates the cancellation into the backend call, which is a
        best-effort cancel.  Whatever has not settled after `timeout` is
        abandoned with a warning -- stop() never waits unboundedly.

        On a twin which already failed this is a bounded no-op: the
        failure tore the twin down on its own, so stop() joins *that*
        teardown (on its budget, not this `timeout`) and leaves the twin
        `failed` -- the error is the more useful fact to report, and
        `last_error` survives.
        """

        if self._stop_task is None:
            # flip the state before scheduling: no new work from here on
            self.state = RuntimeState.STOPPED
            self._stop_task = self._start_teardown(timeout)

        # every caller joins the one teardown; a cancelled caller does not
        # abort it (stop is terminal)
        await asyncio.shield(self._stop_task)

    def _start_teardown(self, timeout: float) -> asyncio.Task:
        """Schedule the one teardown and return its handle.

        Deliberately *not* through `_to_asyncio_task`: teardown cancels
        everything in `running_tasks`, so a teardown registered there
        would cancel itself on its first await.

        The handle is what makes teardown joinable (`stop()`) and
        observable; the done-callback is what keeps it quiet when nobody
        joins it, which is the case whenever a failure started it.
        """

        task = asyncio.ensure_future(self._teardown(timeout))
        task.add_done_callback(self._teardown_done)

        return task

    def _teardown_done(self, task: asyncio.Task):
        """Consume the teardown's outcome: nothing awaits the teardown a
        failure started, and an unretrieved exception would surface as
        loop noise long after the fact.  It is logged, not recorded -- a
        mishap while cleaning up must not overwrite the cause."""

        if task.cancelled():
            return

        exc = task.exception()
        if exc is not None:
            logger.error("twin teardown failed: %s", exc, exc_info=exc)

    async def _teardown(self, timeout: float):
        tasks, self.running_tasks = self.running_tasks, set()
        for task in tasks:
            task.cancel()

        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=timeout)
            if pending:
                logger.warning(
                    "abandoning %d task(s) which ignored cancellation: %s",
                    len(pending),
                    ", ".join(str(task.get_coro()) for task in pending),
                )

        try:
            await asyncio.wait_for(self.streamer.close(), timeout)
        except asyncio.TimeoutError:
            logger.warning("stream client did not close within %ss", timeout)
        except Exception as exc:
            self._record_error(exc)

    async def _call_await(self, func, *args, **kwargs):
        await func(*args, **kwargs)

    def _to_asyncio_task(self, func, *args, **kwargs) -> Optional[asyncio.Task]:
        # a twin which is being torn down starts no new work: a task
        # registered after teardown swapped `running_tasks` out would never
        # be cancelled by anyone
        if self._stop_task is not None or self.state is RuntimeState.FAILED:
            logger.debug("twin is %s - not running %s", self.state, func)
            return None

        task = asyncio.create_task(func(*args, **kwargs))
        self.running_tasks.add(task)
        task.add_done_callback(self._task_done)

        return task

    def _task_done(self, task: asyncio.Task):
        """Done callback for all runtime tasks: cancellation-safe, and it
        routes component failures into the twin state instead of dumping
        them into the event loop's exception handler."""

        self.running_tasks.discard(task)

        if task.cancelled():
            return

        exc = task.exception()
        if exc is not None:
            self._record_error(exc)

    def _record_error(self, exc: BaseException):
        """Route a failure into the twin state, and stop the twin.

        A component failure is a twin failure: the other components have
        lost the graph they were part of, so the twin is torn down exactly
        as `stop()` would tear it down -- but it ends up `failed`, with
        `last_error`, rather than `stopped`.

        Called from several doors, all of them synchronous and all of them
        on the host loop: task done-callbacks, the teardown itself, and
        `PubSubClient.on_error` (the stream backends report from the
        done-callback of their receive loop; a backend whose events arrive
        on a foreign thread hands them over with `call_soon_threadsafe`
        first).  Teardown is therefore *scheduled*, never awaited here.

        Re-entrant by construction: the first failure owns the report and
        the teardown, and everything after it is fallout -- teardown
        cancelling the failure's siblings, a stop hook tripping over a
        half-dead component -- which is logged and dropped.
        """

        error = f"{type(exc).__name__}: {exc}"
        logger.error("twin component failed: %s", error, exc_info=exc)

        if self.state is RuntimeState.FAILED:
            # the cause is already recorded, and its teardown is running
            return

        self.last_error = error

        # a stopped twin stays stopped, but keeps the error for inspection
        if self.state is RuntimeState.STOPPED:
            return

        self.state = RuntimeState.FAILED

        if self._stop_task is None:
            self._stop_task = self._start_teardown(STOP_TIMEOUT)

    def _api(self, ant: _AnnotatedComponent) -> RuntimeAPI:
        return RuntimeAPI(self, ant)

    def _check_mutable(self):
        if self.state is RuntimeState.STOPPED:
            raise RuntimeError("twin is stopped - its graph cannot be changed")

    def add_task(
        self,
        task: UtilityTask,
        input_dtype: DataType,
        output_dtype: DataType,
        is_persistent=False,
    ):
        self._check_mutable()

        ant_comp = _AnnotatedComponent(task, input_dtype, output_dtype, is_persistent)

        # Add component to edge dict
        self.components[input_dtype].append(ant_comp)

        # is input_dtype TRUTHY? If so, run it!
        if input_dtype == TRUTHY:
            logger.debug("Added task with input of TRUTHY... Running.")
            true_data = TypedData(TRUTHY, True)
            # call as a block so it recieves Ctrl-C
            self._to_asyncio_task(self._run_component, ant_comp, true_data)

    def add_investigator(
        self,
        investigator: ModelInvestigator,
        input_dtype: DataType,
        output_dtype: DataType,
        *args,
        **kwargs,
    ):
        self._check_mutable()
        assert input_dtype != TRUTHY

        # check: is there already an investigator or agent assigned to this
        # input output pair?
        for r in self.components.get(input_dtype, []):
            if r.output_dtype == output_dtype and (
                isinstance(r.component, ModelInvestigator)
                or isinstance(r.component, SciAgent)
            ):
                raise ValueError(
                    f"Error: investigator or agent already exists with {input_dtype}-->{output_dtype} mapping"
                )

        ant_comp = _AnnotatedComponent(investigator, input_dtype, output_dtype, False)

        # Add component to edge dict
        self.components[input_dtype].append(ant_comp)

        # start up its main loop
        rt = self._api(ant_comp)
        self._to_asyncio_task(investigator.main_loop, rt, *args, **kwargs)

        # is input_dtype TRUTHY? That doesn't make sense for investigators!

    def _internal_add_investigator(self, ant: _AnnotatedComponent):
        # subscribe to model publishes
        # start up its main loop
        rt = self._api(ant)
        self._to_asyncio_task(ant.component.main_loop, rt)

    def add_agent(
        self,
        agent: SciAgent,
        input_dtype: DataType,
        output_dtype: DataType,
        *args,
        **kwargs,
    ):
        self._check_mutable()
        assert input_dtype != TRUTHY

        # check: is there already an investigator or agent assigned to this
        # input output pair?
        for r in self.components.get(input_dtype, []):
            if r.output_dtype == output_dtype and (
                isinstance(r.component, ModelInvestigator)
                or isinstance(r.component, SciAgent)
            ):
                raise ValueError(
                    f"Error: investigator or agent already exists with {input_dtype}-->{output_dtype} mapping"
                )

        ant_comp = _AnnotatedComponent(agent, input_dtype, output_dtype, False)
        logger.debug(f"Add: {ant_comp}")
        # Add component to edge dict
        self.components[input_dtype].append(ant_comp)

        # start up its main loop. Agents get a patched start_investigator
        rt = self._api(ant_comp)
        rt._internal_add_investigator = self._internal_add_investigator
        self._to_asyncio_task(agent.main_loop, rt, *args, **kwargs)

    # agent-to-agent inference communication
    async def _internal_agent_inference(self, in_data: TypedData, req_dtype: DataType):
        # is there an agent registered that can handle in -> request_out?
        for component in self.components.get(in_data.dtype, []):
            # call its selected model
            if component.output_dtype == req_dtype and component.is_persistent is False:
                # check if agent or investigator
                answer = await self._run_component(
                    component, in_data, skip_queue_out=True
                )
                if answer is None:
                    return TypedData(NULL_DTYPE, None)
                return answer

    # add a split task
    async def add_split_task(
        self, task: SplitTask, input_dtype: DataType, output_dtypes: list[DataType]
    ):
        pass

    # add a barrier
    def add_barrier(self, barrier: Barrier):
        self._check_mutable()

        # a barrier spans across multiple dtypes.... add in the order that
        # follows.
        for dtype in barrier.dtypes:
            self.barriers[dtype].append(barrier)
            if len(self.barriers[dtype]) > 1:
                # NOT the first barrier
                self._to_asyncio_task(
                    self._barrier_consumer, dtype, self.barriers[dtype][-2], barrier
                )

        self._to_asyncio_task(barrier.run)

        # I need a consumer per dtype per barrier.

    async def _barrier_consumer(
        self, dtype, get_barrier: Barrier, put_barrier: Barrier
    ):
        while True:
            val = await get_barrier.get(dtype)
            logger.info(
                f"Barrier receive from {get_barrier}:{dtype}. Put to {put_barrier}"
            )
            await put_barrier.put(val)

    # add a data join
    async def add_data_join(self, *dtypes: DataType):
        pass

    async def _run_component(
        self, ant: _AnnotatedComponent, in_data: TypedData, skip_queue_out=False
    ):
        # wait until start
        await self.is_start.wait()
        logger.info(f"Online run: {type(ant.component).__name__}.")

        assert ant.input_dtype == TRUTHY or ant.input_dtype == in_data.dtype

        for cb in ant.subscriptions[RuntimeAPI.ON_INPUT]:
            logger.info(f"Fire ON_INPUT on {cb}")
            self._to_asyncio_task(self._call_await, cb, in_data)
        # and child investigators
        for i_id, investigator in ant.investigators.items():
            for cb in investigator.subscriptions[RuntimeAPI.ON_INPUT]:
                logger.info(f"Fire ON_INPUT on {cb}")
                self._to_asyncio_task(self._call_await, cb, in_data)

        # run the main loop directly
        if isinstance(ant.component, UtilityTask):

            if ant.is_persistent:
                # is persistent, so subscribe to its output
                if (
                    ant.output_dtype in self.components
                    or ant.output_dtype in self.barriers
                ):
                    # has a task registered, but no queue yet.
                    if ant.output_dtype not in self.dtype_queues:
                        self.dtype_queues[ant.output_dtype] = asyncio.Queue()
                        self._to_asyncio_task(self._launch_consumer, ant.output_dtype)

                    logger.info(f"Subscribe to dtype: {ant.output_dtype}")
                    await self.streamer.subscribe_to_dtype(
                        ant.output_dtype, self.dtype_queues[ant.output_dtype]
                    )
                # else: output is null.

                # run mainloop as async task
                rt = self._api(ant)
                logger.info(f"Run {type(ant.component).__name__} main loop")
                self._to_asyncio_task(ant.component.main_loop, rt, in_data)
                return

            rt = self._api(ant)
            logger.info(f"Run {type(ant.component).__name__} main loop")
            answer = await ant.component.main_loop(rt, in_data)

            if answer is None:
                # no downstream tasks. End
                return
            assert isinstance(answer, TypedData)

            if answer.dtype == NULL_DTYPE:
                return

            if answer.dtype != ant.output_dtype:
                raise ValueError(
                    f"Utility Task {ant.component} did not return the correct dtype. Expected: {ant.output_dtype}"
                )

        # item is an investigator - run its inference
        elif isinstance(ant.component, ModelInvestigator):
            # wait until there is an inference task
            await ant.has_published_model.wait()
            assert ant.inference_task is not None

            logger.info(f"Run {type(ant.component).__name__} inference task")
            answer = await ant.inference_task(in_data, **ant.model_kwargs)
            if answer is None:
                return
            assert isinstance(answer, TypedData)
            if answer.dtype != ant.output_dtype:
                raise ValueError(
                    f"Model Investigator {ant.component} returned {answer.dtype} dtype. Expected: {ant.output_dtype}"
                )
            assert isinstance(answer, TypedData)

        else:
            assert isinstance(ant.component, SciAgent)
            # run a science agent. Call its decision task
            await ant.has_published_selector.wait()
            assert ant.model_select_task is not None
            logger.info(f"Run {type(ant.component).__name__} selection task")

            answer_ms = await ant.model_select_task(
                in_data, *ant.model_select_args, **ant.model_select_kwargs
            )

            # answer is an investigator id.
            if isinstance(answer_ms, tuple) and len(answer_ms) == 2:
                i_select, model_kwargs = answer_ms
            else:
                i_select = answer_ms
                model_kwargs = None

            if i_select not in ant.investigators:
                logger.warning("Model selector pointed to non-existent investigator!")
                return

            logger.info(f"Model selector responded with: {i_select}")
            i_select = ant.investigators[i_select]

            if model_kwargs is None:
                model_kwargs = i_select.model_kwargs

            # now, run the inference of the provided investigator
            for cb in i_select.subscriptions[RuntimeAPI.ON_FILTERED_INPUT]:
                logger.info(f"Fire ON_FILTERED_INPUT on {cb}")
                self._to_asyncio_task(self._call_await, cb, in_data)

            await i_select.has_published_model.wait()
            answer = await i_select.inference_task(in_data, **model_kwargs)

            for cb in i_select.subscriptions[RuntimeAPI.ON_FILTERED_OUTPUT]:
                self._to_asyncio_task(self._call_await, cb, answer)

        if ant.output_dtype == NULL_DTYPE:
            return
        assert isinstance(answer, TypedData) and answer.dtype is not NULL_DTYPE

        for cb in ant.subscriptions[RuntimeAPI.ON_OUTPUT]:
            self._to_asyncio_task(self._call_await, cb, answer)

        # alert child investigators
        for i_id, investigator in ant.investigators.items():
            for cb in investigator.subscriptions[RuntimeAPI.ON_OUTPUT]:
                self._to_asyncio_task(self._call_await, cb, in_data)

        if not (skip_queue_out):
            self._put_to_dtype_queue(answer)

        return answer

    ## flow.block
    async def _dtype_consumer(self, input_data: TypedData):
        # Typed data incoming. Run the tasks concurrently, but block until they
        # are all done (except persistent)

        tasks = []
        for task in self.components[input_data.dtype]:
            # run normal
            tasks.append(self._run_component(task, input_data))

        await asyncio.gather(*tasks)

    def _put_to_dtype_queue(self, t_data: TypedData):
        if t_data.dtype == NULL_DTYPE:
            return

        if t_data.dtype in self.dtype_queues:
            logger.info(f"Enqueue: {t_data.dtype}")
            self.dtype_queues[t_data.dtype].put_nowait(t_data)
            return

        # if not in there, check if there are any associated tasks.
        # if not, drop the dtype
        if t_data.dtype not in self.components:
            return

        # has a task registered, but no queue yet.
        self.dtype_queues[t_data.dtype] = asyncio.Queue()
        self.dtype_queues[t_data.dtype].put_nowait(t_data)

        # create consumer task
        logger.info(f"Create listener for: {t_data.dtype}")
        logger.info(f"Enqueue: {t_data.dtype}")
        self._to_asyncio_task(self._launch_consumer, t_data.dtype)

    async def _launch_b_consumer(self, dtype: DataType, creation: asyncio.Event):
        await creation.wait()
        while True:
            t_data = await self.barriers[dtype][-1].get(dtype)
            # process!
            logger.info(
                f"Final dequeue from barrier ({self.barriers[dtype][-1]}): {t_data.dtype}"
            )
            await self._dtype_consumer(t_data)

    async def _launch_consumer(self, dtype: DataType):
        barrier_creation = asyncio.Event()
        self._to_asyncio_task(self._launch_b_consumer, dtype, barrier_creation)
        while True:

            # is input queue available?
            t_data = await self.dtype_queues[dtype].get()
            if len(self.barriers[dtype]) > 0:
                logger.info(
                    f"Dequeue and place to barrier ({self.barriers[dtype][0]}): {t_data.dtype}"
                )
                barrier_creation.set()
                await self.barriers[dtype][0].put(t_data)
            else:
                # process!
                logger.info(f"Dequeue: {t_data.dtype}")
                await self._dtype_consumer(t_data)

    def describe(self) -> dict:
        """Serializable summary of the twin: graph, dtypes, state.

        Introspection only, but this is the format that goes on the wire --
        `print_graph()` is just a rendering of it.
        """

        def described(ant: _AnnotatedComponent) -> dict:
            entry = {
                "component": type(ant.component).__name__,
                "input_dtype": ant.input_dtype.name,
                "output_dtype": ant.output_dtype.name,
                "is_persistent": ant.is_persistent,
            }
            if ant.investigators:
                entry["investigators"] = [
                    described(inv) for inv in ant.investigators.values()
                ]
            return entry

        components = [
            described(ant) for ants in self.components.values() for ant in ants
        ]

        return {
            "namespace": self.streamer.namespace,
            "state": str(self.state),
            "last_error": self.last_error,
            "components": components,
            "dtypes": sorted(
                {entry["input_dtype"] for entry in components}
                | {entry["output_dtype"] for entry in components}
            ),
            # per dtype, the ordered chain of barriers it passes through
            "barriers": {
                dtype.name: [
                    {"name": barrier.name, "hard": barrier.dtypes[dtype]}
                    for barrier in chain
                ]
                for dtype, chain in self.barriers.items()
            },
        }

    def print_graph(self) -> str:
        """Human-readable rendering of `describe()`."""

        info = self.describe()

        by_input: dict[str, list[dict]] = defaultdict(list)
        for entry in info["components"]:
            by_input[entry["input_dtype"]].append(entry)

        lines = ["=" * 30, f"Digital Twin Flow: {info['namespace']} [{info['state']}]"]

        for input_dtype, entries in by_input.items():
            lines.append(f"IN: {input_dtype}")
            for entry in entries:
                lines.append(
                    f"\t{entry['output_dtype']}: {entry['component']}"
                    f" ({entry['is_persistent']})"
                )

        lines.append("BARRIERS: ")
        for dtype, chain in info["barriers"].items():
            hops = "".join(
                f"{barrier['name']}{'' if barrier['hard'] else ']W'} --> "
                for barrier in chain
            )
            lines.append(f"\t {dtype} --> {hops}")

        lines.append("=" * 30)

        out = "\n".join(lines)
        print(f"\n{out}\n")

        return out
