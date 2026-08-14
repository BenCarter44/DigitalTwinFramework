"""Main runtime for the DT framework

High level:
- Allows for user to build a graph of digital twin components (Utility Tasks, Agents, Investigators)
- The runtime then runs the graph.
- The runtime also translates the graph via its own data type resolver for the
in-situ flow via a system of queues.

"""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable, cast

from digitaltwin.lru import LRUCache

from .components import *
from .components import _TwinComponent
from .streaming import *

from radical.asyncflow import WorkflowEngine  # type: ignore
import logging

logger = logging.getLogger(__name__)


# A special component that is called by the runtime for data join.
# Acts like a persistent utility task.
# It's main loop gets multiple streams...
class _JoinComponent(_TwinComponent):
    """Special component for joining multiple input streams into a single output.

    Should only be built by the runtime.

    The component registers a queue for each data type specified in ``join_dtype.dtypes``.
    Incoming :class:`TypedData` objects are queued via :meth:`update`.  Once items are
    present in all queues, :meth:`main_loop` gathers them, creates a
    :class:`JoinedTypedData` instance containing the list of results, and
    publishes it by calling the supplied ``submit_event_fn``.

    The component runs indefinitely until the surrounding runtime cancels it.

    Args:
        join_dtype: The combined data type that represents all input types.
        submit_event_fn: Function that receives the final :class:`JoinedTypedData`.

    Returns:
        None.
    """

    def __init__(self, join_dtype: JoinDataType, submit_event_fn: Callable) -> None:
        # need a queue for each input.
        self.input_queues: dict[DataType, asyncio.Queue] = {}
        self.submit_event_fn = submit_event_fn

        for dtype in join_dtype.dtypes:
            self.input_queues[dtype] = asyncio.Queue(1)  # only holds one item!

        self.out_dtype = join_dtype

    async def update(self, in_data: TypedData):
        # is the data type part of the ones registered?
        if in_data.dtype not in self.input_queues:
            raise ValueError(f"Received data with unexpected type: {in_data.dtype}!")

        # put the item on the queue - wait if busy
        await self.input_queues[in_data.dtype].put(in_data)

    async def main_loop(self):
        # simply wait on all queues, and then publish result
        while True:
            tk = []
            for t in self.input_queues:
                tk.append(self.input_queues[t].get())
            results = await asyncio.gather(*tk)
            out = JoinedTypedData(dtype=self.out_dtype, data=results)
            self.submit_event_fn(out)


@dataclass
class _SharedStruct:
    """Private container for shared subtask state.

    Each label registered via :meth:`RuntimeAPI.register_shared_subtask` receives a
    :class:`_SharedStruct` instance.  It holds an :class:`asyncio.Lock` to
    protect concurrent access, an :class:`LRUCache` for memoising results, and an
    optional ``wrap_fn`` callable that performs the actual task execution while
    honouring the cache.

    Attributes:
        lock: Synchronisation primitive for the cache.
        cache: LRU cache used to store outstanding or completed futures.
        wrap_fn: The wrapped coroutine that implements the heavy work.
    """

    lock: asyncio.Lock
    cache: LRUCache
    wrap_fn: Optional[Callable] = None


@dataclass
class _AnnotatedComponent:
    """Metadata wrapper for a component used by :class:`DTRuntime`.

    The runtime decouples execution logic from component details by associating
    additional information such as input/output data types, runtime events, and
    subscriptions with each component.  The class also keeps track of child
    investigators, shared subtasks, and various control flags used during the
    workflow.

    The attributes mirror those required by :class:`RuntimeAPI` and enable
    dynamic behavior such as publishing new models, selecting investigative
    models, and registering shared tasks.
    """

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
    model_publish_cb: Optional[Callable] = None
    split_outputs: Optional[tuple[DataType]] = None

    shared_tasks: dict[SharedSubtaskLabel, _SharedStruct] = field(default_factory=dict)


class RuntimeAPI(ABC):
    """External API that components can use to interact with the Digital-Twin
    runtime.

    ``RuntimeAPI`` exposes methods for:
    * Subscribing to runtime events (inputs, outputs, model publishes, etc.).
    * Publishing a freshly trained model and notifying observers.
    * Registering an inference callback used by investigators.
    * Managing investigators and model selectors in ``SciAgent`` instances.
    * Handling shared sub-task memoisation for agents.

    The class stores a reference to its annotated component and keeps track of
    background tasks that are spawned when publishing callbacks.
    """

    ON_INPUT = "runtime/ON_INPUT"
    ON_OUTPUT = "runtime/ON_OUTPUT"
    ON_MODEL_PUBLISH = "runtime/ON_PUBLISH"
    ON_FILTERED_INPUT = "runtime/ON_FILTER_INPUT"
    ON_FILTERED_OUTPUT = "runtime/ON_FILTER_OUTPUT"

    def __init__(self, ant: _AnnotatedComponent, agent_inf: Callable) -> None:
        """Create the runtime API facade for a component.

        Args:
            ant: The annotated component that this API will control.
            agent_inf: Callable used by the runtime to forward inference requests
                to other agents.
        """

        self._ant = ant
        self._internal_add_investigator: Optional[Callable] = None
        self._internal_agent_inference: Callable = agent_inf
        self._background_tasks: set[asyncio.Task] = set()

        if isinstance(self._ant.component, SplitTask):
            self._cmp_type = f"SPLIT"
        elif isinstance(self._ant.component, UtilityTask):
            self._cmp_type = (
                f"UTILITY-{'persist' if self._ant.is_persistent else 'regular'}"
            )
        elif isinstance(self._ant.component, ModelInvestigator):
            self._cmp_type = f"INVESTIGATOR"
        elif isinstance(self._ant.component, SciAgent):
            self._cmp_type = f"AGENT"
        elif isinstance(self._ant.component, _JoinComponent):
            self._cmp_type = f"JOIN"
        else:
            raise ValueError("Unknown component type!")

    def subscribe_to_topic(self, topic: str, task: Callable) -> None:
        """Register a callback under a specific runtime event.

        The callback will be invoked whenever the component receives an event
        from the runtime.

        Supported events:
            * :pyattr:`ON_INPUT` - send all input received by the agent.
            * :pyattr:`ON_OUTPUT` - send all output emitted by the agent.
            * :pyattr:`ON_MODEL_PUBLISH` - for agents when an investigator
              publishes a model.
            * :pyattr:`ON_FILTERED_INPUT` - invoke only for selected
              investigators.
            * :pyattr:`ON_FILTERED_OUTPUT` - invoke only for selected
              investigators.

        Args:
            topic: Name of the runtime event.
            task: Callback to register.
        """

        assert self._cmp_type in ["INVESTIGATOR", "AGENT"]
        self._ant.subscriptions[topic].append(task)

    def publish_new_model(self, model_kwargs={}, acc_kwargs={}) -> None:
        """Publish a newly trained model from an agent.

        The method stores the provided ``model_kwargs`` and ``acc_kwargs`` on the
        annotated component, sets an internal event to signal that a model has
        been published, and optionally triggers a callback if one is defined.

        Args:
            model_kwargs: Keyword arguments describing the model.
            acc_kwargs: Keyword arguments describing accuracy or evaluation.
        """

        assert self._cmp_type in ["INVESTIGATOR"]
        self._ant.model_kwargs = model_kwargs
        self._ant.accuracy_kwargs = acc_kwargs
        self._ant.has_published_model.set()
        if self._ant.model_publish_cb is not None:
            bk = asyncio.create_task(
                self._ant.model_publish_cb(
                    self._ant.component, model_kwargs, acc_kwargs
                )
            )
            self._background_tasks.add(bk)

            def done(r) -> None:
                r.result()  # for error propagation
                self._background_tasks.discard(r)

            bk.add_done_callback(done)

    def set_inference_task(self, task: Callable) -> None:
        """Associate an inference callback with an investigator.

        The callback is stored on the annotated component and will be invoked
        when the investigator receives input data.

        Args:
            task: Callable that implements the inference logic.
        """

        assert self._cmp_type in ["INVESTIGATOR"]
        self._ant.inference_task = task

    def start_investigator(self, investigator: ModelInvestigator):
        """Begin monitoring an investigator from a ``SciAgent``.

        The investigator is wrapped in an :class:`_AnnotatedComponent` and
        added to the agent's investigator registry.  A background task running
        ``investigator.main_loop`` is scheduled to execute the investigation
        logic.

        Args:
            investigator: The investigator component to start.
        """

        assert self._cmp_type in ["AGENT"]
        if self._internal_add_investigator is None:
            raise ValueError("Only can start an investigator inside a SciAgent")

        count = len(self._ant.investigators)

        new = _AnnotatedComponent(
            investigator,
            input_dtype=self._ant.input_dtype,
            output_dtype=self._ant.output_dtype,
            is_persistent=False,
        )
        assert isinstance(self._ant.component, SciAgent)
        new.model_publish_cb = self._ant.component.model_publish_cb
        investigator.runtime_id = count
        self._ant.investigators[count] = new
        self._internal_add_investigator(new)  # calls the loop

    # receives TypedData. Outputs investigator ID.
    def set_model_selection_task(self, task: Callable) -> None:
        """Set the model-selection callback used by a ``SciAgent``.

        The callback chooses an investigator ID or tuple of (ID, kwargs) based on
        input data.

        Args:
            task: Callable that returns selected investigator information.
        """

        assert self._cmp_type in ["AGENT"]
        self._ant.model_select_task = task

    def update_model_selector(self, *args, **kwargs) -> None:
        """Publish arguments for a model-selection call.

        The ``SciAgent`` uses these arguments to invoke :meth:`model_select_task`.

        Args:
            *args: Positional arguments for the selector.
            **kwargs: Keyword arguments for the selector.
        """

        assert self._cmp_type in ["AGENT"]
        self._ant.model_select_args = args
        self._ant.model_select_kwargs = kwargs
        self._ant.has_published_selector.set()

    # the learner can request inference from other agents --- is blocking
    async def get_inference(
        self, input_d: TypedData, output_dtype: DataType
    ) -> TypedData:
        """Request inference from another agent.

        The runtime forwards the request to the appropriate registered agent
        component.  The call is awaited and the resulting :class:`TypedData`
        instance is returned.

        Args:
            input_d: Input data to forward.
            output_dtype: Desired output data type.

        Returns:
            ``TypedData`` produced by the requested agent.
        """

        assert self._cmp_type not in ["JOIN"]
        return await self._internal_agent_inference(input_d, output_dtype)

    # for shared SIMs in the agent.
    def register_shared_subtask(
        self, label: SharedSubtaskLabel, task: Callable, lru_size: int = 128
    ):
        """Register a memoised sub-task that can be shared across investigators.

        The shared task is wrapped to keep a per-label LRU cache.  The wrapper
        ensures that concurrent invocations wait for the same cache entry and
        results are returned from the cache when available.

        Args:
            label: Unique identifier for the shared task.
            task: The coroutine function to execute.
            lru_size: Maximum number of cached results.

        Returns:
            Wrapped coroutine that implements cache logic.
        """

        assert self._cmp_type in ["AGENT"]
        logger.info(f"Register shared subtask with label {label}. LRU size: {lru_size}")

        async def wrapper(*args, **kwargs):
            # task must be awaitable
            return await task(*args, **kwargs)

        cache = LRUCache(lru_size)
        lock = asyncio.Lock()
        self._ant.shared_tasks[label] = _SharedStruct(lock=lock, cache=cache)

        async def fetch_wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            struct = self._ant.shared_tasks[label]

            await struct.lock.acquire()

            if struct.cache.exists(key):
                logger.info(
                    f"Computation of {label} {key if len(str(key)) < 20 else ''} saved. Return future."
                )
                fut = await struct.cache.fetch_item(key)
            else:
                logger.info(
                    f"Begin compute of {label} {key if len(str(key)) < 20 else ''}. Return future."
                )
                fut = asyncio.ensure_future(wrapper(*args, **kwargs))
                await struct.cache.put_item(key, fut)

            struct.lock.release()
            return await fut

        # store wrapped function
        self._ant.shared_tasks[label].wrap_fn = fetch_wrapper

        # add to investigators
        for idx, inv_ant in self._ant.investigators.items():
            inv_ant.shared_tasks[label] = self._ant.shared_tasks[label]

        return fetch_wrapper

    async def call_shared_subtask(self, label: SharedSubtaskLabel, *args, **kwargs):
        """Invoke a previously-registered shared sub-task.

        The method forwards the call to the cached wrapper stored during
        registration.

        Args:
            label: The shared task identifier.
            *args, **kwargs: Arguments for the wrapped task.

        Returns:
            Result of the underlying coroutine.
        """

        assert self._cmp_type in ["AGENT", "INVESTIGATOR"]
        # uses the shared_tasks dict in the annotated component
        # reference was copied to investigator by agent
        if label not in self._ant.shared_tasks:
            raise ValueError(
                f"Unknown shared task label: {label}. Expected: {list(self._ant.shared_tasks.keys())}"
            )
        assert self._ant.shared_tasks[label].wrap_fn is not None
        return await self._ant.shared_tasks[label].wrap_fn(*args, **kwargs)  # type: ignore

    def get_shared_subtask(self, label: SharedSubtaskLabel):
        """Retrieve the wrapped callable for a registered shared sub-task.

        The returned callable can be invoked directly and will honour the LRU
        cache.

        Args:
            label: The shared task identifier.

        Returns:
            Wrapped coroutine implementing the cached logic.
        """

        assert self._cmp_type in ["AGENT", "INVESTIGATOR"]
        # uses the shared_tasks dict in the annotated component
        # reference was copied to investigator by agent.
        #
        # Simply returns the callable.
        if label not in self._ant.shared_tasks:
            raise ValueError(
                f"Unknown shared task label: {label}. Expected: {list(self._ant.shared_tasks.keys())}"
            )
        assert self._ant.shared_tasks[label].wrap_fn is not None

        return self._ant.shared_tasks[label].wrap_fn


class DTRuntime:
    """Workflow builder and dynamic manager.

    The :class:`DTRuntime` orchestrates the execution of Digital Twin
    components.  It keeps an adjacency list of components per input data type,
    queues for data distribution, and supports special operators such as JOINs,
    SPLITs, and Barriers for synchronizing event order.

    Typical usage example:

    .. code-block:: python

        flow = WorkflowEngine()
        streamer = PubSubClient()
        dt = DTRuntime(flow, streamer)
        dt.add_task(...)      # register UtilityTask or SplitTask
        dt.add_investigator(...)   # register investigator edges
        dt.add_agent(...)          # register SciAgent edges
        dt.start()                 # enable processing

    All public methods are documented below.
    """

    def __init__(self, flow: WorkflowEngine, streamer: PubSubClient) -> None:
        """Create a runtime from the provided workflow engine and PubSub
        client.

        Args:
            flow: Instance of :class:`radical.asyncflow.WorkflowEngine`
                used to schedule and block tasks.
            streamer: Backend pub/sub client used to forward messages between
                components.
        """

        super().__init__()

        self.flow = flow
        self.streamer = streamer

        # A digital twin workflow has nodes and edges:
        #  - nodes: the actual DTypes
        #  - edges: Investigators, Utility Tasks

        self._dtype_queues: dict[DataType, asyncio.Queue] = {}

        # the tasks (edges). Defined by the input data type
        self._components: dict[DataType, list[_AnnotatedComponent]] = defaultdict(list)

        # list of barriers: order of PRODUCE --> CONSUME
        self._barriers: dict[DataType, list[Barrier]] = defaultdict(list)

        # join registry so that there are no duplicates
        self._join_components: dict[JoinDataType, _JoinComponent] = {}

        self._truthy_list: list[_AnnotatedComponent] = []

        self.running_aio_tasks: set[asyncio.Task] = set()

        self._is_start = asyncio.Event()

        @flow.block
        async def to_block(func, *args, **kwargs) -> None:
            await func(*args, **kwargs)

        self._to_block = to_block

    def start(self) -> None:
        """Signal that the runtime is ready and allow queued components to
        begin processing.

        The method simply fires an internal event that other coroutine
        functions wait on before executing.
        """

        self._is_start.set()

    async def _call_await(self, func, *args, **kwargs) -> None:
        """Await a given coroutine function.

        The helper wraps an awaitable in a non-blocking fashion but still keeps
        stack traces intact.

        Args:
            func: Coroutine function to await.
            *args, **kwargs: Arguments for ``func``.
        """

        await func(*args, **kwargs)

    def _to_asyncio_task(self, func, *args, **kwargs) -> None:
        """Schedule a coroutine as an :class:`asyncio.Task` and track its
        completion.

        The method creates a background :class:`asyncio.Task` and registers a
        ``done`` callback to automatically propagate exceptions and to remove
        the finished task from the internal tracker.

        Args:
            func: Coroutine function to run.
            *args, **kwargs: Arguments to ``func``.
        """

        result = asyncio.create_task(func(*args, **kwargs))
        self.running_aio_tasks.add(result)

        def done(r) -> None:
            r.result()  # for error propagation
            self.running_aio_tasks.discard(r)

        result.add_done_callback(done)

    def add_task(
        self,
        task: UtilityTask,
        input_dtype: DataType,
        output_dtype: DataType,
        is_persistent: bool = False,
    ) -> None:
        """Register a utility task in the runtime.

        A :class:`UtilityTask` produces an output :class:`TypedData` given an input
        instance.  If persistent, its mainloop will run detached and output TypedData is
        expected to be published via the PubSubClient.

        Args:
            task: UtilityTask instance.
            input_dtype: Data type that the task consumes.
            output_dtype: Desired output data type.
            is_persistent: Persistence flag for the task.
        """

        ant_comp = _AnnotatedComponent(task, input_dtype, output_dtype, is_persistent)

        # Add component to edge dict
        self._components[input_dtype].append(ant_comp)

        # is input_dtype TRUTHY? If so, run it!
        if input_dtype == TRUTHY:
            logger.debug("Added task with input of TRUTHY... Running.")
            true_data = TypedData(TRUTHY, True)
            # call as a block so it receives Ctrl-C
            self._to_asyncio_task(self._run_component, ant_comp, true_data)
            self._truthy_list.append(ant_comp)

    def add_investigator(
        self,
        investigator: ModelInvestigator,
        input_dtype: DataType,
        output_dtype: DataType,
        *args,
        **kwargs,
    ):
        """Register a model investigator.


        A model investigator's main loop will be started immediately.
        The model investigator's inference task will be called for in-situ
        inference.

        Args:
            investigator: Investigator to add.
            input_dtype: Input data type consumed.
            output_dtype: Output data type produced.
            *args, **kwargs: Additional keyword arguments for ``investigator.main_loop``.
        """

        assert input_dtype != TRUTHY

        # check: is there already an investigator or agent assigned to this
        # input output pair?
        for r in self._components.get(input_dtype, []):
            if r.output_dtype == output_dtype and (
                isinstance(r.component, ModelInvestigator)
                or isinstance(r.component, SciAgent)
            ):
                raise ValueError(
                    f"Error: investigator or agent already exists with {input_dtype}-->{output_dtype} mapping"
                )

        ant_comp = _AnnotatedComponent(investigator, input_dtype, output_dtype, False)

        # Add component to edge dict
        self._components[input_dtype].append(ant_comp)

        # start up its main loop
        rt = RuntimeAPI(ant_comp, self._internal_agent_inference)
        self._to_asyncio_task(investigator.main_loop, rt, *args, **kwargs)

    def _internal_add_investigator(self, ant: _AnnotatedComponent) -> None:
        """Internal helper to add an investigator to the runtime.

        The method subscribes investigators to relevant dtype topics and
        schedules their main loops for execution.

        Args:
            ant: Annotated component representing the investigator.
        """

        # subscribe to model publishes
        # start up its main loop
        rt = RuntimeAPI(ant, self._internal_agent_inference)
        self._to_asyncio_task(ant.component.main_loop, rt)

    def add_agent(
        self,
        agent: SciAgent,
        input_dtype: DataType,
        output_dtype: DataType,
        *args,
        **kwargs,
    ):
        """Register a science agent.

        A science agent's main loop will be run right away. The model selection
        task will be called for in-situ inference.

        Args:
            agent: ``SciAgent`` instance.
            input_dtype: Data type consumed.
            output_dtype: Data type produced.
            *args, **kwargs: Additional arguments for ``agent.main_loop``.
        """

        assert input_dtype != TRUTHY

        # check: is there already an investigator or agent assigned to this
        # input output pair?
        for r in self._components.get(input_dtype, []):
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
        self._components[input_dtype].append(ant_comp)

        # start up its main loop. Agents get a patched start_investigator
        rt = RuntimeAPI(ant_comp, self._internal_agent_inference)
        rt._internal_add_investigator = self._internal_add_investigator
        self._to_asyncio_task(agent.main_loop, rt, *args, **kwargs)

    # agent-to-agent inference communication
    async def _internal_agent_inference(self, in_data: TypedData, req_dtype: DataType):
        """Forward agent-to-agent inference requests.

        The method looks up agents registered for the input ``dtype`` and
        requests inference. Bypasses queues, runs in a blocking manner.

        Args:
            in_data: Input data.
            req_dtype: Desired output data type.

        Returns:
            ``TypedData``
        """

        for component in self._components.get(in_data.dtype, []):
            if component.output_dtype == req_dtype and component.is_persistent is False:
                answer = await self._run_component(
                    component, in_data, skip_queue_out=True
                )
                if answer is None:
                    return TypedData(NULL_DTYPE, None)
                return answer

    # add a barrier
    def add_barrier(self, barrier: Barrier) -> None:
        """Register a synchronization barrier.

        A :class:`Barrier` can span multiple data types.  The method schedules
        consumers that route data from the previous barrier to the next one.

        Args:
            barrier: :class:`Barrier` instance to add.
        """

        for dtype in barrier.dtypes:
            self._barriers[dtype].append(barrier)
            if len(self._barriers[dtype]) > 1:
                # NOT the first barrier
                self._to_asyncio_task(
                    self._barrier_consumer, dtype, self._barriers[dtype][-2], barrier
                )

        barrier.start()

        # I need a consumer per dtype per barrier.

    async def _barrier_consumer(
        self, dtype, get_barrier: Barrier, put_barrier: Barrier
    ) -> None:
        """Consume data from one barrier and forward it to the next.

        The consumer continuously waits for data matching ``dtype`` on
        ``get_barrier`` and forwards it to ``put_barrier``.

        Args:
            dtype: Data type routed through the barriers.
            get_barrier: Barrier that provides data.
            put_barrier: Barrier that consumes data.
        """

        while True:
            val = await get_barrier.get(dtype)
            logger.debug(
                f"Barrier receive from {get_barrier}:{dtype}. Put to {put_barrier}"
            )
            await put_barrier.put(val)

    # add a data join
    def add_data_join(self, join_dtype: JoinDataType):
        """Create a data-join component that waits on all input streams.

        The data join task will produce data tagged with the given join dtype.

        Args:
            join_dtype: Combined data type representing the join.
        """

        if join_dtype in self._join_components:
            raise ValueError("Data join already exists for that type")

        cmp = _JoinComponent(join_dtype, self._put_to_dtype_queue)
        self._join_components[join_dtype] = cmp

        # add to component registry
        for dtype in join_dtype.dtypes:
            # component handles its own output
            ant_comp = _AnnotatedComponent(cmp, dtype, join_dtype)
            self._components[dtype].append(ant_comp)

        # start main loop
        self._to_asyncio_task(self._join_components[join_dtype].main_loop)

    # add a data split task - just about the same as a utility task

    def add_data_split_task(
        self, task: SplitTask, input_dtype: DataType, output_dtypes: tuple[DataType]
    ):
        """Register a split task that forwards a single input into multiple
        output data types.

        Args:
            task: Split task instance.
            input_dtype: Input data type consumed.
            output_dtypes: Tuple of output data types produced.
        """

        assert input_dtype != TRUTHY

        ant_comp = _AnnotatedComponent(task, input_dtype, NULL_DTYPE, False)
        ant_comp.split_outputs = tuple(output_dtypes)
        self._components[input_dtype].append(ant_comp)

    async def _run_component(
        self, ant: _AnnotatedComponent, in_data: TypedData, skip_queue_out: bool = False
    ):
        """Execute a component's main loop / its inference tasks.

        This is the meat and potatoes of the runtime, running each component and
        calling necessary subscriptions.

        Args:
            ant: Annotated component to run.
            in_data: Input :class:`TypedData` instance.
            skip_queue_out: Flag indicating whether to skip enqueuing the
                output for the consumer.

        Returns:
            Final ``TypedData`` produced, or ``None`` for special cases (e.g.
            joins and persistent utility tasks.)
        """

        await self._is_start.wait()
        logger.info(f"Online run: {type(ant.component).__name__}.")

        assert ant.input_dtype == TRUTHY or ant.input_dtype == in_data.dtype

        # is the component a data JOIN?
        # special handling
        if isinstance(ant.component, _JoinComponent):
            await ant.component.update(in_data)
            return  # NULL_VAL.. Output done by component directly

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
                    ant.output_dtype in self._components
                    or ant.output_dtype in self._barriers
                ):
                    # has a task registered, but no queue yet.
                    if ant.output_dtype not in self._dtype_queues:
                        self._dtype_queues[ant.output_dtype] = asyncio.Queue()
                        self._to_asyncio_task(self._launch_consumer, ant.output_dtype)

                    logger.info(f"Subscribe to dtype: {ant.output_dtype}")
                    await self.streamer.subscribe_to_dtype(
                        ant.output_dtype, self._dtype_queues[ant.output_dtype]
                    )
                # else: output is null.

                # run mainloop as async task
                rt = RuntimeAPI(ant, self._internal_agent_inference)
                logger.debug(f"Run {type(ant.component).__name__} main loop")
                self._to_asyncio_task(ant.component.main_loop, rt, in_data)
                return

            rt = RuntimeAPI(ant, self._internal_agent_inference)
            logger.debug(f"Run {type(ant.component).__name__} main loop")
            answer = await ant.component.main_loop(rt, in_data)

            # for split tasks, treat the answer differently
            # splits also don't support an output callback
            if isinstance(ant.component, SplitTask):
                # do checks
                assert answer is not None
                l_answer = cast(tuple[TypedData], answer)  # type: ignore
                assert ant.split_outputs is not None
                assert len(l_answer) == len(ant.split_outputs)

                for i in range(len(l_answer)):
                    assert (
                        l_answer[i] is None or l_answer[i].dtype == ant.split_outputs[i]
                    )

                # checks done, send out. None acts as a blank
                for part in l_answer:
                    if part is None:
                        continue
                    self._put_to_dtype_queue(part)
                return

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

            logger.debug(f"Run {type(ant.component).__name__} inference task")
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
            logger.debug(f"Run {type(ant.component).__name__} selection task")

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

            logger.debug(f"Model selector responded with: {i_select}")
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

        if not skip_queue_out:
            self._put_to_dtype_queue(answer)

        return answer

    ## flow.block
    async def _dtype_consumer(self, input_data: TypedData) -> None:
        """Consume data for a given :class:`DataType`.

        The method launches all components registered for the specific data
        type concurrently, waiting for all to complete before returning.

        Args:
            input_data: Incoming :class:`TypedData` instance.

        Returns:
            None. All component loops are executed concurrently.
        """

        tasks = []
        for task in self._components[input_data.dtype]:
            tasks.append(self._run_component(task, input_data))

        await asyncio.gather(*tasks)

    def _put_to_dtype_queue(self, t_data: TypedData) -> None:
        """Enqueue a :class:`TypedData` instance for later consumption.

        If a queue already exists for the data type, the instance is queued.
        Otherwise, a new queue is created and a consumer task is scheduled to process the data.

        Args:
            t_data: Typed data to enqueue.
        """

        if t_data.dtype == NULL_DTYPE:
            return

        if t_data.dtype in self._dtype_queues:
            logger.info(f"Enqueue: {t_data.dtype}")
            self._dtype_queues[t_data.dtype].put_nowait(t_data)
            return

        # if not in there, check if there are any associated tasks.
        # if not, drop the dtype
        if t_data.dtype not in self._components:
            return

        # has a task registered, but no queue yet.
        self._dtype_queues[t_data.dtype] = asyncio.Queue()
        self._dtype_queues[t_data.dtype].put_nowait(t_data)

        # create consumer task
        logger.info(f"Create listener for: {t_data.dtype}")
        logger.info(f"Enqueue: {t_data.dtype}")
        self._to_asyncio_task(self._launch_consumer, t_data.dtype)

    async def _launch_b_consumer(
        self, dtype: DataType, creation: asyncio.Event
    ) -> None:
        """Background consumer that waits for barrier creation and processes data.

        The coroutine continuously pulls data from the last barrier for ``dtype`` and
        forwards it to the generic dtype consumer.

        Args:
            dtype: Data type to consume.
            creation: Event fired when the barrier is ready.
        """

        await creation.wait()
        while True:
            t_data = await self._barriers[dtype][-1].get(dtype)
            logger.info(
                f"Final dequeue from barrier ({self._barriers[dtype][-1]}): {t_data.dtype}"
            )
            await self._dtype_consumer(t_data)

    async def _launch_consumer(self, dtype: DataType) -> None:
        """Consume data from the local queue and route it to barriers or consumers.

        The coroutine pulls typed data from the internal queue, passes it to the
        first barrier if present, or directly to the dtype consumer.

        Args:
            dtype: Data type to consume.
        """

        barrier_creation = asyncio.Event()
        self._to_asyncio_task(self._launch_b_consumer, dtype, barrier_creation)
        while True:

            # is input queue available?
            t_data = await self._dtype_queues[dtype].get()
            if len(self._barriers[dtype]) > 0:
                logger.info(
                    f"Dequeue and place to barrier ({self._barriers[dtype][0]}): {t_data.dtype}"
                )
                barrier_creation.set()
                await self._barriers[dtype][0].put(t_data)
            else:
                # process!
                logger.info(f"Dequeue: {t_data.dtype}")
                await self._dtype_consumer(t_data)

    def print_graph(self):
        """Print a textual representation of the current digital twin graph.

        The method outputs a formatted diagram showing inputs, outputs,
        persistent utilities, splits, joins, and barriers.  It returns a string
        representation identical to the printed output.

        Returns:
            Formatted string of the graph.
        """

        print()
        print("=" * 30)
        out = "=" * 30 + "\n"
        print("Digital Twin Flow:     IN <DTYPE> | OUT <DTYPE> (persist)")
        out += "Digital Twin Flow:     IN <DTYPE> | OUT <DTYPE> (persist)\n"

        # start with TRUTHY
        print("IN: (TRUTHY)")
        out += "IN: (TRUTHY)\n"

        for ant in self._truthy_list:
            print(
                f"\t{ant.output_dtype}: {type(ant.component).__name__} ({ant.is_persistent})"
            )
            out += f"\t{ant.output_dtype}: {type(ant.component).__name__} ({ant.is_persistent})\n"

        # rest of tasks
        for input_dtype in self._components:
            if input_dtype == TRUTHY:
                continue
            print(f"IN: {input_dtype}")
            out += f"IN: {input_dtype}\n"
            for ant in self._components[input_dtype]:
                if isinstance(ant.component, _JoinComponent):
                    print(f"\t{ant.output_dtype}")
                    out += f"\t{ant.output_dtype}\n"
                    continue
                if isinstance(ant.component, SplitTask):
                    print(f"\tSPLIT: {type(ant.component).__name__}")
                    out += f"\tSPIT: {type(ant.component).__name__}\n"
                    for i, a in enumerate(ant.split_outputs):
                        print(f"\t\t{i}. {a}")
                        out += f"\t\t{i}. {a}\n"
                    continue

                print(
                    f"\t{ant.output_dtype}: {type(ant.component).__name__}  ({ant.is_persistent})"
                )
                out += f"\t{ant.output_dtype}: {type(ant.component).__name__}  ({ant.is_persistent})\n"

        print("BARRIERS: ")
        out += "BARRIERS: \n"
        for dtype in self._barriers:
            print(f"\t {dtype} -|-> ", end="")
            out += f"\t {dtype} -|-> "
            for barrier in self._barriers[dtype]:
                is_hard = barrier.dtypes[dtype]
                print(f"{barrier.name}{'' if is_hard else ']W'} -|-> ", end="")
                out += f"{barrier.name}{'' if is_hard else ']W'} -|-> "
            print()
            out += "\n"

        print("=" * 30)
        out += "=" * 30
        print()
        return out
