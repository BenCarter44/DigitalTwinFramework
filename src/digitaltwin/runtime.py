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
    lock: asyncio.Lock
    cache: LRUCache
    wrap_fn: Optional[Callable] = None


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
    model_publish_cb: Optional[Callable] = None
    split_outputs: Optional[tuple[DataType]] = None

    shared_tasks: dict[SharedSubtaskLabel, _SharedStruct] = field(default_factory=dict)


class RuntimeAPI(ABC):
    ON_INPUT = "runtime/ON_INPUT"
    ON_OUTPUT = "runtime/ON_OUTPUT"
    ON_MODEL_PUBLISH = "runtime/ON_PUBLISH"
    ON_FILTERED_INPUT = "runtime/ON_FILTER_INPUT"
    ON_FILTERED_OUTPUT = "runtime/ON_FILTER_OUTPUT"

    def __init__(self, ant: _AnnotatedComponent, agent_inf: Callable) -> None:
        self._ant = ant
        self._internal_add_investigator: Optional[Callable] = None
        self._internal_agent_inference: Callable = agent_inf
        self._background_tasks: set[asyncio.Task] = set()

        if isinstance(self._ant.component, SplitTask):
            self.cmp_type = f"SPLIT"
        elif isinstance(self._ant.component, UtilityTask):
            self.cmp_type = (
                f"UTILITY-{'persist' if self._ant.is_persistent else 'regular'}"
            )
        elif isinstance(self._ant.component, ModelInvestigator):
            self.cmp_type = f"INVESTIGATOR"
        elif isinstance(self._ant.component, SciAgent):
            self.cmp_type = f"AGENT"
        elif isinstance(self._ant.component, _JoinComponent):
            self.cmp_type = f"JOIN"
        else:
            raise ValueError("Unknown component type!")

    def subscribe_to_topic(self, topic: str, task: Callable) -> None:
        assert self.cmp_type in ["INVESTIGATOR", "AGENT"]
        self._ant.subscriptions[topic].append(task)

    def publish_new_model(self, model_kwargs={}, acc_kwargs={}) -> None:
        assert self.cmp_type in ["AGENT"]
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
        assert self.cmp_type in ["INVESTIGATOR"]
        self._ant.inference_task = task

    def start_investigator(self, investigator: ModelInvestigator):
        assert self.cmp_type in ["AGENT"]
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
        assert self.cmp_type in ["AGENT"]
        self._ant.model_select_task = task

    def update_model_selector(self, *args, **kwargs) -> None:
        assert self.cmp_type in ["AGENT"]
        self._ant.model_select_args = args
        self._ant.model_select_kwargs = kwargs
        self._ant.has_published_selector.set()

    # the learner can request inference from other agents --- is blocking
    async def get_inference(
        self, input_d: TypedData, output_dtype: DataType
    ) -> TypedData:
        assert self.cmp_type not in ["JOIN"]
        return await self._internal_agent_inference(input_d, output_dtype)

    # for shared SIMs in the agent.
    def register_shared_subtask(
        self, label: SharedSubtaskLabel, task: Callable, lru_size: int = 128
    ):
        assert self.cmp_type in ["AGENT"]
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
                logger.info(f"Computation of {label} with {key} saved! Return future.")
                fut = await struct.cache.fetch_item(key)
            else:
                logger.info(f"Begin compute of {label} with {key}! Return future.")
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
        assert self.cmp_type in ["AGENT", "INVESTIGATOR"]
        # uses the shared_tasks dict in the annotated component
        # reference was copied to investigator by agent
        if label not in self._ant.shared_tasks:
            raise ValueError(
                f"Unknown shared task label: {label}. Expected: {list(self._ant.shared_tasks.keys())}"
            )
        assert self._ant.shared_tasks[label].wrap_fn is not None
        return await self._ant.shared_tasks[label].wrap_fn(*args, **kwargs)  # type: ignore

    def get_shared_subtask(self, label: SharedSubtaskLabel):
        assert self.cmp_type in ["AGENT", "INVESTIGATOR"]
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
    _to_block            FLOW_BLOCK
    _call_await          resolves FLOW_BLOCK, FLOW_TASK, and ASYNC)



    On dtype creation:
    - _launch_consumer runs in separate ASYNC TASK
        (calls _dtype_consumer as FLOW BLOCK)

    - _dtype_consumer (calls _run_component as tasks)
        - awaits all.

    """

    def __init__(self, flow: WorkflowEngine, streamer: PubSubClient) -> None:
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

        # join registry so that there are no duplicates
        self.join_components: dict[JoinDataType, _JoinComponent] = {}

        self.truthy_list: list[_AnnotatedComponent] = []

        self.running_tasks: set[asyncio.Task] = set()

        self.is_start = asyncio.Event()

        @flow.block
        async def to_block(func, *args, **kwargs) -> None:
            await func(*args, **kwargs)

        self._to_block = to_block

    def start(self) -> None:
        self.is_start.set()

    async def _call_await(self, func, *args, **kwargs) -> None:
        await func(*args, **kwargs)

    def _to_asyncio_task(self, func, *args, **kwargs) -> None:
        result = asyncio.create_task(func(*args, **kwargs))
        self.running_tasks.add(result)

        def done(r) -> None:
            r.result()  # for error propagation
            self.running_tasks.discard(r)

        result.add_done_callback(done)

    def add_task(
        self,
        task: UtilityTask,
        input_dtype: DataType,
        output_dtype: DataType,
        is_persistent: bool = False,
    ) -> None:

        ant_comp = _AnnotatedComponent(task, input_dtype, output_dtype, is_persistent)

        # Add component to edge dict
        self.components[input_dtype].append(ant_comp)

        # is input_dtype TRUTHY? If so, run it!
        if input_dtype == TRUTHY:
            logger.debug("Added task with input of TRUTHY... Running.")
            true_data = TypedData(TRUTHY, True)
            # call as a block so it receives Ctrl-C
            self._to_asyncio_task(self._run_component, ant_comp, true_data)
            self.truthy_list.append(ant_comp)

    def add_investigator(
        self,
        investigator: ModelInvestigator,
        input_dtype: DataType,
        output_dtype: DataType,
        *args,
        **kwargs,
    ):
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
        rt = RuntimeAPI(ant_comp, self._internal_agent_inference)
        self._to_asyncio_task(investigator.main_loop, rt, *args, **kwargs)

        # is input_dtype TRUTHY? That doesn't make sense for investigators!

    def _internal_add_investigator(self, ant: _AnnotatedComponent) -> None:
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
        rt = RuntimeAPI(ant_comp, self._internal_agent_inference)
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

    # add a barrier
    def add_barrier(self, barrier: Barrier) -> None:
        # a barrier spans across multiple dtypes.... add in the order that
        # follows.
        for dtype in barrier.dtypes:
            self.barriers[dtype].append(barrier)
            if len(self.barriers[dtype]) > 1:
                # NOT the first barrier
                self._to_asyncio_task(
                    self._barrier_consumer, dtype, self.barriers[dtype][-2], barrier
                )

        barrier.start()

        # I need a consumer per dtype per barrier.

    async def _barrier_consumer(
        self, dtype, get_barrier: Barrier, put_barrier: Barrier
    ) -> None:
        while True:
            val = await get_barrier.get(dtype)
            logger.info(
                f"Barrier receive from {get_barrier}:{dtype}. Put to {put_barrier}"
            )
            await put_barrier.put(val)

    # add a data join
    def add_data_join(self, join_dtype: JoinDataType):

        # A data join waits until all items have arrived in input streams
        # (a hard barrier + join)

        if join_dtype in self.join_components:
            raise ValueError("Data join already exists for that type")

        cmp = _JoinComponent(join_dtype, self._put_to_dtype_queue)
        self.join_components[join_dtype] = cmp

        # add to component registry
        for dtype in join_dtype.dtypes:
            # component handles its own output
            ant_comp = _AnnotatedComponent(cmp, dtype, join_dtype)
            self.components[dtype].append(ant_comp)

        # start main loop
        self._to_asyncio_task(self.join_components[join_dtype].main_loop)

    # add a data split task - just about the same as a utility task

    def add_data_split_task(
        self, task: SplitTask, input_dtype: DataType, output_dtypes: tuple[DataType]
    ) -> None:
        assert input_dtype != TRUTHY

        # ensure tuple
        output_dtypes = tuple(output_dtypes)  # type: ignore

        # output will be handled separately....
        ant_comp = _AnnotatedComponent(task, input_dtype, NULL_DTYPE, False)
        ant_comp.split_outputs = output_dtypes

        # Add component to edge dict
        self.components[input_dtype].append(ant_comp)

    async def _run_component(
        self, ant: _AnnotatedComponent, in_data: TypedData, skip_queue_out: bool = False
    ):
        # wait until start
        await self.is_start.wait()
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
                rt = RuntimeAPI(ant, self._internal_agent_inference)
                logger.info(f"Run {type(ant.component).__name__} main loop")
                self._to_asyncio_task(ant.component.main_loop, rt, in_data)
                return

            rt = RuntimeAPI(ant, self._internal_agent_inference)
            logger.info(f"Run {type(ant.component).__name__} main loop")
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
    async def _dtype_consumer(self, input_data: TypedData) -> None:
        # Typed data incoming. Run the tasks concurrently, but block until they
        # are all done (except persistent)

        tasks = []
        for task in self.components[input_data.dtype]:
            # run normal
            tasks.append(self._run_component(task, input_data))

        await asyncio.gather(*tasks)

    def _put_to_dtype_queue(self, t_data: TypedData) -> None:
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

    async def _launch_b_consumer(
        self, dtype: DataType, creation: asyncio.Event
    ) -> None:
        await creation.wait()
        while True:
            t_data = await self.barriers[dtype][-1].get(dtype)
            # process!
            logger.info(
                f"Final dequeue from barrier ({self.barriers[dtype][-1]}): {t_data.dtype}"
            )
            await self._dtype_consumer(t_data)

    async def _launch_consumer(self, dtype: DataType) -> None:
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

    def print_graph(self):
        print()
        print("=" * 30)
        out = "=" * 30 + "\n"
        print("Digital Twin Flow:     IN <DTYPE> | OUT <DTYPE> (persist)")
        out += "Digital Twin Flow:     IN <DTYPE> | OUT <DTYPE> (persist)\n"

        # start with TRUTHY
        print("IN: (TRUTHY)")
        out += "IN: (TRUTHY)\n"

        for ant in self.truthy_list:
            print(
                f"\t{ant.output_dtype}: {type(ant.component).__name__} ({ant.is_persistent})"
            )
            out += f"\t{ant.output_dtype}: {type(ant.component).__name__} ({ant.is_persistent})\n"

        # rest of tasks
        for input_dtype in self.components:
            if input_dtype == TRUTHY:
                continue
            print(f"IN: {input_dtype}")
            out += f"IN: {input_dtype}\n"
            for ant in self.components[input_dtype]:
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
        for dtype in self.barriers:
            print(f"\t {dtype} -|-> ", end="")
            out += f"\t {dtype} -|-> "
            for barrier in self.barriers[dtype]:
                is_hard = barrier.dtypes[dtype]
                print(f"{barrier.name}{'' if is_hard else ']W'} -|-> ", end="")
                out += f"{barrier.name}{'' if is_hard else ']W'} -|-> "
            print()
            out += "\n"

        print("=" * 30)
        out += "=" * 30
        print()
        return out
