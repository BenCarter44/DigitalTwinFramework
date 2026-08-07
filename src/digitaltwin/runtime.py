from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable, cast

from .components import *
from .components import _TwinComponent
from .streaming import *

from radical.asyncflow import WorkflowEngine  # type: ignore
import logging

logger = logging.getLogger(__name__)


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


class RuntimeAPI(ABC):
    ON_INPUT = "runtime/ON_INPUT"
    ON_OUTPUT = "runtime/ON_OUTPUT"
    ON_MODEL_PUBLISH = "runtime/ON_PUBLISH"
    ON_FILTERED_INPUT = "runtime/ON_FILTER_INPUT"
    ON_FILTERED_OUTPUT = "runtime/ON_FILTER_OUTPUT"

    def __init__(self, ant: _AnnotatedComponent, agent_inf: Callable):
        self._ant = ant
        self._internal_add_investigator: Optional[Callable] = None
        self._internal_agent_inference: Callable = agent_inf
        self._background_tasks: set[asyncio.Task] = set()

    def subscribe_to_topic(self, topic: str, task: Callable):
        self._ant.subscriptions[topic].append(task)

    def publish_new_model(self, model_kwargs={}, acc_kwargs={}):
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
            def done(r):
                r.result() # for error propagation
                self._background_tasks.discard(r)
            bk.add_done_callback(done)

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
        return await self._internal_agent_inference(input_d, output_dtype)


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

        self.truthy_list: list[_AnnotatedComponent] = []

        self.running_tasks: set[asyncio.Task] = set()

        self.is_start = asyncio.Event()

        @flow.block
        async def to_block(func, *args, **kwargs):
            await func(*args, **kwargs)

        self._to_block = to_block

    def start(self):
        self.is_start.set()

    async def _call_await(self, func, *args, **kwargs):
        await func(*args, **kwargs)

    def _to_asyncio_task(self, func, *args, **kwargs):
        result = asyncio.create_task(func(*args, **kwargs))
        self.running_tasks.add(result)
        def done(r):
            r.result() # for error propagation
            self.running_tasks.discard(r)
        result.add_done_callback(done)

    def add_task(
        self,
        task: UtilityTask,
        input_dtype: DataType,
        output_dtype: DataType,
        is_persistent=False,
    ):

        ant_comp = _AnnotatedComponent(task, input_dtype, output_dtype, is_persistent)

        # Add component to edge dict
        self.components[input_dtype].append(ant_comp)

        # is input_dtype TRUTHY? If so, run it!
        if input_dtype == TRUTHY:
            logger.debug("Added task with input of TRUTHY... Running.")
            true_data = TypedData(TRUTHY, True)
            # call as a block so it recieves Ctrl-C
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

    def _internal_add_investigator(self, ant: _AnnotatedComponent):
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
                answer = self._run_component(component, in_data, skip_queue_out=True)
                if answer is None:
                    return TypedData(NULL_DTYPE, None)
                return answer

    # add a split task
    async def add_split_task(
        self, task: SplitTask, input_dtype: DataType, output_dtypes: list[DataType]
    ):
        pass

    # add a barrier
    async def add_barrier(self, barrier: Barrier):
        pass


    # add a data join
    async def add_data_join(self, *dtypes: DataType) -> DataType:
        pass

    async def _run_component(
        self, ant: _AnnotatedComponent, in_data: TypedData, skip_queue_out=False
    ):
        # wait until start
        await self.is_start.wait()
        logger.info(
            f"Online run: {type(ant.component).__name__}. In: {in_data.dtype}:{in_data.data}"
        )

        assert ant.input_dtype == TRUTHY or ant.input_dtype == in_data.dtype

        for cb in ant.subscriptions[RuntimeAPI.ON_INPUT]:
            logger.info(f"Fire ON_INPUT on {cb} In: {in_data.dtype}:{in_data.data}")
            self._to_asyncio_task(self._call_await, cb, in_data)
        # and child investigators
        for i_id, investigator in ant.investigators.items():
            for cb in investigator.subscriptions[RuntimeAPI.ON_INPUT]:
                logger.info(f"Fire ON_INPUT on {cb} In: {in_data.dtype}:{in_data.data}")
                self._to_asyncio_task(self._call_await, cb, in_data)

        # run the main loop directly
        if isinstance(ant.component, UtilityTask):

            if ant.is_persistent:
                # is persistent, so subscribe to its output
                if ant.output_dtype in self.components:
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
                    f"Model Investigator {ant.component} did not return the correct dtype. Expected: {ant.output_dtype}"
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
                logger.info(
                    f"Fire ON_FILTERED_INPUT on {cb} In: {in_data.dtype}:{in_data.data}"
                )
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

    async def _launch_consumer(self, dtype: DataType):
        while True:
            t_data = await self.dtype_queues[dtype].get()
            logger.info(f"Dequeue: {t_data.dtype}")
            await self._dtype_consumer(t_data)

    def print_graph(self):
        print("Digital Twin Flow: ")

        # start with TRUTHY
        print("IN: (TRUTHY)")
        for ant in self.truthy_list:
            print(f"\t{ant.output_dtype}: {ant.component} ({ant.is_persistent})")

        # rest of tasks
        for input_dtype in self.components:
            if input_dtype == TRUTHY:
                continue
            print(f"IN: {input_dtype}")
            for ant in self.components[input_dtype]:
                print(f"\t{ant.output_dtype}: {ant.component}  ({ant.is_persistent})")
