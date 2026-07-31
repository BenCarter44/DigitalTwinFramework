from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable

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
    model_args: tuple = tuple()
    inference_task: Optional[Callable] = None


class RuntimeAPI(ABC):
    ON_INPUT = "runtime/ON_INPUT"
    ON_OUTPUT = "runtime/ON_OUTPUT"

    def __init__(self, ant: _AnnotatedComponent):
        self._ant = ant

    def subscribe_to_topic(self, topic: str, task: Callable):
        self._ant.subscriptions[topic].append(task)

    def publish_new_model(self, *args, **kwargs):
        self._ant.model_args = args
        self._ant.model_kwargs = kwargs

    def set_inference_task(self, task: Callable):
        self._ant.inference_task = task


class DTRuntime:
    """Workflow builder / dynamic manager.

    Receives:   --- building workflow
        - Add agent requests                            add_agent()
            - Agent Host:
                - Launches Mainloop task
                - Executes callbacks
        - Add Utility / Persistent Tasks                add_task()
                - Launches Mainloop
                - Executes callbacks
    At runtime:
        - Receives updates to agent's tasks
        - agent can spawn their own tasks
        - PubSub triggers tasks to run
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

    def start(self):
        self.is_start.set()

    def _to_asyncio_task(self, func, *args, **kwargs):
        result = asyncio.create_task(func(*args, **kwargs))
        self.running_tasks.add(result)
        result.add_done_callback(self.running_tasks.discard)

    ## flow.block
    async def _to_block(self, func, *args, **kwargs):
        await func(*args, **kwargs)

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
        ant_comp = _AnnotatedComponent(investigator, input_dtype, output_dtype, False)

        # Add component to edge dict
        self.components[input_dtype].append(ant_comp)

        # start up its main loop
        rt = RuntimeAPI(ant_comp)
        self._to_asyncio_task(investigator.main_loop, rt, *args, **kwargs)

        # is input_dtype TRUTHY? That doesn't make sense for investigators!

    async def _run_component(self, ant: _AnnotatedComponent, in_data: TypedData):
        # wait until start
        await self.is_start.wait()

        assert ant.input_dtype == TRUTHY or ant.input_dtype == in_data.dtype

        for cb in ant.subscriptions[RuntimeAPI.ON_INPUT]:
            self._to_asyncio_task(self._to_block, cb, in_data)

        # run the main loop directly
        if isinstance(ant.component, UtilityTask):

            if ant.is_persistent and ant.output_dtype != NULL_DTYPE:
                # is persistent, so subscribe to its output
                if ant.output_dtype in self.components:
                    # has a task registered, but no queue yet.
                    if ant.output_dtype not in self.dtype_queues:
                        self.dtype_queues[ant.output_dtype] = asyncio.Queue()
                        self._to_asyncio_task(self._launch_consumer, ant.output_dtype)

                    logger.debug(f"Runtime subscribing to dtype: {ant.output_dtype}")
                    await self.streamer.subscribe_to_dtype(
                        ant.output_dtype, self.dtype_queues[ant.output_dtype]
                    )

            rt = RuntimeAPI(ant)
            logger.info("Trigger UtilityTask main loop")
            answer = await ant.component.main_loop(rt, in_data)

            if ant.is_persistent:
                return  # don't use its answer. everything comes from pubsub for persistent tasks

            if answer is None:
                # no downstream tasks. End
                return
            assert isinstance(answer, TypedData)

            if answer.dtype != ant.output_dtype:
                raise ValueError(
                    f"Utility Task {ant.component} did not return the correct dtype. Expected: {ant.output_dtype}"
                )

            if answer.dtype == NULL_DTYPE:
                return

            # don't wait as I want to ensure order and not yield
            self._put_to_dtype_queue(answer)

        # item is an investigator - run its inference
        else:
            assert isinstance(ant.component, ModelInvestigator)

            # wait until there is an inference task
            while ant.inference_task is None:
                await asyncio.sleep(0.01)

            logger.info("DT Runtime submit inference task ")
            answer = await ant.inference_task(
                in_data, *ant.model_args, **ant.model_kwargs
            )
            if answer is None:
                return
            if answer.dtype != ant.output_dtype:
                raise ValueError(
                    f"Model Investigator {ant.component} did not return the correct dtype. Expected: {ant.output_dtype}"
                )
            assert isinstance(answer, TypedData)

        for cb in ant.subscriptions[RuntimeAPI.ON_OUTPUT]:
            self._to_asyncio_task(self._to_block, cb, answer)

        self._put_to_dtype_queue(answer)

    ## flow.block
    async def _dtype_consumer(self, input_data: TypedData):
        # Typed data incoming. Run the tasks concurrently, but block until they
        # are all done (except persistent)

        tasks = []
        for task in self.components[input_data.dtype]:
            if task.is_persistent:
                self._to_asyncio_task(self._to_block, task, input_data)
                continue
            # run normal
            tasks.append(self._to_block(self._run_component, task, input_data))

        await asyncio.gather(*tasks)

    def _put_to_dtype_queue(self, t_data: TypedData):
        if t_data.dtype == NULL_DTYPE:
            return

        if t_data.dtype in self.dtype_queues:
            self.dtype_queues[t_data.dtype].put_nowait(t_data)
            return

        # if not in there, check if there are any associated tasks.
        # if not, drop the dtype
        if t_data.dtype not in self.components:
            return

        # has a task registered, but no queue yet.
        self.dtype_queues[t_data.dtype] = asyncio.Queue()

        # create consumer task
        self._to_asyncio_task(self._launch_consumer, t_data)

    async def _launch_consumer(self, dtype: DataType):
        while True:
            t_data = await self.dtype_queues[dtype].get()
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
            print(f"\t{ant.output_dtype}: {ant.component}  ({ant.is_persistent})")
