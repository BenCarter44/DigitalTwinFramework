from abc import ABC, abstractmethod

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

from radical.asyncflow import WorkflowEngine

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import DTRuntime


@dataclass
class DataType:
    name: str = "None"

    # write fields of the data type here
    def __hash__(self):
        return hash(self.name)

    def __eq__(self, obj):
        return isinstance(obj, DataType) and obj.name == self.name

    def __str__(self):
        return self.name


TRUTHY = DataType("TRUE")
NULL_DTYPE = DataType("NULL")


@dataclass
class TypedData:
    dtype: DataType
    data: Any


# emitted by barrier


@dataclass
class WindowDataType(DataType):
    dtype: DataType = NULL_DTYPE

    def __init__(self, dtype: DataType, name: str):
        super().__init__(name=f"W[{dtype} by B-{name}]")
        self.dtype = dtype

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, obj):
        return (
            isinstance(obj, WindowDataType)
            and obj.dtype == obj.dtype
            and obj.name == self.name
        )

    def __str__(self):
        return super().__str__()


@dataclass
class WindowedTypeData(TypedData):
    # FIFO... oldest first, newest last
    sequence: list[Any]

    def __init__(self, dtype: WindowDataType, sequence: list[Any]):
        super().__init__(dtype=dtype, data=sequence)
        self.sequence = sequence


class _TwinComponent:
    # A twin component handles the things in common between Twin Agents
    def __init__(self):
        pass

    async def main_loop(self, runtime, *args, **kwargs) -> TypedData | None:
        pass


class ModelInvestigator(_TwinComponent):
    def __init__(self, flow: WorkflowEngine):
        super().__init__()
        self.flow = flow
        # for use by SciAgent
        self.runtime_id: Optional[int] = None

    def agent_feedback(self, *args, **kwargs):
        pass

    def get_id(self):
        return self.runtime_id

    # callbacks
    # async def my_callback(self, in_data: TypedData):
    #     pass

    # # inference task signature:
    # async def inference_task(in_data: TypedData, **model_kwargs) --> TypedData:
    #    pass

    def __eq__(self, obj):
        if isinstance(obj, ModelInvestigator):
            return self.runtime_id == obj.runtime_id
        else:
            return False

    # inference tasks also receive typed data and must return typed data


class UtilityTask(_TwinComponent):
    def __init__(self, flow: WorkflowEngine):
        super().__init__()
        self.flow = flow

    async def main_loop(
        self, runtime, in_data: TypedData, *args, **kwargs
    ) -> TypedData | None:
        pass


class SplitTask(UtilityTask):
    def __init__(self, input_dtype: DataType, output_dtypes: list[DataType]):
        pass

    async def main_loop(self, runtime, in_data: TypedData):
        return TypedData(DataType("a"), 1), TypedData(DataType("a"), 2)


class SciAgent(_TwinComponent):
    def __init__(self, flow: WorkflowEngine):
        super().__init__()
        self.flow = flow

        self.investigators: dict[int, ModelInvestigator] = {}
        self._investigator_counter = -1

    def _generate_runtime_id(self):
        self._investigator_counter += 1
        return self._investigator_counter

    # # model selector signature:
    # async def model_select_task(in_data: TypedData, *model_select_args, **model_select_kwargs):
    #    return investigator_id, model_args
    #
    #    Model args returned are passed to the investigator inference task's
    #    model_args
    #
    #    IF returned model_args is NONE or missing, use latest model published by investigator

    async def model_publish_cb(
        self, investigator: ModelInvestigator, model_args: dict, acc_metrics: dict
    ):
        pass

    async def main_loop(self, runtime):
        pass


class Barrier:
    def __init__(self, name: str, hard=True):
        self.is_hard_barrier = hard
        self.name = name

        self.output_queues: dict[DataType, asyncio.Queue] = {}

        self.global_version = 0

        self.dtypes: dict[DataType, bool] = {}
        self.previous: dict[DataType, list[Any]] = defaultdict(list)
        self.previous_retain: dict[DataType, bool] = {}

        # default: -1
        self.version_numbers: dict[DataType, int] = {}

        self.condition = asyncio.Condition()
        self._update = asyncio.Semaphore(0)

        self.count_hard = 0
        self.count_soft = 0

        self.set_soft = False

    def __str__(self):
        return self.name

    def add_dtype(self, dtype: DataType, hard: Optional[bool] = None):
        # if soft, emits a sequence data type!
        if hard is None:
            hard = self.is_hard_barrier

        self.dtypes[dtype] = hard
        self.version_numbers[dtype] = self.global_version - 1
        self.output_queues[dtype] = asyncio.Queue()
        self.count_hard += 1 if hard else 0
        self.count_soft += 0 if hard else 1

        if hard:
            return dtype
        else:
            return WindowDataType(dtype, self.name)

    async def start(self):
        self.loop_task = asyncio.create_task(self._loop())
        self.loop_task.add_done_callback(lambda r: r.result())

    async def put(self, in_data: TypedData):
        dtype = in_data.dtype
        if not (self.dtypes[dtype]):
            # soft. just store the result
            if self.previous_retain.get(dtype, True):
                self.previous[dtype] = [in_data.data]
                self.previous_retain[dtype] = False
            else:
                self.previous[dtype].append(in_data.data)
            if not (self.set_soft):
                self.set_soft = True
                for i in range(self.count_soft):
                    self._update.release()
            return

        def predicate():
            return self.version_numbers[dtype] < self.global_version

        # not ok to increment. WAIT for self.version_numbers[dtype] < self.global_version
        async with self.condition:
            while True:
                await self.condition.wait_for(predicate)
                if self.version_numbers[dtype] < self.global_version:
                    # OK to increment
                    self.version_numbers[dtype] += 1

                    if dtype not in self.output_queues:
                        self.output_queues[dtype] = asyncio.Queue()

                    # is hard
                    # V() on update
                    self._update.release()
                    self.output_queues[dtype].put_nowait(in_data)
                    return

    async def get(self, dtype: DataType, wait=True):
        if dtype not in self.output_queues:
            raise ValueError("Unrecognized datatype for barrier")
        if wait:
            return await self.output_queues[dtype].get()
        else:
            return self.output_queues[dtype].get_nowait()

    async def _loop(self):
        # wait for there to be at least one task
        while self.count_soft + self.count_hard == 0:
            await asyncio.sleep(0.01)
        while True:
            await self.condition.acquire()
            self.condition.notify_all()
            self.condition.release()

            for i in range(self.count_hard + self.count_soft):
                await self._update.acquire()

            self.set_soft = False
            for dtype in self.dtypes:
                if self.dtypes[dtype]:
                    continue
                # emit on any soft barriers

                # drain previous in reverse append order
                self.output_queues[dtype].put_nowait(
                    WindowedTypeData(
                        WindowDataType(dtype, self.name), self.previous[dtype]
                    )
                )
                self.previous[dtype] = [self.previous[dtype][-1]]
                self.previous_retain[dtype] = True

            # all updates have been sent. Increment global version
            self.global_version += 1


if __name__ == "__main__":
    # Barrier test

    apple = DataType("apple")
    orange = DataType("orange")
    pear = DataType("pear")

    b = Barrier("barrier1")

    async def apple_producer():
        counter = 0
        while True:
            print(f"Produce apple: {counter}")
            await b.put(TypedData(apple, counter))
            counter += 1
            await asyncio.sleep(1)

    async def orange_producer():
        counter = 0
        while True:
            print(f"Produce orange: {counter}")
            await b.put(TypedData(orange, counter))
            counter += 1
            await asyncio.sleep(2)

    async def pear_producer():
        counter = 0
        while True:
            print(f"Produce pear: {counter}")
            await b.put(TypedData(pear, counter))
            counter += 1
            await asyncio.sleep(5)

    async def apple_consumer():
        while True:
            out = await b.get(apple)
            print(f"Consume apple: {out.data}")

    async def orange_consumer():
        while True:
            out = await b.get(orange)
            print(f"Consume orange: {out.data}")

    async def pear_consumer():
        while True:
            out = await b.get(pear)
            print(f"Consume pear: {out.data}")

    async def main():

        b.add_dtype(apple)
        b.add_dtype(orange)
        b.add_dtype(pear, hard=True)

        await b.start()

        t1 = asyncio.create_task(apple_producer())
        t2 = asyncio.create_task(orange_producer())
        t3 = asyncio.create_task(pear_producer())

        # consumer

        t1 = asyncio.create_task(apple_consumer())
        t2 = asyncio.create_task(orange_consumer())
        t3 = asyncio.create_task(pear_consumer())

        await asyncio.sleep(30)

    asyncio.run(main())
