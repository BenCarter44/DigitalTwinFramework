from abc import ABC, abstractmethod

import asyncio
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
