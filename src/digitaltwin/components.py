from abc import ABC, abstractmethod

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

from radical.asyncflow import WorkflowEngine  # type: ignore


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

    # inference tasks also receive typed data and must return typed data


class UtilityTask(_TwinComponent):
    def __init__(self, flow: WorkflowEngine):
        super().__init__()
        self.flow = flow

    async def main_loop(
        self, runtime, in_data: TypedData, *args, **kwargs
    ) -> TypedData | None:
        pass
