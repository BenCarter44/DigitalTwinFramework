# basic LRU cache. Python's functools doesn't allow for inspection
# asyncio locks.

from collections import OrderedDict
import asyncio
from typing import Any


class LRUCache:
    def __init__(self, size: int = 128) -> None:
        self.cache: OrderedDict[Any, Any] = OrderedDict()
        self.edit_lock = asyncio.Lock()
        self.max_size = size

    async def put_item(self, key, value) -> None:

        async with self.edit_lock:
            if key in self.cache:
                self.cache[key] = value
                self.cache.move_to_end(key)
                return

            # add key to value
            self.cache[key] = value

            # is size over max?
            if len(self.cache) > self.max_size:
                self.cache.popitem(last=False)

    async def fetch_item(self, key):
        # fetch
        if key not in self.cache:
            raise KeyError

        async with self.edit_lock:
            self.cache.move_to_end(key)
            return self.cache[key]

    def exists(self, key) -> bool:
        return key in self.cache
