# -*- coding: utf-8 -*-

"""
streamer.operator
---

The module contains some iterator operator - add operation to an iterator and keep its laziness.
"""

from typing import Generic, TypeVar, Iterator

T = TypeVar('T')


class Deduplicator(Generic[T]):
    def __init__(self, stream: Iterator[T]):
        self.__appeared = set()
        self.__stream = stream

    def __next__(self) -> T:
        item = next(self.__stream)
        while item in self.__appeared:
            item = next(self.__stream)
        self.__appeared.add(item)
        return item

    def __iter__(self) -> Iterator[T]:
        return self
