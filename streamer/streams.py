# -*- coding: utf-8 -*-

"""
streamer.streams
---

The module contains many stream generators.
"""
from typing import Iterable, TypeVar, Callable, Tuple, Collection
from .stream import Stream
from .operator import Cartesian, ConstantOf, RepeatApply


T = TypeVar("T")


def constant_of(value: T, times: int) -> Stream[T]:
    return Stream(ConstantOf(value, times))


def iterate(seed: T, operator: Callable) -> Stream:
    return Stream(RepeatApply(seed, operator))


def generate(gen_func: Callable[[], T]) -> Stream[T]:
    return Stream(RepeatApply(None, lambda _: gen_func())).skip(1)


def cartesian_product_stream(*streams: Iterable) -> Stream[Tuple]:
    return Stream(Cartesian(*streams))


def cartesian_power(power: int, collection: Collection) -> Stream[Tuple]:
    if power <= 0:
        raise ValueError("Cartesian power number should be at least 1")
    return Stream(Cartesian(*(iter(collection) for _ in range(power))))
