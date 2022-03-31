import platform
from functools import partial
from time import sleep

import pytest
from redis.client import StrictRedis

from redis_throttled_queue import Resolution
from redis_throttled_queue import ThrottledQueue

pytest_plugins = ('pytester',)

skipifpypy = partial(pytest.mark.skipif(platform.python_implementation() == 'PyPy'))


def test_simple(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, "test", limit=5, resolution=Resolution.SECOND)
    for pos, item in enumerate(range(10)):
        queue.push('aaaaaa', f'a{item}', priority=10 - pos)
    for pos, item in enumerate(range(10)):
        queue.push('bbbbbb', f'b{item}', priority=10 - pos)

    assert len(queue) == 20
    items = ','.join(queue.pop() for _ in range(10))
    assert items == 'a0,b0,a1,b1,a2,b2,a3,b3,a4,b4'
    assert len(queue) == 10
    assert queue.pop() is None
    assert len(queue) == 10

    sleep(1)

    items = ','.join(queue.pop() for _ in range(10))
    assert items == 'a5,b5,a6,b6,a7,b7,a8,b8,a9,b9'
    assert queue.pop() is None
    assert len(queue) == 0


def test_priority(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, "test", limit=5, resolution=Resolution.SECOND)
    for pos, item in enumerate(range(10)):
        queue.push('aaaaaa', f'a{item}', priority=10 - pos)
    for pos, item in enumerate(range(10)):
        queue.push('bbbbbb', f'b{item}', priority=pos)

    assert len(queue) == 20
    items = ','.join(queue.pop() for _ in range(10))
    assert items == 'a0,b9,a1,b8,a2,b7,a3,b6,a4,b5'
    assert len(queue) == 10
    assert queue.pop() is None
    assert len(queue) == 10

    sleep(1)

    items = ','.join(queue.pop() for _ in range(10))
    assert items == 'a5,b4,a6,b3,a7,b2,a8,b1,a9,b0'
    assert queue.pop() is None
    assert len(queue) == 0


def test_extras(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, "test", limit=5, resolution=Resolution.SECOND)
    for pos, item in enumerate(range(10)):
        queue.push('aaaaaa', f'a{item}', priority=10 - pos)
    for pos, item in enumerate(range(10)):
        queue.push('bbbbbb', f'b{item}', priority=10 - pos)

    assert len(queue) == 20
    assert queue.pop() == 'a0'
    assert len(queue) == 19
    queue.push('cccccc', 'c0', priority=11)
    queue.push('cccccc', 'c1', priority=-1)
    queue.push('aaaaaa', 'aX', priority=11)
    queue.push('aaaaaa', 'aY', priority=-1)
    assert len(queue) == 23

    items = ','.join(str(queue.pop()) for _ in range(13))
    assert len(queue) == 12
    assert items in ['b0,aX,b1,a1,b2,a2,b3,a3,b4,c0,c1,None,None']

    sleep(1)

    items = ','.join(str(queue.pop()) for _ in range(12))
    assert len(queue) == 2
    assert items in ['a4,b5,a5,b6,a6,b7,a7,b8,a8,b9,None,None']

    sleep(1)

    items = ','.join(str(queue.pop()) for _ in range(4))
    assert len(queue) == 0
    assert items in ['a9,aY,None,None']
