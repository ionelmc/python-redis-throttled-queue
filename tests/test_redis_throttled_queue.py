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

    items = ','.join(queue.pop() for _ in range(10))
    assert items in [
        'a0,b0,a1,b1,a2,b2,a3,b3,a4,b4',
        'b0,a0,b1,a1,b2,a2,b3,a3,b4,a4',
    ]
    assert queue.pop() is None

    sleep(1)

    items = ','.join(queue.pop() for _ in range(10))
    assert items in [
        'a5,b5,a6,b6,a7,b7,a8,b8,a9,b9',
        'b5,a5,b6,a6,b7,a7,b8,a8,b9,a9',
    ]
    assert queue.pop() is None


def test_extras(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, "test", limit=5, resolution=Resolution.SECOND)
    for pos, item in enumerate(range(10)):
        queue.push('aaaaaa', f'a{item}', priority=10 - pos)
    for pos, item in enumerate(range(10)):
        queue.push('bbbbbb', f'b{item}', priority=10 - pos)

    assert queue.pop() in ['a0', 'b0']
    queue.push('cccccc', 'c0', priority=11)
    queue.push('cccccc', 'c1', priority=-1)
    queue.push('aaaaaa', 'aX', priority=11)
    queue.push('aaaaaa', 'aY', priority=-1)
    items = ','.join(str(queue.pop()) for _ in range(13))

    assert items in ['b0,aX,b1,a1,b2,a2,b3,a3,b4,c0,c1,None,None']

    sleep(1)

    items = ','.join(str(queue.pop()) for _ in range(12))
    assert items in ['a4,b5,a5,b6,a6,b7,a7,b8,a8,b9,None,None']

    sleep(1)

    items = ','.join(str(queue.pop()) for _ in range(4))
    assert items in ['a9,aY,None,None']
