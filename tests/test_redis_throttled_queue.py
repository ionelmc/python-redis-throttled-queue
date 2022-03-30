import platform
from functools import partial
from time import sleep

import pytest
from redis.client import StrictRedis

from redis_throttled_queue import ThrottledQueue, Resolution

pytest_plugins = ('pytester',)

skipifpypy = partial(pytest.mark.skipif(platform.python_implementation() == 'PyPy'))


def test_simple(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, "test", limit=5, resolution=Resolution.SECOND)
    for pos, item in enumerate(range(10)):
        queue.push('aaaaaa', f'a{item}', priority=10 - pos)
    for pos, item in enumerate(range(10)):
        queue.push('bbbbbb', f'b{item}', priority=10 - pos)

    for key in redis_conn.scan_iter('test:queue:*', 1):
        if key == 'test:queue:aaaaaa':
            first, second = 'ab'
        elif key == 'test:queue:bbbbbb':
            first, second = 'ba'
        else:
            raise AssertionError(key)
        break

    assert queue.pop() == f'{first}0'
    assert queue.pop() == f'{first}1'
    assert queue.pop() == f'{first}2'
    assert queue.pop() == f'{first}3'
    assert queue.pop() == f'{first}4'

    assert queue.pop() == f'{second}0'
    assert queue.pop() == f'{second}1'
    assert queue.pop() == f'{second}2'
    assert queue.pop() == f'{second}3'
    assert queue.pop() == f'{second}4'

    sleep(1)

    assert queue.pop() == f'{first}5'
    assert queue.pop() == f'{first}6'
    assert queue.pop() == f'{first}7'
    assert queue.pop() == f'{first}8'
    assert queue.pop() == f'{first}9'

    assert queue.pop() == f'{second}5'
    assert queue.pop() == f'{second}6'
    assert queue.pop() == f'{second}7'
    assert queue.pop() == f'{second}8'
    assert queue.pop() == f'{second}9'

    assert queue.pop() is None


def test_extras(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, "test", limit=5, resolution=Resolution.SECOND)
    for pos, item in enumerate(range(10)):
        queue.push('aaaaaa', f'a{item}', priority=10 - pos)
    for pos, item in enumerate(range(10)):
        queue.push('bbbbbb', f'b{item}', priority=10 - pos)

    for key in redis_conn.scan_iter('test:queue:*', 1):
        if key == 'test:queue:aaaaaa':
            first, second = 'ab'
        elif key == 'test:queue:bbbbbb':
            first, second = 'ba'
        else:
            raise AssertionError(key)
        break

    assert queue.pop() == f'{first}0'
    queue.push(first * 6, 'X', priority=11)
    queue.push(first * 6, 'Y', priority=-1)
    assert queue.pop() == f'X'
    assert queue.pop() == f'{first}1'
    assert queue.pop() == f'{first}2'
    assert queue.pop() == f'{first}3'

    assert queue.pop() == f'{second}0'
    assert queue.pop() == f'{second}1'
    assert queue.pop() == f'{second}2'
    assert queue.pop() == f'{second}3'
    assert queue.pop() == f'{second}4'

    sleep(1)

    assert queue.pop() == f'{first}4'
    assert queue.pop() == f'{first}5'
    assert queue.pop() == f'{first}6'
    assert queue.pop() == f'{first}7'
    assert queue.pop() == f'{first}8'

    assert queue.pop() == f'{second}5'
    assert queue.pop() == f'{second}6'
    assert queue.pop() == f'{second}7'
    assert queue.pop() == f'{second}8'
    assert queue.pop() == f'{second}9'

    assert queue.pop() is None

    sleep(1)

    assert queue.pop() == f'{first}9'
    assert queue.pop() == f'Y'
    assert queue.pop() is None

