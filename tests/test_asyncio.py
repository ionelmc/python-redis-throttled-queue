import asyncio
from time import sleep
from types import SimpleNamespace

import pytest
from redis.asyncio import StrictRedis

from redis_throttled_queue import AsyncThrottledQueue as ThrottledQueue
from redis_throttled_queue import Resolution

pytest_plugins = ('pytester',)


@pytest.fixture
async def redis_conn(redis_server):
    """
    Returns the async variant of the redis client.
    """
    ThrottledQueue._library_missing = True
    async with StrictRedis(unix_socket_path=redis_server, decode_responses=True) as conn:
        await ThrottledQueue.register_library(conn)
        yield conn


async def get_ttl(redis_conn):
    return {':'.join(key.split(':')[:2]): (await redis_conn.ttl(key)) for key in await redis_conn.keys('*:usage:*')}


async def test_simple(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for item in range(10):
        await queue.push('aaaaaa', f'a{item}', priority=10 - item)
    for item in range(10):
        await queue.push('bbbbbb', f'b{item}', priority=10 - item)

    assert await queue.size() == 20
    items = ','.join([(await queue.pop()) for _ in range(10)])
    assert items == 'a0,b0,a1,b1,a2,b2,a3,b3,a4,b4'
    assert await queue.size() == 10
    assert await get_ttl(redis_conn) == {'test:usage': 1}
    assert await queue.pop() is None
    assert await queue.size() == 10

    await asyncio.sleep(1)

    items = ','.join([(await queue.pop()) for _ in range(10)])
    assert items == 'a5,b5,a6,b6,a7,b7,a8,b8,a9,b9'
    assert await queue.pop() is None
    assert await queue.size() == 0


async def test_usage_expiry(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=2, resolution=Resolution.SECOND)
    for i in range(10):
        await queue.push('name', f'foo{i}')

    assert await queue.size() == 10
    assert await queue.pop() == 'foo9'
    assert await queue.pop() == 'foo8'
    assert await get_ttl(redis_conn) == {'test:usage': 1}
    assert await queue.pop() is None
    assert await get_ttl(redis_conn) == {'test:usage': 1}

    await asyncio.sleep(1)

    assert await get_ttl(redis_conn) == {}
    assert await queue.pop() == 'foo7'
    assert await queue.pop() == 'foo6'
    assert await queue.pop() is None
    assert await get_ttl(redis_conn) == {'test:usage': 1}

    await asyncio.sleep(1)

    assert await get_ttl(redis_conn) == {}


async def test_dupes(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for _ in range(3):
        for item in range(10):
            await queue.push('aaaaaa', f'a{item}', priority=10 - item)
        for item in range(10):
            await queue.push('bbbbbb', f'b{item}', priority=10 - item)

    assert await queue.size() == 20
    items = ','.join([(await queue.pop()) for _ in range(10)])
    assert items == 'a0,b0,a1,b1,a2,b2,a3,b3,a4,b4'
    assert await queue.size() == 10
    assert await get_ttl(redis_conn) == {'test:usage': 1}
    assert await queue.pop() is None
    assert await queue.size() == 10

    sleep(1)

    items = ','.join([(await queue.pop()) for _ in range(10)])
    assert items == 'a5,b5,a6,b6,a7,b7,a8,b8,a9,b9'
    assert await queue.pop() is None
    assert await queue.size() == 0


async def test_cleanup(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for item in range(10):
        await queue.push('aaaaaa', f'a{item}', priority=10 - item)
    for item in range(10):
        await queue.push('bbbbbb', f'b{item}', priority=10 - item)

    assert await queue.size() == 20
    items = ','.join([(await queue.pop()) for _ in range(10)])
    assert items == 'a0,b0,a1,b1,a2,b2,a3,b3,a4,b4'
    assert await queue.size() == 10
    assert await queue.pop() is None
    assert await queue.size() == 10
    assert await get_ttl(redis_conn) == {'test:usage': 1}

    await asyncio.sleep(1)

    await queue.cleanup()
    assert await queue.size() == 0
    assert await queue.pop() is None
    assert await get_ttl(redis_conn) == {}


async def test_cleanup_directly(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for item in range(10):
        await queue.push('aaaaaa', f'a{item}', priority=10 - item)
    for item in range(10):
        await queue.push('bbbbbb', f'b{item}', priority=10 - item)

    assert await queue.size() == 20
    await queue.cleanup()
    assert await queue.size() == 0
    assert await queue.pop() is None


async def test_cleanup_nothing(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    assert await queue.size() == 0
    await queue.cleanup()
    assert await queue.size() == 0
    await queue.cleanup()
    assert await queue.pop() is None


async def test_priority(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for item in range(10):
        await queue.push('aaaaaa', f'a{item}', priority=10 - item)
    for item in range(10):
        await queue.push('bbbbbb', f'b{item}', priority=item)

    assert await queue.size() == 20
    items = ','.join([await (queue.pop()) for _ in range(10)])
    assert items == 'a0,b9,a1,b8,a2,b7,a3,b6,a4,b5'
    assert await queue.size() == 10
    assert await queue.pop() is None
    assert await queue.size() == 10
    assert await get_ttl(redis_conn) == {'test:usage': 1}

    await asyncio.sleep(1)
    assert queue.idle_seconds == pytest.approx(1, 0.05)

    items = ','.join([(await queue.pop()) for _ in range(10)])
    assert items == 'a5,b4,a6,b3,a7,b2,a8,b1,a9,b0'
    assert await queue.pop() is None
    assert await queue.size() == 0
    assert await get_ttl(redis_conn) == {'test:usage': 1}


async def test_window(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=1, resolution=Resolution.SECOND)
    for item in range(10):
        await queue.push('A', f'a{item}', priority=item)

    assert await queue.size() == 10
    assert await queue.pop('X') == 'a9'
    assert await get_ttl(redis_conn) == {'test:usage': 1}
    assert await queue.size() == 9
    assert await queue.pop('X') is None
    assert await queue.size() == 9
    assert await queue.pop('Y') == 'a8'
    assert await queue.size() == 8
    assert await queue.pop('Y') is None
    assert await queue.size() == 8
    await asyncio.sleep(1)
    assert await queue.pop('X') == 'a7'
    assert await queue.size() == 7
    assert await queue.pop('X') is None
    assert await queue.size() == 7
    assert await queue.pop('Y') == 'a6'
    assert await queue.size() == 6
    assert await queue.pop('Y') is None
    assert await queue.size() == 6


async def test_extras(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for item in range(10):
        await queue.push('aaaaaa', f'a{item}', priority=10 - item)
    for item in range(10):
        await queue.push('bbbbbb', f'b{item}', priority=10 - item)

    assert await queue.size() == 20
    assert await queue.pop() == 'a0'
    assert await get_ttl(redis_conn) == {'test:usage': 1}
    assert await queue.size() == 19
    await queue.push('cccccc', 'c0', priority=11)
    await queue.push('cccccc', 'c1', priority=-1)
    await queue.push('aaaaaa', 'aX', priority=11)
    await queue.push('aaaaaa', 'aY', priority=-1)
    assert await queue.size() == 23

    items = ','.join([str(await queue.pop()) for _ in range(13)])
    assert await queue.size() == 14
    assert items in ['b0,aX,b1,a1,b2,a2,b3,a3,b4,None,None,None,None']

    await asyncio.sleep(1)
    assert queue.idle_seconds == pytest.approx(1, 0.05)

    items = [bool(await queue.pop()) for _ in range(12)]
    assert await queue.size() == 2
    assert items == [True] * 12

    await asyncio.sleep(1)
    assert queue.idle_seconds == pytest.approx(1, 0.05)

    items = [bool(await queue.pop()) for _ in range(4)]
    assert await queue.size() == 0
    assert items == [True, True, False, False]


async def test_validation():
    conn = SimpleNamespace(info=lambda: {'redis_version': '10'}, register_script=lambda _: None)
    pytest.raises(TypeError, ThrottledQueue, conn, b'caca')
    pytest.raises(TypeError, ThrottledQueue, conn, 123)
    with pytest.raises(ValueError):
        await ThrottledQueue(conn, 'foo').push(':', None)
