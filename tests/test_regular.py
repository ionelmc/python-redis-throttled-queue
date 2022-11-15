from time import sleep
from types import SimpleNamespace

import freezegun
import pytest
from redis.client import StrictRedis

from redis_throttled_queue import Resolution
from redis_throttled_queue import ThrottledQueue

pytest_plugins = ('pytester',)


def get_ttl(redis_conn):
    return {':'.join(key.split(':')[:2]): ttl for key in redis_conn.keys('*:usage:*') for ttl in (redis_conn.ttl(key),) if ttl > 0}


def test_simple(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for item in range(10):
        queue.push('aaaaaa', f'a{item}', priority=10 - item)
    for item in range(10):
        queue.push('bbbbbb', f'b{item}', priority=10 - item)

    assert len(queue) == 20
    items = ','.join(queue.pop() for _ in range(10))
    assert items == 'a0,b0,a1,b1,a2,b2,a3,b3,a4,b4'
    assert len(queue) == 10
    assert get_ttl(redis_conn) == {'test:usage': 1}
    assert queue.pop() is None
    assert len(queue) == 10

    sleep(1)

    items = ','.join(queue.pop() for _ in range(10))
    assert items == 'a5,b5,a6,b6,a7,b7,a8,b8,a9,b9'
    assert queue.pop() is None
    assert len(queue) == 0


def test_usage_expiry(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=2, resolution=Resolution.SECOND)
    for i in range(10):
        queue.push('name', f'foo{i}')

    assert len(queue) == 10
    assert queue.pop() == 'foo9'
    assert queue.pop() == 'foo8'
    assert get_ttl(redis_conn) == {'test:usage': 1}
    assert queue.pop() is None
    assert get_ttl(redis_conn) == {'test:usage': 1}

    sleep(1)

    assert get_ttl(redis_conn) == {}
    assert queue.pop() == 'foo7'
    assert queue.pop() == 'foo6'
    assert queue.pop() is None
    assert get_ttl(redis_conn) == {'test:usage': 1}

    sleep(1)

    assert get_ttl(redis_conn) == {}


def test_dupes(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for _ in range(30):
        for item in range(10):
            queue.push('aaaaaa', f'a{item}', priority=11 - item)
        for item in range(10):
            queue.push('bbbbbb', f'b{item}', priority=10 - item)

    assert len(queue) == 20
    items = ','.join(queue.pop() for _ in range(10))
    assert items == 'a0,b0,a1,b1,a2,b2,a3,b3,a4,b4'
    assert len(queue) == 10
    assert get_ttl(redis_conn) == {'test:usage': 1}
    assert queue.pop() is None
    assert len(queue) == 10

    sleep(1)

    items = ','.join(queue.pop() for _ in range(10))
    assert items == 'a5,b5,a6,b6,a7,b7,a8,b8,a9,b9'
    assert queue.pop() is None
    assert len(queue) == 0


def test_cleanup(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for item in range(10):
        queue.push('aaaaaa', f'a{item}', priority=10 - item)
    for item in range(10):
        queue.push('bbbbbb', f'b{item}', priority=10 - item)

    assert len(queue) == 20
    items = ','.join(queue.pop() for _ in range(10))
    assert items == 'a0,b0,a1,b1,a2,b2,a3,b3,a4,b4'
    assert len(queue) == 10
    assert queue.pop() is None
    assert len(queue) == 10
    assert get_ttl(redis_conn) == {'test:usage': 1}

    sleep(1)

    queue.cleanup()
    assert len(queue) == 0
    assert queue.pop() is None
    assert get_ttl(redis_conn) == {}


def test_cleanup_directly(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for item in range(10):
        queue.push('aaaaaa', f'a{item}', priority=10 - item)
    for item in range(10):
        queue.push('bbbbbb', f'b{item}', priority=10 - item)

    assert len(queue) == 20
    queue.cleanup()
    assert len(queue) == 0
    assert queue.pop() is None


def test_cleanup_nothing(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    assert len(queue) == 0
    queue.cleanup()
    assert len(queue) == 0
    queue.cleanup()
    assert queue.pop() is None


def test_priority(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for item in range(10):
        queue.push('aaaaaa', f'a{item}', priority=10 - item)
    for item in range(10):
        queue.push('bbbbbb', f'b{item}', priority=item)

    assert len(queue) == 20
    items = ','.join(queue.pop() for _ in range(10))
    assert items == 'a0,b9,a1,b8,a2,b7,a3,b6,a4,b5'
    assert len(queue) == 10
    assert queue.pop() is None
    assert len(queue) == 10
    assert get_ttl(redis_conn) == {'test:usage': 1}

    sleep(1)
    assert queue.idle_seconds == pytest.approx(1, 0.05)

    items = ','.join(queue.pop() for _ in range(10))
    assert items == 'a5,b4,a6,b3,a7,b2,a8,b1,a9,b0'
    assert queue.pop() is None
    assert len(queue) == 0
    assert get_ttl(redis_conn) == {'test:usage': 1}


def test_window(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=1, resolution=Resolution.SECOND)
    for item in range(10):
        queue.push('A', f'a{item}', priority=item)

    assert len(queue) == 10
    assert queue.pop('X') == 'a9'
    assert get_ttl(redis_conn) == {'test:usage': 1}
    assert len(queue) == 9
    assert queue.pop('X') is None
    assert len(queue) == 9
    assert queue.pop('Y') == 'a8'
    assert len(queue) == 8
    assert queue.pop('Y') is None
    assert len(queue) == 8
    sleep(1.01)
    assert queue.pop('X') == 'a7'
    assert len(queue) == 7
    assert queue.pop('X') is None
    assert len(queue) == 7
    assert queue.pop('Y') == 'a6'
    assert len(queue) == 6
    assert queue.pop('Y') is None
    assert len(queue) == 6


def test_extras(redis_conn: StrictRedis, redis_monitor):
    queue = ThrottledQueue(redis_conn, 'test', limit=5, resolution=Resolution.SECOND)
    for item in range(10):
        queue.push('aaaaaa', f'a{item}', priority=10 - item)
    for item in range(10):
        queue.push('bbbbbb', f'b{item}', priority=10 - item)

    assert len(queue) == 20
    assert queue.pop() == 'a0'
    assert get_ttl(redis_conn) == {'test:usage': 1}
    assert len(queue) == 19
    queue.push('cccccc', 'c0', priority=11)
    queue.push('cccccc', 'c1', priority=-1)
    queue.push('aaaaaa', 'aX', priority=11)
    queue.push('aaaaaa', 'aY', priority=-1)
    assert len(queue) == 23

    items = ','.join(str(queue.pop()) for _ in range(13))
    assert len(queue) == 14
    assert items in ['b0,aX,b1,a1,b2,a2,b3,a3,b4,None,None,None,None']

    sleep(1)
    assert queue.idle_seconds == pytest.approx(1, 0.05)

    items = ','.join(str(queue.pop()) for _ in range(12))
    assert len(queue) == 2
    assert items in ['a4,b5,c0,a5,b6,c1,a6,b7,a7,b8,a8,b9']

    sleep(1)
    assert queue.idle_seconds == pytest.approx(1, 0.05)

    items = ','.join(str(queue.pop()) for _ in range(4))
    assert len(queue) == 0
    assert items in ['a9,aY,None,None']


def test_mocked_resolution():
    calls = []
    conn = SimpleNamespace(info=lambda: {'redis_version': '10'}, function_list=lambda _: [1], fcall=lambda *args: calls.append(args))
    with freezegun.freeze_time('2022-02-22') as ft:
        queue = ThrottledQueue(
            conn,
            'foobar',
            limit='?',
            resolution=10,
        )
        queue.pop()
        assert calls == [
            ('RTQ_POP', 0, 'foobar', 0, '?', 10),
        ]
        ft.tick(9)
        queue.pop()
        assert calls == [
            ('RTQ_POP', 0, 'foobar', 0, '?', 10),
            ('RTQ_POP', 0, 'foobar', 0, '?', 10),
        ]
        ft.tick(1)
        queue.pop()
        assert calls == [
            ('RTQ_POP', 0, 'foobar', 0, '?', 10),
            ('RTQ_POP', 0, 'foobar', 0, '?', 10),
            ('RTQ_POP', 0, 'foobar', 1, '?', 10),
        ]


def test_mocked_window():
    calls = []
    conn = SimpleNamespace(info=lambda: {'redis_version': '10'}, function_list=lambda _: [1], fcall=lambda *args: calls.append(args))
    queue = ThrottledQueue(
        conn,
        'foobar',
        limit='?',
        resolution=10,
    )
    queue.pop(window='foobar1')
    assert calls == [
        ('RTQ_POP', 0, 'foobar', 'foobar1', '?', 10),
    ]
    queue.pop(window='foobar2')
    assert calls == [
        ('RTQ_POP', 0, 'foobar', 'foobar1', '?', 10),
        ('RTQ_POP', 0, 'foobar', 'foobar2', '?', 10),
    ]


def test_validation():
    old_conn = SimpleNamespace(info=lambda: {'redis_version': '6.1'}, register_script=lambda _: None)
    pytest.raises(RuntimeError, ThrottledQueue, old_conn, 'foo')
    conn = SimpleNamespace(info=lambda: {'redis_version': '10'}, register_script=lambda _: None)
    pytest.raises(TypeError, ThrottledQueue, conn, b'caca')
    pytest.raises(TypeError, ThrottledQueue, conn, 123)
    pytest.raises(ValueError, ThrottledQueue(conn, 'foo').push, ':', None)
