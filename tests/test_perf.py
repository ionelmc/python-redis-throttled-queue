import pytest
from redis.client import StrictRedis

from redis_throttled_queue import ThrottledQueue


@pytest.mark.parametrize('items', [1000])
@pytest.mark.parametrize('limit', [5])
@pytest.mark.parametrize('names', [100000])
@pytest.mark.parametrize('push_names', [100])
@pytest.mark.parametrize('resolution', [10])
def test_push(
    benchmark,
    items,
    limit,
    names,
    push_names,
    redis_conn: StrictRedis,
    redis_slowlog,
    resolution,
):
    queue = ThrottledQueue(redis_conn, 'test', limit=limit, resolution=resolution)

    def setup():
        for name in range(names):
            queue.push(f'N{name}', 'initial')

    def run():
        for name in range(push_names):
            for item in range(items):
                queue.push(f'N{name}', str(name))

    benchmark.pedantic(run, setup=setup, iterations=1, rounds=10)
