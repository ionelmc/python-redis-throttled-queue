from operator import itemgetter
from textwrap import indent

import pytest
from process_tests import TestProcess
from process_tests import wait_for_strings
from redis.client import StrictRedis

from redis_throttled_queue import ThrottledQueue


@pytest.fixture
def redis_server(tmp_path):
    redis_socket = str(tmp_path.joinpath('redis.sock'))
    with TestProcess(
        'redis-server',
        '--slowlog-log-slower-than',
        '100',
        '--port',
        '0',
        '--appendonly',
        'no',
        '--save',
        '',
        '--maxmemory',
        '4G',
        '--dir',
        tmp_path,
        '--unixsocket',
        redis_socket,
    ) as redis_server:
        wait_for_strings(redis_server.read, 2, 'ready to accept connections')
        yield redis_socket
        print(redis_server.read())


@pytest.fixture
def redis_monitor(redis_server):
    with TestProcess('redis-cli', '-s', redis_server, 'monitor') as redis_monitor:
        yield
        print(redis_monitor.read())


@pytest.fixture
def redis_slowlog(redis_server):
    yield
    client = StrictRedis(unix_socket_path=redis_server, decode_responses=True)
    print(f'SLOWLOG ({client.slowlog_len()} entries):')
    for item in sorted(client.slowlog_get(), key=itemgetter('duration'), reverse=True):
        command = item["command"].decode()
        print(f'{item["duration"]:5}ms: {indent(command, "         ").strip()}')


@pytest.fixture
def redis_conn(redis_server):
    ThrottledQueue._library_missing = True
    return StrictRedis(unix_socket_path=redis_server, decode_responses=True)
