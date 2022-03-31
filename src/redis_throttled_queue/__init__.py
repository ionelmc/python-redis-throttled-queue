import time
from enum import IntEnum
from logging import getLogger
from pathlib import Path
from typing import Union

from packaging.version import Version
from redis.client import StrictRedis

__version__ = '0.3.0'
__file_as_path__ = Path(__file__)

logger = getLogger(__name__)


class Resolution(IntEnum):
    SECOND = 1
    MINUTE = 60


# Get an item from the queue
# Arguments:
# - ARGV: <prefix> <window> <limit> <resolution>
POP_ITEM_SCRIPT = __file_as_path__.with_name('pop_item_script.lua').read_text()
PUSH_ITEM_SCRIPT = __file_as_path__.with_name('push_item_script.lua').read_text()


class ThrottledQueue(object):
    """
    Queue system with key-based throttling implemented over Redis.

    Publishers push given a key.

    Consumers pop one item at a time for the first key that has not exceeded the throttling limit withing the resolution window.
    """

    _client: StrictRedis
    _pop_item_script = None
    _push_item_script = None
    _count_items_script = None

    def __init__(self, redis_client, prefix, limit=10, resolution=Resolution.SECOND):
        """
        :param redis_client:
            An instance of :class:`~StrictRedis`.
        :param prefix:
            Redis key prefix.
        :param limit:
            Throttling limit. The queue won't retrieve more items in the given resolution for a given `key`.
        :param resolution:
            Resolution to use.
        """
        self._client = redis_client
        if not isinstance(prefix, str):
            raise TypeError(f"Incorrect type for `prefix`. Must be str, not {type(prefix)}.")
        self._prefix = prefix
        self._limit = limit
        self._resolution = resolution
        self._count_key = f"{self._prefix}:total"
        self.register_scripts(redis_client)
        info = redis_client.info()
        version = info["redis_version"]
        if Version(version) < Version('6.2.0'):
            raise RuntimeError(f"Redis 6.2 is the minimum version supported. The server reported version {version!r}.")

    def push(self, name: str, data: Union[str, bytes], *, priority: int = 0):
        if ":" in name:
            raise ValueError('Incorrect value for `key`. Cannot contain ":".')
        return self._push_item_script(client=self._client, keys=(), args=(self._prefix, name, priority, data))

    def pop(self, window: Union[str, bytes, int] = Ellipsis) -> Union[str, bytes, None]:
        if window is Ellipsis:
            window = int(time.time()) // self._resolution % 60
        return self._pop_item_script(client=self._client, keys=(), args=(self._prefix, window, self._limit, int(self._resolution)))

    def __len__(self):
        return int(self._client.get(self._count_key))

    @classmethod
    def register_scripts(cls, redis_client):
        if cls._pop_item_script is None:
            cls._pop_item_script = redis_client.register_script(POP_ITEM_SCRIPT)
        if cls._push_item_script is None:
            cls._push_item_script = redis_client.register_script(PUSH_ITEM_SCRIPT)
