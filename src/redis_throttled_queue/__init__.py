from enum import IntEnum
from logging import getLogger
from pathlib import Path
from time import time
from typing import Union

from packaging.version import Version
from redis.asyncio import StrictRedis as AsyncStrictRedis
from redis.client import StrictRedis

__version__ = '1.0.0'
__file_as_path__ = Path(__file__)

logger = getLogger(__name__)


class Resolution(IntEnum):
    SECOND = 1
    MINUTE = 60


LIBRARY = __file_as_path__.with_name('library.lua').read_text()


class ThrottledQueue(object):
    """
    Queue system with key-based throttling implemented over Redis.

    Publishers push given a key.

    Consumers pop one item at a time for the first key that has not exceeded the throttling limit withing the resolution window.
    """

    limit: int
    resolution: int
    last_activity: float
    _client: StrictRedis
    _library_missing: bool = True

    def __init__(
        self,
        redis_client: StrictRedis,
        prefix: str,
        limit: int = 10,
        resolution=Resolution.SECOND,
        validate_version=True,
        register_library=True,
    ):
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
            raise TypeError(f'Incorrect type for `prefix`. Must be str, not {type(prefix)}.')
        self._prefix = prefix
        self.limit = limit
        self.resolution = resolution
        self.last_activity = time()
        self._count_key = f'{self._prefix}:total'
        if register_library:
            self.register_library(redis_client)
        if validate_version:
            self.ensure_supported_redis(redis_client.info())

    @classmethod
    def ensure_supported_redis(cls, info: dict):
        version = info['redis_version']
        if Version(version) < Version('7.0'):
            raise RuntimeError(f'Redis 7.0 is the minimum version supported. The server reported version {version!r}.')

    def __len__(self):
        return int(self._client.get(self._count_key) or 0)

    def push(self, name: str, data: Union[str, bytes], *, priority: int = 0):
        if ':' in name:
            raise ValueError('Incorrect value for `key`. Cannot contain ":".')
        self.last_activity = time()
        return self._client.fcall('RTQ_PUSH', 0, self._prefix, name, priority, data)

    def pop(self, window: Union[str, bytes, int] = Ellipsis) -> Union[str, bytes, None]:
        if window is Ellipsis:
            window = int(time()) // self.resolution % 60
        value = self._client.fcall('RTQ_POP', 0, self._prefix, window, self.limit, int(self.resolution))
        if value is not None:
            self.last_activity = time()
        return value

    @property
    def idle_seconds(self) -> float:
        return time() - self.last_activity

    def cleanup(self):
        return self._client.fcall('RTQ_CLEANUP', 0, self._prefix)

    @classmethod
    def register_library(cls, redis_client: StrictRedis):
        if cls._library_missing:
            if not redis_client.function_list('RTQ'):
                redis_client.function_load(LIBRARY, replace=True)
            cls._library_missing = False


class AsyncThrottledQueue(ThrottledQueue):
    _client: AsyncStrictRedis

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, validate_version=False, register_library=False)

    @classmethod
    async def register_library(cls, redis_client: StrictRedis):
        if cls._library_missing:
            if not await redis_client.function_list('RTQ'):
                await redis_client.function_load(LIBRARY, replace=True)
            cls._library_missing = False

    async def validate_version(self):
        self.ensure_supported_redis(await self._client.info())

    async def size(self):
        return int(await self._client.get(self._count_key) or 0)

    async def push(self, name: str, data: Union[str, bytes], *, priority: int = 0):
        if ':' in name:
            raise ValueError('Incorrect value for `key`. Cannot contain ":".')
        self.last_activity = time()
        return await self._client.fcall('RTQ_PUSH', 0, self._prefix, name, priority, data)

    async def pop(self, window: Union[str, bytes, int] = Ellipsis) -> Union[str, bytes, None]:
        if window is Ellipsis:
            window = int(time()) // self.resolution % 60
        value = await self._client.fcall('RTQ_POP', 0, self._prefix, window, self.limit, int(self.resolution))
        if value is not None:
            self.last_activity = time()
        return value

    @property
    def idle_seconds(self) -> float:
        return time() - self.last_activity

    async def cleanup(self):
        return await self._client.fcall('RTQ_CLEANUP', 0, self._prefix)
