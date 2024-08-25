import inspect
import logging
import os
from datetime import timedelta, datetime
from functools import wraps

import psutil
from fsspec.implementations.cache_mapper import AbstractCacheMapper
from fsspec.implementations.cached import SimpleCacheFileSystem

from universql.util import get_total_directory_size

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_lake")


class FileNameCacheMapper(AbstractCacheMapper):
    def __init__(self, directory):
        self.directory = directory

    def __call__(self, path: str) -> str:
        os.makedirs(os.path.dirname(os.path.join(self.directory, path)), exist_ok=True)
        return path


class throttle(object):
    """
    Decorator that prevents a function from being called more than once every
    time period.
    To create a function that cannot be called more than once a minute:
        @throttle(minutes=1)
        def my_fun():
            pass
    """

    def __init__(self, seconds=0, minutes=0, hours=0):
        self.throttle_period = timedelta(
            seconds=seconds, minutes=minutes, hours=hours
        )
        self.time_of_last_call = datetime.min

    def __call__(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            now = datetime.now()
            time_since_last_call = now - self.time_of_last_call

            if time_since_last_call > self.throttle_period:
                self.time_of_last_call = now
                return fn(*args, **kwargs)

        return wrapper


def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


last_free = None
first_free = None


def get_friendly_disk_usage(storage: str, debug=False) -> str:
    global last_free
    global first_free
    usage = psutil.disk_usage(storage)
    if first_free is None:
        first_free = usage.free
    current_usage = get_total_directory_size(storage)
    message = f"{sizeof_fmt(current_usage)} used {sizeof_fmt(usage.free)} available"
    if last_free is not None:
        downloaded_recently = last_free - usage.free
        if downloaded_recently > 1_000_000 or debug:
            downloaded_since_start = first_free - usage.free
            message += f" downloaded since start: {sizeof_fmt(downloaded_since_start)}"

    last_free = usage.free
    return message


class MonitoredSimpleCacheFileSystem(SimpleCacheFileSystem):

    def __init__(self, **kwargs):
        kwargs["cache_storage"] = os.path.join(kwargs.get("cache_storage"), kwargs.get('fs').protocol[0])
        super().__init__(**kwargs)
        self._mapper = FileNameCacheMapper(kwargs.get('cache_storage'))

    def _check_file(self, path):
        self._check_cache()
        cache_path = self._mapper(path)
        for storage in self.storage:
            fn = os.path.join(storage, cache_path)
            if os.path.exists(fn):
                return fn
            logger.info(f"Downloading {self.protocol[0]}://{path}")

    def glob(self, path):
        return [self._strip_protocol(path)]

    def size(self, path):
        cached_file = self._check_file(self._strip_protocol(path))
        if cached_file is None:
            return self.fs.size(path)
        else:
            return os.path.getsize(cached_file)

    def __getattribute__(self, item):
        if item in {
            # new items
            "size",
            "glob",
            # previous
            "load_cache",
            "_open",
            "save_cache",
            "close_and_update",
            "__init__",
            "__getattribute__",
            "__reduce__",
            "_make_local_details",
            "open",
            "cat",
            "cat_file",
            "cat_ranges",
            "get",
            "read_block",
            "tail",
            "head",
            "info",
            "ls",
            "exists",
            "isfile",
            "isdir",
            "_check_file",
            "_check_cache",
            "_mkcache",
            "clear_cache",
            "clear_expired_cache",
            "pop_from_cache",
            "local_file",
            "_paths_from_path",
            "get_mapper",
            "open_many",
            "commit_many",
            "hash_name",
            "__hash__",
            "__eq__",
            "to_json",
            "to_dict",
            "cache_size",
            "pipe_file",
            "pipe",
            "start_transaction",
            "end_transaction",
        }:
            # all the methods defined in this class. Note `open` here, since
            # it calls `_open`, but is actually in superclass
            return lambda *args, **kw: getattr(type(self), item).__get__(self)(
                *args, **kw
            )
        if item in ["__reduce_ex__"]:
            raise AttributeError
        if item in ["transaction"]:
            # property
            return type(self).transaction.__get__(self)
        if item in ["_cache", "transaction_type"]:
            # class attributes
            return getattr(type(self), item)
        if item == "__class__":
            return type(self)
        d = object.__getattribute__(self, "__dict__")
        fs = d.get("fs", None)  # fs is not immediately defined
        if item in d:
            return d[item]
        elif fs is not None:
            if item in fs.__dict__:
                # attribute of instance
                return fs.__dict__[item]
            # attributed belonging to the target filesystem
            cls = type(fs)
            m = getattr(cls, item)
            if (inspect.isfunction(m) or inspect.isdatadescriptor(m)) and (
                    not hasattr(m, "__self__") or m.__self__ is None
            ):
                # instance method
                return m.__get__(fs, cls)
            return m  # class method or attribute
        else:
            # attributes of the superclass, while target is being set up
            return super().__getattribute__(item)
