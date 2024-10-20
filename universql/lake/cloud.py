import os

import aiobotocore
import gcsfs
import s3fs
from fsspec.core import logger
from fsspec.utils import setup_logging
from pyiceberg.io import PY_IO_IMPL
from pyiceberg.table import StaticTable

from universql.lake.fsspec_util import MonitoredSimpleCacheFileSystem

in_lambda = os.environ.get('AWS_EXECUTION_ENV') is not None

def s3(cache_storage: str, profile: str = "default"):
    session = aiobotocore.session.AioSession(profile=profile)
    s3_file_system = s3fs.S3FileSystem(session=session)
    caching_fs = MonitoredSimpleCacheFileSystem(
        fs=s3_file_system,
        cache_storage=cache_storage,
    )
    return caching_fs


def gcs(cache_storage, project=None, token=None):
    setup_logging(logger=logger, level="ERROR")
    gcs_file_system = gcsfs.GCSFileSystem(project=project, token=token)
    caching_fs = MonitoredSimpleCacheFileSystem(
        fs=gcs_file_system,
        cache_storage=cache_storage,
    )
    return caching_fs


CACHE_DIRECTORY_KEY = "universql.cache_directory"
MAX_CACHE_SIZE = "universql.max_cache_size"


def iceberg(data):
    from pyiceberg.io.fsspec import FsspecFileIO
    io = FsspecFileIO(data)
    directory = data.get(CACHE_DIRECTORY_KEY)
    max_cache_size = data.get(MAX_CACHE_SIZE)
    get_fs = io.get_fs
    if max_cache_size is not None and max_cache_size != '0':
        logger.info(f"Using local cache in {directory}")
        io.get_fs = lambda name: MonitoredSimpleCacheFileSystem(
            fs=get_fs(name),
            cache_storage=directory,
        )
    else:
        logger.info("Local cache is not in use")
    return io
