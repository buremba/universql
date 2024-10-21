import os

import aiobotocore
import gcsfs
import s3fs
from fsspec.core import logger
from fsspec.utils import setup_logging

from universql.lake.fsspec_util import MonitoredSimpleCacheFileSystem

in_lambda = os.environ.get('AWS_EXECUTION_ENV') is not None


def s3(context: dict):
    cache_storage = context.get('cache_directory')
    session = aiobotocore.session.AioSession(profile=context.get("aws_profile"))
    s3_file_system = s3fs.S3FileSystem(session=session)
    if context.get("max_cache_size", "0") != "0":
        s3_file_system = MonitoredSimpleCacheFileSystem(
            fs=s3_file_system,
            cache_storage=cache_storage,
        )

    return s3_file_system


def gcs(context: dict):
    cache_storage = context.get('cache_directory')
    setup_logging(logger=logger, level="ERROR")
    gcs_file_system = gcsfs.GCSFileSystem(project=context.get('gcp_project'))
    if context.get("max_cache_size", "0") != "0":
        gcs_file_system = MonitoredSimpleCacheFileSystem(
            fs=gcs_file_system,
            cache_storage=cache_storage,
        )
    return gcs_file_system


CACHE_DIRECTORY_KEY = "universql.cache_directory"
MAX_CACHE_SIZE = "universql.max_cache_size"


def iceberg(context: dict):
    from pyiceberg.io.fsspec import FsspecFileIO
    io = FsspecFileIO(context)
    directory = context.get(CACHE_DIRECTORY_KEY)
    max_cache_size = context.get(MAX_CACHE_SIZE)
    get_fs = io.get_fs
    if max_cache_size is not None and max_cache_size != '0':
        io.get_fs = lambda name: MonitoredSimpleCacheFileSystem(
            fs=get_fs(name),
            cache_storage=directory,
        )
    return io
