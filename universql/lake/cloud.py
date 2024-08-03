import aiobotocore
import gcsfs
import s3fs
from duckdb import DuckDBPyConnection
# from fsspec.utils import setup_logging
from pyiceberg.io import PY_IO_IMPL
from pyiceberg.io.fsspec import FsspecFileIO
from pyiceberg.table import StaticTable

from universql.lake.fsspec_util import MonitoredSimpleCacheFileSystem


def s3(cache_storage: str, profile: str = "default"):
    session = aiobotocore.session.AioSession(profile=profile)
    s3_file_system = s3fs.S3FileSystem(session=session)
    caching_fs = MonitoredSimpleCacheFileSystem(
        fs=s3_file_system,
        cache_storage=cache_storage,
    )
    return caching_fs


def gcs(cache_storage, project=None, token=None):
    # setup_logging(logger=logger, level="DEBUG")
    gcs_file_system = gcsfs.GCSFileSystem(project=project, token=token)
    caching_fs = MonitoredSimpleCacheFileSystem(
        fs=gcs_file_system,
        cache_storage=cache_storage,
    )
    return caching_fs


CACHE_DIRECTORY_KEY = "universql.cache_directory"


def iceberg(data):
    io = FsspecFileIO(data)
    get_fs = io.get_fs
    io.get_fs = lambda name: MonitoredSimpleCacheFileSystem(
        fs=get_fs(name),
        cache_storage=data.get(CACHE_DIRECTORY_KEY),
    )
    return io


def get_iceberg_table_from_data_lake(metadata_file_path: str, cache_directory):
    from_metadata = StaticTable.from_metadata(metadata_file_path, {
        PY_IO_IMPL: "universql.lake.cloud.iceberg",
        CACHE_DIRECTORY_KEY: cache_directory,
    })
    return from_metadata


def register_data_lake(duckdb: DuckDBPyConnection, args: dict):
    duckdb.register_filesystem(s3(args.get('cache_directory'), args.get('aws_profile')))
    duckdb.register_filesystem(gcs(args.get('cache_directory'), args.get('gcp_project')))
