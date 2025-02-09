import os
import configparser
from pathlib import Path
import aiobotocore
import gcsfs
import s3fs
from fsspec.core import logger
from fsspec.utils import setup_logging
from pprint import pp

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

def get_aws_credentials(profile_name: str = None) -> dict:
    """
    Reads AWS credentials following standard AWS credential resolution
    Returns dict with aws_access_key_id and aws_secret_access_key
    
    Args:
        profile_name (str): The AWS profile name to use. Defaults to 'default'
    """
    
    profile_name = profile_name if profile_name is not None else "default"
    credentials = {}
    
    # Check for credentials file location
    credentials_path = os.environ.get('AWS_SHARED_CREDENTIALS_FILE')
    if not credentials_path:
        credentials_path = os.path.join(str(Path.home()), '.aws', 'credentials')
        
    # Check for config file location
    config_path = os.environ.get('AWS_CONFIG_FILE')
    if not config_path:
        config_path = os.path.join(str(Path.home()), '.aws', 'config')
        
    config = configparser.ConfigParser()
    
    # Read both files if they exist
    for path in [credentials_path, config_path]:
        if os.path.exists(path):
            config.read(path)
            
            # Config file uses 'profile name' instead of just 'name'
            profile_prefix = 'profile ' if path == config_path else ''
            section_name = f"{profile_prefix}{profile_name}"
            
            if section_name in config:
                profile = config[section_name]
                if 'aws_access_key_id' in profile:
                    credentials['aws_access_key_id'] = profile.get('aws_access_key_id')
                if 'aws_secret_access_key' in profile:
                    credentials['aws_secret_access_key'] = profile.get('aws_secret_access_key')
                if 'aws_session_token' in profile:
                    credentials['aws_session_token'] = profile.get('aws_session_token')
    
    return credentials