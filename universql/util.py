import datetime
import gzip
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Tuple

import click
import humanize
import psutil
import sentry_sdk
import sqlglot
from starlette.exceptions import HTTPException
from starlette.requests import Request

logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)s %(name)s %(levelname)-6s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

sentry_sdk.init(
    dsn="https://7dd8ba359188efce454c24defced2f13@o29344.ingest.us.sentry.io/4508141310836736",
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for tracing.
    traces_sample_rate=1.0,
    debug=False,
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=0,
)

class Compute(Enum):
    LOCAL = "local"
    CLOUD = "cloud"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"


SNOWFLAKE_HOST = os.getenv('SNOWFLAKE_HOST')

# LOCALHOST_UNIVERSQL_COM_BYTES = {
#     "cert": base64.b64encode("""-----BEGIN CERTIFICATE-----
# -----END CERTIFICATE-----""".encode('utf-8')),
#     "key": base64.b64encode("""-----BEGIN EC PRIVATE KEY-----
# -----END EC PRIVATE KEY-----""".encode('utf-8'))
# }
# print(LOCALHOST_UNIVERSQL_COM_BYTES)

LOCALHOST_UNIVERSQL_COM_BYTES = {
    'cert': b'LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURqVENDQXhPZ0F3SUJBZ0lTQTgycTJLRVpFRHhhakRoUENObnpuR3F1TUFvR0NDcUdTTTQ5QkFNRE1ESXgKQ3pBSkJnTlZCQVlUQWxWVE1SWXdGQVlEVlFRS0V3MU1aWFFuY3lCRmJtTnllWEIwTVFzd0NRWURWUVFERXdKRgpOVEFlRncweU5UQXlNVEF4TlRNME5USmFGdzB5TlRBMU1URXhOVE0wTlRGYU1DRXhIekFkQmdOVkJBTVRGbXh2ClkyRnNhRzl6ZEdOdmJYQjFkR2x1Wnk1amIyMHdXVEFUQmdjcWhrak9QUUlCQmdncWhrak9QUU1CQndOQ0FBUjEKU01Ua0NDZjJnMUhYcGIxRGNUT1YrWm4wY0xuY2hVZDVURXNiSmYweFhMNk9xY2ZZbWMwL0s3MTNQbk55SFVpNQpwdzh4NXlSNkpHd05xS1FHaWN2b280SUNHRENDQWhRd0RnWURWUjBQQVFIL0JBUURBZ2VBTUIwR0ExVWRKUVFXCk1CUUdDQ3NHQVFVRkJ3TUJCZ2dyQmdFRkJRY0RBakFNQmdOVkhSTUJBZjhFQWpBQU1CMEdBMVVkRGdRV0JCVEgKTEF2aUJqeHc3K05jOTB1NTllNCsvNGRlY0RBZkJnTlZIU01FR0RBV2dCU2ZLMS9QUENGUG5RUzM3U3NzeE1adwppOUxYRFRCVkJnZ3JCZ0VGQlFjQkFRUkpNRWN3SVFZSUt3WUJCUVVITUFHR0ZXaDBkSEE2THk5bE5TNXZMbXhsCmJtTnlMbTl5WnpBaUJnZ3JCZ0VGQlFjd0FvWVdhSFIwY0RvdkwyVTFMbWt1YkdWdVkzSXViM0puTHpBaEJnTlYKSFJFRUdqQVlnaFpzYjJOaGJHaHZjM1JqYjIxd2RYUnBibWN1WTI5dE1CTUdBMVVkSUFRTU1Bb3dDQVlHWjRFTQpBUUlCTUlJQkJBWUtLd1lCQkFIV2VRSUVBZ1NCOVFTQjhnRHdBSFlBNXRJeFkwQjNqTUVRUVFiWGNibk93ZEpBCjlwYUVodnU2aHpJZC9SNDNqbEFBQUFHVThMVlRyd0FBQkFNQVJ6QkZBaUVBeUNSNlZlMVNoYWFicFMzeDNUL3QKSWl3VDNpRHovS0xWWEszNFcyMC9LdEVDSUhpMDdkcDB2SHlGd3hRb3FWMVlONHJxTGNDdFE3R0REODFkRENsVQo2TmhaQUhZQVRuV2pKMXlhRU1NNFcyelUzejlTNngzdzRJNGJqV25Bc2Zwa3NXS2FPZDhBQUFHVThMVlRvd0FBCkJBTUFSekJGQWlFQS9NbUxVRmhiRjNBbXVad1Y0cU11ZHJoTkVVWWJRWEdFMTQvZU5OeUJ4aDhDSUFzWDVrSk8KYjFtR0NTOFoyT0F0cGRuQ3JVcjVneXF5OG1DVnhkbWowak4xTUFvR0NDcUdTTTQ5QkFNREEyZ0FNR1VDTVFDRAowWmxQbnBlUEdsZDdMVnloQTFFVXlHVVhZb2o2ZUs0Ynd1VUJnZFhWMXphRkhlUGlDYjV0VHMxNm4wM1g5eVlDCk1GQkwrQjZLTWR2Y0s0bnIxVlU5aEd3cmJuK3dUa1J5NVpBL2toWjRQSSswVmh2OWIzNkN2MEt5ZytNLzJOODEKcGc9PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCi0tLS0tQkVHSU4gQ0VSVElGSUNBVEUtLS0tLQpNSUlFVnpDQ0FqK2dBd0lCQWdJUkFJT1BiR1BPc1RtTVlnWmlneFhKL2Q0d0RRWUpLb1pJaHZjTkFRRUxCUUF3ClR6RUxNQWtHQTFVRUJoTUNWVk14S1RBbkJnTlZCQW9USUVsdWRHVnlibVYwSUZObFkzVnlhWFI1SUZKbGMyVmgKY21Ob0lFZHliM1Z3TVJVd0V3WURWUVFERXd4SlUxSkhJRkp2YjNRZ1dERXdIaGNOTWpRd016RXpNREF3TURBdwpXaGNOTWpjd016RXlNak0xT1RVNVdqQXlNUXN3Q1FZRFZRUUdFd0pWVXpFV01CUUdBMVVFQ2hNTlRHVjBKM01nClJXNWpjbmx3ZERFTE1Ba0dBMVVFQXhNQ1JUVXdkakFRQmdjcWhrak9QUUlCQmdVcmdRUUFJZ05pQUFRTkN6cUsKYTJHT3R1L2NYMWpueGtKRlZLdGo5bVpoU0FvdVdYVzBnUUkzVUxjL0ZubmNtT3loS0pkeUlCd3N6OVY4VWlCTwpWSGhiaEJScndKQ3VoZXpBVVVFOFdvZC9CazNVL21EUittd3Q0WDJWRUlpaUNGUVBtUnBNNXVvS3JOaWpnZmd3CmdmVXdEZ1lEVlIwUEFRSC9CQVFEQWdHR01CMEdBMVVkSlFRV01CUUdDQ3NHQVFVRkJ3TUNCZ2dyQmdFRkJRY0QKQVRBU0JnTlZIUk1CQWY4RUNEQUdBUUgvQWdFQU1CMEdBMVVkRGdRV0JCU2ZLMS9QUENGUG5RUzM3U3NzeE1adwppOUxYRFRBZkJnTlZIU01FR0RBV2dCUjV0Rm5tZTdibDVBRnpnQWlJeUJwWTl1bWJiakF5QmdnckJnRUZCUWNCCkFRUW1NQ1F3SWdZSUt3WUJCUVVITUFLR0ZtaDBkSEE2THk5NE1TNXBMbXhsYm1OeUxtOXlaeTh3RXdZRFZSMGcKQkF3d0NqQUlCZ1puZ1F3QkFnRXdKd1lEVlIwZkJDQXdIakFjb0JxZ0dJWVdhSFIwY0RvdkwzZ3hMbU11YkdWdQpZM0l1YjNKbkx6QU5CZ2txaGtpRzl3MEJBUXNGQUFPQ0FnRUFIM0tkTkVWQ1FkcWswTEt5dU5JbVRLZFJKWTFDCjJ1dzJTSmFqdWhxa3lHUFk4Qyt6enN1ZlorbWduaG5xMUEyS1ZRT1N5a09FblVieDFjeTYzN3JCQWloeDk3cisKYmN3YlpNNnNURElhRXJpUi9QTGs2TEtzOUJlMHVvVnhnT0tEY3BHOXN2RDMzSitHOUxjZnYxSzlsdURtU1RnRwo2WE5GSU41dmZJNWdzL2xNUHlvakVNZEl6SzlibGNsMi8xdkt4TzhXR0NjanZzUTFuSi9Qd3Q4TFFaQmZPRnlWClhQOHViQXAvYXUzZGM0RUtXRzlNTzV6Y3gxcVQ5K05YUkdkVld4R3ZtQkZSQWFqY2lNZlhNRTFadUdtazMvR08Ka29BTTdaa2pabWxleW9rUDFMR3ptZkpjVWQ5czdlZXUxLzkvZWc1WGxYZC81NUd0WWpBTStDNERHNWk3ZWFOcQpjbTJGK3l4WUlQdDZjYmJ0WVZOSkNHZkhXcUhFUTRGWVN0VXlGbnY4c2p5cVU4eXBnWmFOSjlhVmNXU0lDTE9JCkUxL1F2LzdvS3NuWkNXSjkyNndVNlJxRzFPWVBHT2kxenVBQmhMdzYxY3VQVkRUMjhuUVMvZTZ6OTVjSlhxMGUKSzFCY2FKNmZKWnNtYmpSZ0Q1cDNtdkVmNXZkUU03TUNFdlUwdEhic3gySTVtSEhKb0FCSGI4S1ZCZ1dwL2xjWApHV2lXYWVPeUI3UlArT2ZEdHZpMk9zYXB4WGlWN3ZOVnM3Zk1sclJqWTFqb0thcW1teWNuQnZBcTE0QUVidHlMCnNWZk9TNjZCOGFwa2VGWDJOWTRYUEVZVjRaU0NlOFZIUHJkckVSazJ3SUxHM1QvRUdtU0lrQ1lWVU1TbmptSmQKVlFEOUY2TmEvK3ptWENjPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0t',
    'key': b'LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IY0NBUUVFSU1yQWxNMDJySG5vNm02b3BhU0YvNk9OS2o0bkhzTStJSlRiS3Mwck1NUnJvQW9HQ0NxR1NNNDkKQXdFSG9VUURRZ0FFZFVqRTVBZ245b05SMTZXOVEzRXpsZm1aOUhDNTNJVkhlVXhMR3lYOU1WeStqcW5IMkpuTgpQeXU5ZHo1emNoMUl1YWNQTWVja2VpUnNEYWlrQm9uTDZBPT0KLS0tLS1FTkQgRUMgUFJJVkFURSBLRVktLS0tLQ=='}


class Catalog(Enum):
    SNOWFLAKE = "snowflake"
    POLARIS = "polaris"


parameters = [
    {
        "name": "TIMESTAMP_OUTPUT_FORMAT",
        "value": "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM"
    },
    {
        "name": "CLIENT_PREFETCH_THREADS",
        "value": 4
    },
    {
        "name": "TIME_OUTPUT_FORMAT",
        "value": "HH24:MI:SS"
    },
    {
        "name": "TIMESTAMP_TZ_OUTPUT_FORMAT",
        "value": ""
    },
    {
        "name": "CLIENT_RESULT_CHUNK_SIZE",
        "value": 640
    },
    {
        "name": "CLIENT_SESSION_KEEP_ALIVE",
        "value": False
    },
    {
        "name": "QUERY_CONTEXT_CACHE_SIZE",
        "value": 5
    },
    {
        "name": "CLIENT_METADATA_USE_SESSION_DATABASE",
        "value": False
    },
    {
        "name": "CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED",
        "value": False
    },
    {
        "name": "TIMESTAMP_NTZ_OUTPUT_FORMAT",
        "value": "YYYY-MM-DD HH24:MI:SS.FF3"
    },
    {
        "name": "CLIENT_RESULT_PREFETCH_THREADS",
        "value": 1
    },
    {
        "name": "CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX",
        "value": False
    },
    {
        "name": "CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ",
        "value": True
    },
    {
        "name": "CLIENT_MEMORY_LIMIT",
        "value": 15360
    },
    {
        "name": "CLIENT_TIMESTAMP_TYPE_MAPPING",
        "value": "TIMESTAMP_LTZ"
    },
    {
        "name": "TIMEZONE",
        "value": "America/Los_Angeles"
    },
    {
        "name": "PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER",
        "value": True
    },
    {
        "name": "SNOWPARK_REQUEST_TIMEOUT_IN_SECONDS",
        "value": 86400
    },
    {
        "name": "PYTHON_CONNECTOR_USE_NANOARROW",
        "value": True
    },
    {
        "name": "CLIENT_RESULT_PREFETCH_SLOTS",
        "value": 2
    },
    {
        "name": "CLIENT_TELEMETRY_ENABLED",
        "value": False
    },
    {
        "name": "CLIENT_DISABLE_INCIDENTS",
        "value": False
    },
    {
        "name": "CLIENT_USE_V1_QUERY_API",
        "value": False
    },
    {
        "name": "CLIENT_RESULT_COLUMN_CASE_INSENSITIVE",
        "value": False
    },
    {
        "name": "BINARY_OUTPUT_FORMAT",
        "value": "HEX"
    },
    {
        "name": "CSV_TIMESTAMP_FORMAT",
        "value": ""
    },
    {
        "name": "CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS",
        "value": False
    },
    {
        "name": "CLIENT_TELEMETRY_SESSIONLESS_ENABLED",
        "value": True
    },
    {
        "name": "DATE_OUTPUT_FORMAT",
        "value": "YYYY-MM-DD"
    },
    {
        "name": "CLIENT_CONSENT_CACHE_ID_TOKEN",
        "value": False
    },
    {
        "name": "CLIENT_FORCE_PROTECT_ID_TOKEN",
        "value": True
    },
    {
        "name": "CLIENT_STAGE_ARRAY_BINDING_THRESHOLD",
        "value": 65280
    },
    {
        "name": "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY",
        "value": 36000
    },
    {
        "name": "CLIENT_SESSION_CLONE",
        "value": False
    },
    {
        "name": "AUTOCOMMIT",
        "value": True
    }
]


# parameters = [{"name": "TIMESTAMP_OUTPUT_FORMAT", "value": "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM"},
#               {"name": "CLIENT_PREFETCH_THREADS", "value": 4}, {"name": "TIME_OUTPUT_FORMAT", "value": "HH24:MI:SS"},
#               {"name": "TIMESTAMP_TZ_OUTPUT_FORMAT", "value": ""}, {"name": "CLIENT_RESULT_CHUNK_SIZE", "value": 160},
#               {"name": "CLIENT_SESSION_KEEP_ALIVE", "value": False},
#               {"name": "JDBC_RS_COLUMN_CASE_INSENSITIVE", "value": False},
#               {"name": "SNOWPARK_HIDE_INTERNAL_ALIAS", "value": True},
#               {"name": "CLIENT_CONSERVATIVE_MEMORY_ADJUST_STEP", "value": 64},
#               {"name": "CLIENT_METADATA_USE_SESSION_DATABASE", "value": False},
#               {"name": "QUERY_CONTEXT_CACHE_SIZE", "value": 5},
#               {"name": "JDBC_ENABLE_COMBINED_DESCRIBE", "value": False},
#               {"name": "ENABLE_STAGE_S3_PRIVATELINK_FOR_US_EAST_1", "value": False},
#               {"name": "CLIENT_RESULT_PREFETCH_THREADS", "value": 1},
#               {"name": "TIMESTAMP_NTZ_OUTPUT_FORMAT", "value": "YYYY-MM-DD HH24:MI:SS.FF3"},
#               {"name": "JDBC_TREAT_DECIMAL_AS_INT", "value": True},
#               {"name": "CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX", "value": False},
#               {"name": "CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ", "value": True},
#               {"name": "CLIENT_MEMORY_LIMIT", "value": 1536},
#               {"name": "CLIENT_TIMESTAMP_TYPE_MAPPING", "value": "TIMESTAMP_LTZ"},
#               {"name": "JDBC_EFFICIENT_CHUNK_STORAGE", "value": True},
#               {"name": "TIMEZONE", "value": "America/Los_Angeles"},
#               {"name": "SNOWPARK_REQUEST_TIMEOUT_IN_SECONDS", "value": 86400},
#               {"name": "CLIENT_RESULT_PREFETCH_SLOTS", "value": 2}, {"name": "CLIENT_DISABLE_INCIDENTS", "value": True},
#               {"name": "JDBC_ENABLE_PUT_GET", "value": True}, {"name": "BINARY_OUTPUT_FORMAT", "value": "HEX"},
#               {"name": "CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE", "value": True},
#               {"name": "CSV_TIMESTAMP_FORMAT", "value": ""},
#               {"name": "CLIENT_TELEMETRY_SESSIONLESS_ENABLED", "value": True},
#               {"name": "CLIENT_FORCE_PROTECT_ID_TOKEN", "value": True},
#               {"name": "CLIENT_CONSENT_CACHE_ID_TOKEN", "value": False},
#               {"name": "DATE_OUTPUT_FORMAT", "value": "YYYY-MM-DD"},
#               {"name": "JDBC_FORMAT_DATE_WITH_TIMEZONE", "value": True},
#               {"name": "SNOWPARK_LAZY_ANALYSIS", "value": True}, {"name": "JDBC_USE_JSON_PARSER", "value": True},
#               {"name": "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "value": 3600},
#               {"name": "AUTOCOMMIT", "value": True}, {"name": "CLIENT_SESSION_CLONE", "value": False},
#               {"name": "TIMESTAMP_LTZ_OUTPUT_FORMAT", "value": ""},
#               {"name": "JDBC_USE_SESSION_TIMEZONE", "value": True},
#               {"name": "JDBC_EXECUTE_RETURN_COUNT_FOR_DML", "value": False},
#               {"name": "JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC", "value": False},
#               {"name": "ENABLE_FIX_1247059", "value": True},
#               {"name": "CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED", "value": False},
#               {"name": "SNOWPARK_USE_SCOPED_TEMP_OBJECTS", "value": False},
#               {"name": "CLIENT_TELEMETRY_ENABLED", "value": True}, {"name": "CLIENT_USE_V1_QUERY_API", "value": True},
#               {"name": "CLIENT_RESULT_COLUMN_CASE_INSENSITIVE", "value": False},
#               {"name": "CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS", "value": False},
#               {"name": "CLIENT_STAGE_ARRAY_BINDING_THRESHOLD", "value": 65280}]


@dataclass
class TextLiteral:
    value: str


async def unpack_request_body(request: Request) -> dict:
    body = await request.body()
    if request.headers.get('content-encoding') == 'gzip':
        uz = gzip.decompress(body)
    else:
        uz = body
    return json.loads(uz)


class QueryError(Exception):
    def __init__(self, message: str, sql_state: str = "02000"):
        self.message = message
        self.sql_state = sql_state


class SnowflakeError(QueryError):
    def __init__(self, id: str, message: str, sql_state: str = "02000"):
        self.id = id
        self.message = message
        self.sql_state = sql_state

    def to_dict(self):
        return {'data': {'internalError': False, 'errorCode': '002043', 'age': 0, 'sqlState': self.sql_state,
                         'queryId': self.id, 'line': -1, 'pos': -1, 'type': 'UNIVERSQL'},
                'code': '002043',
                'message': self.message,
                'success': False, 'headers': None}


def session_from_request(sessions: List[str], request: Request):
    """
    Get a request's relevant session
    """
    auth = request.headers.get("Authorization")
    if not auth:
        raise HTTPException(status_code=401, detail="Session token not found in the request data.")
    if not auth.startswith('Snowflake Token="'):
        raise HTTPException(status_code=401, detail="Invalid Authorization header")
    token = auth[17:-1]
    if token not in sessions:
        raise HTTPException(status_code=401,
                            detail="User must login again to access the service. Maybe server restarted? Restarting Universql aborts all the concurrent sessions.")
    return sessions[token]


def pprint_secs(secs):
    """Format seconds in a human readable form."""
    now = time.time()
    secs_ago = int(now - secs)
    fmt = "%M:%S" if secs_ago < 60 * 60 * 24 else "%Y-%m-%d %H:%M:%S"
    return datetime.datetime.fromtimestamp(secs_ago).strftime(fmt)


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1000.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1000.0
    return f"{num:.1f}Y{suffix}"


def get_friendly_time_since(start_time, performance_counter):
    return humanize.precisedelta(datetime.timedelta(seconds=performance_counter - start_time),
                                 suppress=["days"], format="%0.3f")


def prepend_to_lines(input_string, prepend_string=" ", vertical_string='------', max=2500):
    if len(input_string) > max:
        input_string = input_string[0:max] + '[striped due to max 2500 character limit]'
    lines = input_string.split('\n')
    modified_lines = [prepend_string + line for line in lines]
    modified_string = '\n'.join(modified_lines)
    return modified_string + '\n' + vertical_string


def print_dict_as_markdown_table(input_dict, footer_message: Tuple[str], column_width=(8, 80)):
    top_bottom_line = "─" * (87 + 8)
    result = top_bottom_line
    for key, value in input_dict.items():
        result += f"\n│ {str(key).ljust(column_width[0])} │ {str(value).ljust(column_width[1])} │"

    footer = '\n' + top_bottom_line + '\n' + '\n'.join(
        ["│ " + message.ljust(92) + '│' for message in footer_message]) + '\n'
    return result + footer + top_bottom_line

def get_context_params(endpoint):
    env_vars = {}
    for param in endpoint.params:
        if param.default is not None:
            env_vars[param.name] = str(param.default)
        if param.envvar is not None:
            env_value = os.getenv(param.envvar, None)
            if env_value is not None:
                env_vars[param.name] = env_value
    return env_vars

def time_me(func):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        original_return_val = func(*args, **kwargs)
        end = time.perf_counter()
        print("time elapsed in ", func.__name__, ": ", end - start, sep='')
        return original_return_val

    return wrapper


def get_total_directory_size(directory: str):
    return sum(f.stat().st_size for f in Path(directory).glob('**/*') if f.is_file())


def remove_nulls_from_dict(input_dict):
    return {k: v for k, v in input_dict.items() if v is not None}


TOTAL_MEMORY_SIZE = psutil.virtual_memory().total

def calculate_script_cost(duration_second, electricity_rate=0.15, pc_lifetime_years=5):
    execution_time_hours = duration_second / (60 * 60)  # Convert ms to hours

    # Get system information
    cpu_count = psutil.cpu_count()
    memory_gb = TOTAL_MEMORY_SIZE / (1024 ** 3)  # Convert bytes to GB

    # Estimate hardware costs
    cpu_cost_per_core = 50
    memory_cost_per_gb = 5
    total_hardware_cost = (cpu_count * cpu_cost_per_core) + (memory_gb * memory_cost_per_gb)

    # Calculate hardware depreciation cost for the script duration
    hardware_cost = (total_hardware_cost / (pc_lifetime_years * 365 * 24)) * execution_time_hours

    # Estimate power consumption (assuming 50% utilization)
    estimated_power_watts = (cpu_count * 25) + (memory_gb * 0.3)
    power_consumed = (estimated_power_watts * 0.5 * execution_time_hours) / 1000  # in kWh

    # Calculate electricity cost
    electricity_cost = power_consumed * electricity_rate

    total_cost = (electricity_cost + hardware_cost)

    # (
    #     f"Estimated Power: {estimated_power_watts:.2f} watts | "
    #     f"Power Consumed: {power_consumed:.6f} kWh | "
    #     f"Electricity Cost: ${electricity_cost:.6f} | "
    #     f"Hardware Cost: ${hardware_cost:.6f} | "
    #     f"Total Cost: ${total_cost:.6f}"
    # )
    return f"~ ${total_cost:.6f}"


pattern = r'(\w+)(?:\(([^)]*)\))'
DEFAULTS = {
    "max_memory": sizeof_fmt(TOTAL_MEMORY_SIZE * 0.8),
    "max_cache_size": sizeof_fmt(psutil.disk_usage("./").free * 0.8)
}

LOCALHOSTCOMPUTING_COM = "localhostcomputing.com"


@dataclass
class SnowflakeAccount:
    account: str
    region: str
    cloud: str


# account_info = parse_snowflake_account('lt51601.europe-west2.gcp')
# print(account_info)  # Output: ('lt51601', 'europe-west2', 'gcp')
#
# account_info = parse_snowflake_account('xy12345.fhplus.us-gov-west-1.aws')
# print(account_info)  # Output: ('xy12345', 'fhplus.us-gov-west-1', 'aws')
def parse_snowflake_account(account_identifier: str) -> SnowflakeAccount:
    # Split the account identifier into parts
    parts = account_identifier.split('.')

    # Extract the account, region, and cloud (if present)
    account = parts[0]

    if len(parts) == 2:
        region = parts[1]
        cloud = 'aws'
    elif len(parts) >= 3:
        region = '.'.join(parts[1:-1])
        cloud = parts[-1]
    else:
        region = None
        cloud = 'aws'

    # Assume aws if cloud is not one of aws, gcp, or azure
    if cloud not in ('aws', 'gcp', 'azure'):
        cloud = 'aws'

    return SnowflakeAccount(account, region, cloud)


def full_qualifier(table: sqlglot.exp.Table, credentials: dict):
    catalog = sqlglot.exp.Identifier(this=credentials.get('database')) \
        if table.args.get('catalog') is None else table.args.get('catalog')
    db = sqlglot.exp.Identifier(this=credentials.get('schema')) \
        if table.args.get('db') is None else table.args.get('db')
    new_table = sqlglot.exp.Table(catalog=catalog, db=db, this=table.this)
    return new_table


MODULES = [
    "universql.warehouse.duckdb",
    "universql.warehouse.bigquery",
    "universql.warehouse.snowflake",
    "universql.warehouse.snowflake",
]


def load_catalogs():
    """
    Import a predefined list of modules.
    """
    for module_path in MODULES:
        try:
            __import__(module_path)
        except Exception as e:
            print(f"Failed to load {module_path}: {e}")

current_context = None

def initialize_context():
    context = click.get_current_context(silent=True)
    if context is None:
        # set log level
        logging.getLogger().setLevel(logging.INFO)

        # not running through CLI
        from universql.main import snowflake
        globals()["current_context"] = get_context_params(snowflake)
    else:
        globals()["current_context"] = context.params