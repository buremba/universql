import base64
import datetime
import gzip
import json
import os
import re
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Tuple

import humanize
import psutil
from starlette.exceptions import HTTPException
from starlette.requests import Request


class Compute(Enum):
    LOCAL = "local"
    CLOUD = "cloud"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"


SNOWFLAKE_HOST = os.getenv('SNOWFLAKE_HOST')

# LOCALHOST_UNIVERSQL_COM_BYTES = {
#     "cert": base64.b64encode("""-----BEGIN CERTIFICATE-----
# -----END CERTIFICATE-----""".encode('utf-8')),
#     "key": base64.b64encode("""-----BEGIN PRIVATE KEY-----
# -----END PRIVATE KEY-----""".encode('utf-8'))
# }

LOCALHOST_UNIVERSQL_COM_BYTES = {
    'cert': b'LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURqRENDQXhLZ0F3SUJBZ0lTQTlISENaclNzdVFhVUxmNk1Cd1BsemdRTUFvR0NDcUdTTTQ5QkFNRE1ESXgKQ3pBSkJnTlZCQVlUQWxWVE1SWXdGQVlEVlFRS0V3MU1aWFFuY3lCRmJtTnllWEIwTVFzd0NRWURWUVFERXdKRgpOVEFlRncweU5ERXhNVEl4TkRJMU16ZGFGdzB5TlRBeU1UQXhOREkxTXpaYU1DRXhIekFkQmdOVkJBTVRGbXh2ClkyRnNhRzl6ZEdOdmJYQjFkR2x1Wnk1amIyMHdXVEFUQmdjcWhrak9QUUlCQmdncWhrak9QUU1CQndOQ0FBVDcKeWxDU0cvNXlqRWNlYXFVaEI3Q0tZRzRGL21oZEJhN2lOcFZQZXh0TDhHT2VWK0tQa1hGYkp5eUUwYmdtV3J4Tgo2MXo3c2FvMXFDOGNISGtWOFdZT280SUNGekNDQWhNd0RnWURWUjBQQVFIL0JBUURBZ2VBTUIwR0ExVWRKUVFXCk1CUUdDQ3NHQVFVRkJ3TUJCZ2dyQmdFRkJRY0RBakFNQmdOVkhSTUJBZjhFQWpBQU1CMEdBMVVkRGdRV0JCVEgKN296ZWdCc0M3M3g3WWhSdWNDRU9CSkhPY2pBZkJnTlZIU01FR0RBV2dCU2ZLMS9QUENGUG5RUzM3U3NzeE1adwppOUxYRFRCVkJnZ3JCZ0VGQlFjQkFRUkpNRWN3SVFZSUt3WUJCUVVITUFHR0ZXaDBkSEE2THk5bE5TNXZMbXhsCmJtTnlMbTl5WnpBaUJnZ3JCZ0VGQlFjd0FvWVdhSFIwY0RvdkwyVTFMbWt1YkdWdVkzSXViM0puTHpBaEJnTlYKSFJFRUdqQVlnaFpzYjJOaGJHaHZjM1JqYjIxd2RYUnBibWN1WTI5dE1CTUdBMVVkSUFRTU1Bb3dDQVlHWjRFTQpBUUlCTUlJQkF3WUtLd1lCQkFIV2VRSUVBZ1NCOUFTQjhRRHZBSFlBb3VNSzVFWHZ2YTJiZmpqdFIyZDNVOWVDClc0U1UxeXRlR3l6RXVWQ2tSK2NBQUFHVElQbVNGQUFBQkFNQVJ6QkZBaUF3MkdKSGhIQUs0STZJQjJKWjlaRzEKeDk4Mzlhc0Q3Q0FUVlFRL2YrUjFSd0loQU8xZm5WSy9JMVduZ1BSRDczejN1SE5LelN6S1NQVU9WSlVVSlNGNAphN2VGQUhVQXp4Rlc3dFV1ZksvemgxdlphUzZiNlJweFowcXdGK3lzQWRKYmQ4N01Pd2dBQUFHVElQbVNRd0FBCkJBTUFSakJFQWlBelhBdnQ1MzJlaFBhMndGN0E5Zk9UL0lnQ1pQNHhYV2JpN2FRaTg5RmU2UUlnYndjcHJWaWMKOEh3SncyTURKbVRmZEg0TlpRVWNsbXcvZ0xBNzFSVU1LUEF3Q2dZSUtvWkl6ajBFQXdNRGFBQXdaUUl3R2pSVwpqRGZmQVdHb2pDM08zVUhnS3l1NEZFK2IyaVNyTzZvN1ZLaU84bjJldjZlWXY3OEhGdEVVazUxWnk3MXhBakVBCnFWSTh4UUxnZHRIcnZiaW0wSlU0a2JpK3R4OTJkVzUwS2ovb3Yxckd1bXJRQWhPc3hIa1B3WXBUd1hZTXZ3SWoKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQotLS0tLUJFR0lOIENFUlRJRklDQVRFLS0tLS0KTUlJRVZ6Q0NBaitnQXdJQkFnSVJBSU9QYkdQT3NUbU1ZZ1ppZ3hYSi9kNHdEUVlKS29aSWh2Y05BUUVMQlFBdwpUekVMTUFrR0ExVUVCaE1DVlZNeEtUQW5CZ05WQkFvVElFbHVkR1Z5Ym1WMElGTmxZM1Z5YVhSNUlGSmxjMlZoCmNtTm9JRWR5YjNWd01SVXdFd1lEVlFRREV3eEpVMUpISUZKdmIzUWdXREV3SGhjTk1qUXdNekV6TURBd01EQXcKV2hjTk1qY3dNekV5TWpNMU9UVTVXakF5TVFzd0NRWURWUVFHRXdKVlV6RVdNQlFHQTFVRUNoTU5UR1YwSjNNZwpSVzVqY25sd2RERUxNQWtHQTFVRUF4TUNSVFV3ZGpBUUJnY3Foa2pPUFFJQkJnVXJnUVFBSWdOaUFBUU5DenFLCmEyR090dS9jWDFqbnhrSkZWS3RqOW1aaFNBb3VXWFcwZ1FJM1VMYy9Gbm5jbU95aEtKZHlJQndzejlWOFVpQk8KVkhoYmhCUnJ3SkN1aGV6QVVVRThXb2QvQmszVS9tRFIrbXd0NFgyVkVJaWlDRlFQbVJwTTV1b0tyTmlqZ2ZndwpnZlV3RGdZRFZSMFBBUUgvQkFRREFnR0dNQjBHQTFVZEpRUVdNQlFHQ0NzR0FRVUZCd01DQmdnckJnRUZCUWNECkFUQVNCZ05WSFJNQkFmOEVDREFHQVFIL0FnRUFNQjBHQTFVZERnUVdCQlNmSzEvUFBDRlBuUVMzN1Nzc3hNWncKaTlMWERUQWZCZ05WSFNNRUdEQVdnQlI1dEZubWU3Ymw1QUZ6Z0FpSXlCcFk5dW1iYmpBeUJnZ3JCZ0VGQlFjQgpBUVFtTUNRd0lnWUlLd1lCQlFVSE1BS0dGbWgwZEhBNkx5OTRNUzVwTG14bGJtTnlMbTl5Wnk4d0V3WURWUjBnCkJBd3dDakFJQmdabmdRd0JBZ0V3SndZRFZSMGZCQ0F3SGpBY29CcWdHSVlXYUhSMGNEb3ZMM2d4TG1NdWJHVnUKWTNJdWIzSm5MekFOQmdrcWhraUc5dzBCQVFzRkFBT0NBZ0VBSDNLZE5FVkNRZHFrMExLeXVOSW1US2RSSlkxQwoydXcyU0phanVocWt5R1BZOEMrenpzdWZaK21nbmhucTFBMktWUU9TeWtPRW5VYngxY3k2MzdyQkFpaHg5N3IrCmJjd2JaTTZzVERJYUVyaVIvUExrNkxLczlCZTB1b1Z4Z09LRGNwRzlzdkQzM0orRzlMY2Z2MUs5bHVEbVNUZ0cKNlhORklONXZmSTVncy9sTVB5b2pFTWRJeks5YmxjbDIvMXZLeE84V0dDY2p2c1ExbkovUHd0OExRWkJmT0Z5VgpYUDh1YkFwL2F1M2RjNEVLV0c5TU81emN4MXFUOStOWFJHZFZXeEd2bUJGUkFhamNpTWZYTUUxWnVHbWszL0dPCmtvQU03WmtqWm1sZXlva1AxTEd6bWZKY1VkOXM3ZWV1MS85L2VnNVhsWGQvNTVHdFlqQU0rQzRERzVpN2VhTnEKY20yRit5eFlJUHQ2Y2JidFlWTkpDR2ZIV3FIRVE0RllTdFV5Rm52OHNqeXFVOHlwZ1phTko5YVZjV1NJQ0xPSQpFMS9Rdi83b0tzblpDV0o5MjZ3VTZScUcxT1lQR09pMXp1QUJoTHc2MWN1UFZEVDI4blFTL2U2ejk1Y0pYcTBlCksxQmNhSjZmSlpzbWJqUmdENXAzbXZFZjV2ZFFNN01DRXZVMHRIYnN4Mkk1bUhISm9BQkhiOEtWQmdXcC9sY1gKR1dpV2FlT3lCN1JQK09mRHR2aTJPc2FweFhpVjd2TlZzN2ZNbHJSalkxam9LYXFtbXljbkJ2QXExNEFFYnR5TApzVmZPUzY2QjhhcGtlRlgyTlk0WFBFWVY0WlNDZThWSFByZHJFUmsyd0lMRzNUL0VHbVNJa0NZVlVNU25qbUpkClZRRDlGNk5hLyt6bVhDYz0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQ==',
    'key': b'LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JR0hBZ0VBTUJNR0J5cUdTTTQ5QWdFR0NDcUdTTTQ5QXdFSEJHMHdhd0lCQVFRZ0FUQmR4ZVhWdEcrSFpLREEKdll0eFR6alFLNHFYZnlnVXFBeDBYTy9lSWlPaFJBTkNBQVQ3eWxDU0cvNXlqRWNlYXFVaEI3Q0tZRzRGL21oZApCYTdpTnBWUGV4dEw4R09lVitLUGtYRmJKeXlFMGJnbVdyeE42MXo3c2FvMXFDOGNISGtWOFdZTwotLS0tLUVORCBQUklWQVRFIEtFWS0tLS0t'}


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


def session_from_request(self, request):
    """
    Get a request's relevant session
    """
    auth = request.headers.get('Authorization')
    if not auth:
        raise HTTPException(status_code=401, detail='No Authorization header')
    if not auth.startswith('Snowflake Token="'):
        raise HTTPException(status_code=401, detail='Invalid Authorization header')
    token = auth[17:-1]
    if token not in self.sessions:
        raise HTTPException(status_code=401, detail='Invalid Authorization header')
    return self.sessions[token]


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


def prepend_to_lines(input_string, prepend_string=" ", vertical_string='------'):
    if len(input_string) > 2500:
        input_string = input_string[0:2500] + '[striped due to max 2500 character limit]'
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


def parse_compute(value):
    if value is not None:
        matches = re.findall(pattern, value)
        if len(matches) == 0:
            matches = (('local', ''), ('snowflake', f'warehouse={value}'))
    else:
        matches = (('local', ''), ('snowflake', ''))

    result = []
    for func_name, args_str in matches:
        args = {}
        if args_str:
            for arg in args_str.split(','):
                if '=' in arg:
                    key, value = arg.split('=', 1)
                    args[key.strip()] = value.strip()
                else:
                    args[arg.strip()] = None  # Handle arguments without '='
        result.append({'name': func_name, 'args': args})
    return result


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
