import os
import socketserver
import sys
import threading
from contextlib import contextmanager
from typing import Generator

import pyarrow
import pytest
from click.testing import CliRunner
from snowflake.connector import connect as snowflake_connect, SnowflakeConnection
from snowflake.connector.config_manager import CONFIG_MANAGER
from snowflake.connector.constants import CONNECTIONS_FILE
from itertools import product

from universql.util import LOCALHOSTCOMPUTING_COM

# Configuration using separate connection strings for direct and proxy connections
# export SNOWFLAKE_CONNECTION_STRING="account=xxx;user=xxx;password=xxx;warehouse=xxx;database=xxx;schema=xxx"
# export UNIVERSQL_CONNECTION_STRING="warehouse=xxx"
SNOWFLAKE_CONNECTION_NAME = os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"

# Allow Universql to start
os.environ["MAX_CON_RETRY_ATTEMPTS"] = "100"

SIMPLE_QUERY = """
SELECT 1 as test
"""

ALL_COLUMNS_QUERY = """
SELECT
-- Numeric data types
12345678901234567890123456789012345678::NUMBER AS sample_number,
123.45::DECIMAL AS sample_decimal,
6789::INT AS sample_int,
9876543210::BIGINT AS sample_bigint,
123::SMALLINT AS sample_smallint,
42::TINYINT AS sample_tinyint,
255::BYTEINT AS sample_byteint,
12345.6789::FLOAT AS sample_float,
123456789.123456789::DOUBLE AS sample_double,

-- String & binary data types
'Sample text'::VARCHAR AS sample_varchar,
'C'::CHAR AS sample_char,
'Another sample text'::STRING AS sample_string,
'More text'::TEXT AS sample_text,
cast('307834' as binary) AS sample_binary,
cast('307834' as varbinary) AS sample_varbinary,

-- Logical data types
TRUE::BOOLEAN AS sample_boolean,

-- Date & time data types
'2023-01-01'::DATE AS sample_date,
-- '12:34:56'::TIME AS sample_time, # somehow python is broken but java sdk works

 '2023-01-01 10:34:56'::DATETIME AS sample_datetime,
 '2023-01-01 11:34:56'::TIMESTAMP AS sample_timestamp,
-- no support for duckdb
 '2023-01-01 12:34:56'::TIMESTAMP_LTZ AS sample_timestamp_ltz,
 '2023-01-01 13:34:56'::TIMESTAMP_NTZ AS sample_timestamp_ntz,

-- no support for snowflake + duckdb
'2024-08-03 22:51:25.595+01'::TIMESTAMP_TZ AS sample_timestamp_tz,

-- Semi-structured data types
PARSE_JSON('{"key":"value"}')::VARIANT AS sample_variant,
OBJECT_CONSTRUCT('foo', 1234567, 'distinct_province', (SELECT 1)) AS sample_object,
ARRAY_CONSTRUCT(1, 2, 3, 4) AS sample_array,

-- no support for
-- Geospatial data types
-- TO_GEOGRAPHY('LINESTRING(30 10, 10 30, 40 40)') AS sample_geometry,

-- no support for
-- Vector data types
-- [1.1,2.2,3]::VECTOR(FLOAT,3) AS sample_vector
"""


@pytest.fixture(scope="session")
def snowflake_connection() -> Generator:
    conn = snowflake_connect(connection_name=SNOWFLAKE_CONNECTION_NAME)
    yield conn
    conn.close()


@contextmanager
def universql_connection(**properties) -> SnowflakeConnection:
    """Create a connection through UniversQL proxy."""
    print(f"Reading {CONNECTIONS_FILE} with {properties}")
    connections = CONFIG_MANAGER["connections"]
    if SNOWFLAKE_CONNECTION_NAME not in connections:
        raise pytest.fail(f"Snowflake connection '{SNOWFLAKE_CONNECTION_NAME}' not found in config")
    connection = connections[SNOWFLAKE_CONNECTION_NAME]
    from universql.main import snowflake
    with socketserver.TCPServer(("localhost", 0), None) as s:
        free_port = s.server_address[1]

    def start_universql():
        runner = CliRunner()
        try:
            invoke = runner.invoke(snowflake,
                                   [
                                       '--account', connection.get('account'),
                                       '--port', free_port, '--catalog', 'snowflake',
                                       # AWS_DEFAULT_PROFILE env can be used to pass AWS profile
                                   ],
                                   )
        except Exception as e:
            pytest.fail(e)

        if invoke.exit_code != 0:
            pytest.fail("Unable to start Universql")

    thread = threading.Thread(target=start_universql)
    thread.daemon = True
    thread.start()
    # with runner.isolated_filesystem():
    uni_string = {"host": LOCALHOSTCOMPUTING_COM, "port": free_port} | properties

    try:
        connect = snowflake_connect(connection_name=SNOWFLAKE_CONNECTION_NAME, **uni_string)
        yield connect
    finally:  # Force stop the thread
        connect.close()

@contextmanager
def dynamic_universql_connection(**properties) -> SnowflakeConnection:
    """Create a connection through UniversQL proxy."""
    from universql.main import snowflake
    with socketserver.TCPServer(("localhost", 0), None) as s:
        free_port = s.server_address[1]

    def start_universql():
        runner = CliRunner()
        try:
            invoke = runner.invoke(snowflake,
                               ['--account', properties.get('account'), '--port', free_port, '--catalog',
                                'snowflake'], )
        except Exception as e:
            pytest.fail(e)

        if invoke.exit_code != 0:
            pytest.fail("Unable to start Universql")

    thread = threading.Thread(target=start_universql)
    thread.daemon = True
    thread.start()

    uni_string = {"host": LOCALHOSTCOMPUTING_COM, "port": free_port} | properties
    try:
        connection = snowflake_connect(**uni_string)
        yield connection
    finally:
        connection.close()

def execute_query(conn, query: str) -> pyarrow.Table:
    cur = conn.cursor()
    try:
        cur.execute(query)
        return cur.fetch_arrow_all()
    finally:
        cur.close()


def compare_results(snowflake_result: pyarrow.Table, universql_result: pyarrow.Table):
    # Compare schemas
    if snowflake_result.schema != universql_result.schema:
        schema_diff = []
        for field1, field2 in zip(snowflake_result.schema, universql_result.schema):
            if field1.name != field2.name and field1.type != field2.type:
                schema_diff.append(f"Expected field {field1}, but got {field2}")
        if len(snowflake_result.schema) != len(universql_result.schema):
            schema_diff.append(f"Schema lengths differ: "
                               f"Snowflake={len(snowflake_result.schema)} "
                               f"Universql={len(universql_result.schema)}")
        if len(schema_diff) > 0:
            raise pytest.fail("Schema mismatch:\n" + "\n".join(schema_diff))

    # Compare row counts
    if snowflake_result.num_rows != universql_result.num_rows:
        raise pytest.fail(f"Row count mismatch: Snowflake={snowflake_result.num_rows} "
                          f"Universql={universql_result.num_rows}")

    # Compare data row by row and column by column
    data_diff = []
    for row_index in range(snowflake_result.num_rows):
        for col_index in range(snowflake_result.num_columns):
            value1 = snowflake_result.column(col_index)[row_index].as_py()
            value2 = universql_result.column(col_index)[row_index].as_py()
            if value1 != value2:
                data_diff.append(f"Row {row_index}, Column {col_index}: "
                                 f"Snowflake={value1}, Universql={value2}")

    if data_diff:
        raise pytest.fail("Data mismatch:\n" + "\n".join(data_diff))

    print("Results match perfectly!")


@contextmanager
def cleanup_table(conn, table_name: str):
    """Context manager to ensure table cleanup after tests."""
    try:
        yield
    finally:
        try:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            cur.close()
        except Exception as e:
            print(f"Error during cleanup: {e}")

def generate_name_variants(name, include_blank = False):
    lowercase = name.lower()
    uppercase = name.upper()
    mixed_case = name.capitalize()
    in_quotes = '"' + name.upper() + '"'
    print([lowercase, uppercase, mixed_case, in_quotes])
    return [lowercase, uppercase, mixed_case, in_quotes]

def generate_select_statement_combos(table, schema = None, database = None):
    select_statements = []
    table_variants = generate_name_variants(table)

    if database is not None:
        database_variants = generate_name_variants(database)
        schema_variants = generate_name_variants(schema)
        object_name_combos = product(database_variants, schema_variants, table_variants)
        for db_name, schema_name, table_name in object_name_combos:
            select_statements.append(f"SELECT * FROM {db_name}.{schema_name}.{table_name}")
    else:
        if schema is not None:
            schema_variants = generate_name_variants(schema)
            object_name_combos = product(schema_variants, table_variants)
            for schema_name, table_name in object_name_combos:
                select_statements.append(f"SELECT * FROM {schema_name}.{table_name}")
        else:
            for table_variant in table_variants:
                select_statements.append(f"SELECT * FROM {table_variant}")

    return select_statements

def generate_usql_connection_params(account, user, password, role, database = None, schema = None):
    params = {
        "account": account,
        "user": user,
        "password": password,
        "role": role,
        "warehouse": "local()",
    }
    if database is not None:
        params["database"] = database
    if schema is not None:
        params["schema"] = schema

    return params