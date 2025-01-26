import logging
import os
import socketserver
import threading
from contextlib import contextmanager
from itertools import product
from typing import Generator

import pyarrow
import pytest
from click.testing import CliRunner
from snowflake.connector import connect as snowflake_connect, SnowflakeConnection
from snowflake.connector.config_manager import CONFIG_MANAGER
from snowflake.connector.constants import CONNECTIONS_FILE

logger = logging.getLogger(__name__)

from universql.util import LOCALHOSTCOMPUTING_COM

# Configuration using separate connection strings for direct and proxy connections
# export SNOWFLAKE_CONNECTION_STRING="account=xxx;user=xxx;password=xxx;warehouse=xxx;database=xxx;schema=xxx"
# export UNIVERSQL_CONNECTION_STRING="warehouse=xxx"
SNOWFLAKE_CONNECTION_NAME = os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
logging.getLogger("snowflake.connector").setLevel(logging.INFO)

# Allow Universql to start
os.environ["MAX_CON_RETRY_ATTEMPTS"] = "15"

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

server_cache = {}


@contextmanager
def snowflake_connection(**properties) -> Generator:
    print(f"Reading {CONNECTIONS_FILE} with {properties}")
    snowflake_connection_name = _set_connection_name(properties)
    conn = snowflake_connect(connection_name=snowflake_connection_name, **properties)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def universql_connection(**properties) -> SnowflakeConnection:
    # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#connecting-using-the-connections-toml-file
    print(f"Reading {CONNECTIONS_FILE} with {properties}")
    connections = CONFIG_MANAGER["connections"]
    snowflake_connection_name = _set_connection_name(properties)
    if snowflake_connection_name not in connections:
        raise pytest.fail(f"Snowflake connection '{snowflake_connection_name}' not found in config")
    connection = connections[snowflake_connection_name]
    account = connection.get('account')
    if account in server_cache:
        uni_string = {"host": LOCALHOSTCOMPUTING_COM, "port": server_cache[account]} | properties
    else:
        from universql.main import snowflake
        with socketserver.TCPServer(("localhost", 0), None) as s:
            free_port = s.server_address[1]
        print(f"Reusing existing server running on port {free_port} for account {account}")

        def start_universql():
            runner = CliRunner()
            invoke = runner.invoke(snowflake,
                                   [
                                       '--account', account,
                                       '--port', free_port, '--catalog', 'snowflake',
                                       # AWS_DEFAULT_PROFILE env can be used to pass AWS profile
                                   ],
                                   catch_exceptions=False
                                   )
            if invoke.exit_code != 0:
                raise Exception("Unable to start Universql")

        print(f"Starting running on port {free_port} for account {account}")
        thread = threading.Thread(target=start_universql)
        thread.daemon = True
        thread.start()
        server_cache[account] = free_port
        uni_string = {"host": LOCALHOSTCOMPUTING_COM, "port": free_port} | properties

    connect = None
    try:
        connect = snowflake_connect(connection_name=snowflake_connection_name, **uni_string)
        yield connect
    finally:
        if connect is not None:
            connect.close()


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


def generate_name_variants(name, include_blank=False):
    lowercase = name.lower()
    uppercase = name.upper()
    mixed_case = name.capitalize()
    in_quotes = '"' + name.upper() + '"'
    print([lowercase, uppercase, mixed_case, in_quotes])
    return [lowercase, uppercase, mixed_case, in_quotes]


def generate_select_statement_combos(sets_of_identifiers, connected_db=None, connected_schema=None):
    select_statements = []
    for set in sets_of_identifiers:
        set_of_select_statements = []
        database = set.get("database")
        schema = set.get("schema")
        table = set.get("table")
        if table is not None:
            table_variants = generate_name_variants(table)
            if database == connected_db and schema == connected_schema:
                for table_variant in table_variants:
                    set_of_select_statements.append(f"SELECT * FROM {table_variant}")
        else:
            raise Exception("No table name provided for a select statement combo.")

        if schema is not None:
            schema_variants = generate_name_variants(schema)
            if database == connected_db:
                object_name_combos = product(schema_variants, table_variants)
                for schema_name, table_name in object_name_combos:
                    set_of_select_statements.append(f"SELECT * FROM {schema_name}.{table_name}")
        else:
            if database is not None:
                raise Exception("You must provide a schema name if you provide a database name.")

        if database is not None:
            database_variants = generate_name_variants(database)
            object_name_combos = product(database_variants, schema_variants, table_variants)
            for db_name, schema_name, table_name in object_name_combos:
                set_of_select_statements.append(f"SELECT * FROM {db_name}.{schema_name}.{table_name}")
        select_statements = select_statements + set_of_select_statements
        logger.info(f"database: {database}, schema: {schema}, table: {table}")
        for statement in set_of_select_statements:
            logger.info(statement)
        # logger.info(f"database: {database}, schema: {schema}, table: {table}")

    return select_statements


def _set_connection_name(connection_dict={}):
    snowflake_connection_name = connection_dict.get("snowflake_connection_name", SNOWFLAKE_CONNECTION_NAME)
    logger.info(f"Using the {snowflake_connection_name} connection")
    return snowflake_connection_name

