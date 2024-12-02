import os
import socketserver
import threading
import time
from contextlib import contextmanager
from typing import Generator, Dict

import pandas as pd
import pytest
from click.testing import CliRunner
from snowflake.connector import connect as snowflake_connect
from snowflake.connector.config_manager import CONFIG_MANAGER

from universql.util import LOCALHOSTCOMPUTING_COM

# Configuration using separate connection strings for direct and proxy connections
# export SNOWFLAKE_CONNECTION_STRING="account=xxx;user=xxx;password=xxx;warehouse=xxx;database=xxx;schema=xxx"
# export UNIVERSQL_CONNECTION_STRING="warehouse=xxx"
SNOWFLAKE_CONNECTION_STRING = os.getenv("SNOWFLAKE_CONNECTION_STRING") or "connection_name=default"
UNIVERSQL_CONNECTION_STRING = os.getenv("UNIVERSQL_CONNECTION_STRING")

# Allow Universql to start
os.environ["MAX_CON_RETRY_ATTEMPTS"] = "100"

def parse_connection_string(conn_string: str) -> Dict[str, str]:
    """Parse connection string into connection parameters."""
    params = {}
    for param in conn_string.split(';'):
        key, value = param.split('=')
        params[key.lower()] = value
    return params


class QueryResult:
    def __init__(self, success: bool, data=None, error=None, execution_time=None):
        self.success = success
        self.data = data
        self.error = error
        self.execution_time = execution_time


@pytest.fixture(scope="session")
def snowflake_connection_params() -> dict:
    """Create a direct connection to Snowflake."""
    if SNOWFLAKE_CONNECTION_STRING is None:
        raise pytest.fail("SNOWFLAKE_CONNECTION_STRING env is required.")
    return parse_connection_string(SNOWFLAKE_CONNECTION_STRING)


@pytest.fixture(scope="session")
def snowflake_connection(snowflake_connection_params) -> Generator:
    conn = snowflake_connect(**snowflake_connection_params)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def universql_connection(snowflake_connection_params) -> Generator:
    """Create a connection through UniversQL proxy."""
    if UNIVERSQL_CONNECTION_STRING is not None:
        uni_string = parse_connection_string(UNIVERSQL_CONNECTION_STRING)
    else:
        connections = CONFIG_MANAGER["connections"]
        connection = connections["default"]
        from universql.main import snowflake
        with socketserver.TCPServer(("localhost", 0), None) as s:
            free_port = s.server_address[1]
        def start_universql():
            runner = CliRunner()
            invoke = runner.invoke(snowflake,
                                   ['--account', connection.get('account'), '--port', free_port, '--catalog',
                                    'snowflake'], )
            if invoke.exit_code != 0:
                pytest.fail("Unable to start Universql")
        thread = threading.Thread(target=start_universql)
        thread.daemon = True
        thread.start()
        # with runner.isolated_filesystem():
        uni_string = {"host": LOCALHOSTCOMPUTING_COM, "port": free_port}

    conn = snowflake_connect(**(snowflake_connection_params | uni_string))
    yield conn
    conn.close()


def execute_query(conn, query: str) -> QueryResult:
    """Execute a query using the provided connection and measure execution time."""
    start_time = time.time()
    try:
        cur = conn.cursor()
        cur.execute(query)

        if query.strip().upper().startswith("SELECT"):
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            df = pd.DataFrame(data, columns=columns)
            return QueryResult(True, data=df, execution_time=time.time() - start_time)

        return QueryResult(True, execution_time=time.time() - start_time)
    except Exception as e:
        return QueryResult(False, error=str(e), execution_time=time.time() - start_time)
    finally:
        cur.close()


def compare_results(snowflake_result: QueryResult, universql_result: QueryResult) -> bool:
    """Compare results from both systems."""
    if not snowflake_result.success or not universql_result.success:
        return pytest.fail("Query is failed")

    if snowflake_result.data is not None and universql_result.data is not None:
        return pd.testing.assert_frame_equal(
            snowflake_result.data,
            universql_result.data,
            check_dtype=False
        ) is None

    return True


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


@pytest.mark.integration
class TestCTAS:
    """Test CREATE TABLE AS SELECT scenarios."""

    def test_basic_ctas(self, snowflake_connection, universql_connection):
        source_table = "test_source_table"
        target_table = "test_ctas_basic"

        setup_query = f"""
        CREATE TABLE {source_table} (
            id INTEGER,
            name STRING,
            value DECIMAL(10,2)
        )
        """

        for conn in [snowflake_connection, universql_connection]:
            execute_query(conn, setup_query)
            execute_query(conn, f"""
                INSERT INTO {source_table} VALUES
                (1, 'Item 1', 10.50),
                (2, 'Item 2', 20.75),
                (3, 'Item 3', 30.25)
            """)

        # Test CTAS
        ctas_query = f"""
        CREATE TABLE {target_table} AS
        SELECT id, name, value * 2 as doubled_value
        FROM {source_table}
        WHERE id > 1
        """

        snowflake_result = execute_query(snowflake_connection, ctas_query)
        universql_result = execute_query(universql_connection, ctas_query)

        assert compare_results(snowflake_result, universql_result)

        # Verify resulting table contents
        verify_query = f"SELECT * FROM {target_table} ORDER BY id"
        snowflake_result = execute_query(snowflake_connection, verify_query)
        universql_result = execute_query(universql_connection, verify_query)

        assert compare_results(snowflake_result, universql_result)

    def test_ctas_with_joins(self, snowflake_connection, universql_connection):
        orders_table = "test_ctas_orders"
        products_table = "test_ctas_products"
        summary_table = "test_ctas_summary"

        # Setup tables
        for conn in [snowflake_connection, universql_connection]:
            execute_query(conn, f"""
                CREATE TABLE {products_table} (
                    product_id INTEGER,
                    product_name STRING,
                    price DECIMAL(10,2)
                )
            """)

            execute_query(conn, f"""
                CREATE TABLE {orders_table} (
                    order_id INTEGER,
                    product_id INTEGER,
                    quantity INTEGER
                )
            """)

            # Insert sample data
            execute_query(conn, f"""
                INSERT INTO {products_table} VALUES
                (1, 'Product A', 10.00),
                (2, 'Product B', 20.00)
            """)

            execute_query(conn, f"""
                INSERT INTO {orders_table} VALUES
                (1, 1, 5),
                (2, 2, 3),
                (3, 1, 2)
            """)

        # Test CTAS with JOIN
        ctas_query = f"""
        CREATE TABLE {summary_table} AS
        SELECT 
            p.product_name,
            COUNT(o.order_id) as total_orders,
            SUM(o.quantity) as total_quantity,
            SUM(o.quantity * p.price) as total_revenue
        FROM {products_table} p
        LEFT JOIN {orders_table} o ON p.product_id = o.product_id
        GROUP BY p.product_name
        """

        snowflake_result = execute_query(snowflake_connection, ctas_query)
        universql_result = execute_query(universql_connection, ctas_query)

        assert compare_results(snowflake_result, universql_result)

        # Verify resulting table
        verify_query = f"SELECT * FROM {summary_table} ORDER BY product_name"
        snowflake_result = execute_query(snowflake_connection, verify_query)
        universql_result = execute_query(universql_connection, verify_query)

        assert compare_results(snowflake_result, universql_result)

    def test_ctas_with_window_functions(self, snowflake_connection, universql_connection):
        source_table = "test_ctas_source_window"
        target_table = "test_ctas_window_result"

        # Setup source table
        for conn in [snowflake_connection, universql_connection]:
            execute_query(conn, f"""
                CREATE TABLE {source_table} (
                    dept STRING,
                    employee STRING,
                    salary INTEGER
                )
            """)

            execute_query(conn, f"""
                INSERT INTO {source_table} VALUES
                ('IT', 'Alice', 80000),
                ('IT', 'Bob', 70000),
                ('HR', 'Carol', 75000),
                ('HR', 'Dave', 85000)
            """)

        # Test CTAS with window functions
        ctas_query = f"""
        CREATE TABLE {target_table} AS
        SELECT 
            dept,
            employee,
            salary,
            AVG(salary) OVER (PARTITION BY dept) as dept_avg,
            salary - AVG(salary) OVER (PARTITION BY dept) as salary_diff,
            RANK() OVER (ORDER BY salary DESC) as overall_rank
        FROM {source_table}
        """

        snowflake_result = execute_query(snowflake_connection, ctas_query)
        universql_result = execute_query(universql_connection, ctas_query)

        assert compare_results(snowflake_result, universql_result)

        # Verify resulting table
        verify_query = f"SELECT * FROM {target_table} ORDER BY overall_rank"
        snowflake_result = execute_query(snowflake_connection, verify_query)
        universql_result = execute_query(universql_connection, verify_query)

        assert compare_results(snowflake_result, universql_result)


@pytest.mark.integration
class TestDataTypes:
    """Test all Snowflake data types."""

    def test_create_temp_all_types(self, snowflake_connection, universql_connection):
        test_table = "test_all_data_types"

        with cleanup_table(snowflake_connection, universql_connection, test_table):
            # Create table with all data types
            create_query = f"""
            CREATE TEMP TABLE {test_table} AS
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
                TO_BINARY(HEX_ENCODE('binary_data')) AS sample_binary,
                TO_BINARY(HEX_ENCODE('varbinary_data'))::VARBINARY AS sample_varbinary,

                -- Logical data types
                TRUE::BOOLEAN AS sample_boolean,

                -- Date & time data types
                '2023-01-01'::DATE AS sample_date,
                '2023-01-01 12:34:56'::DATETIME AS sample_datetime,
                '12:34:56'::TIME AS sample_time,
                '2023-01-01 12:34:56'::TIMESTAMP AS sample_timestamp,
                '2023-01-01 12:34:56 +00:00'::TIMESTAMP_LTZ AS sample_timestamp_ltz,
                '2023-01-01 12:34:56'::TIMESTAMP_NTZ AS sample_timestamp_ntz,
                '2023-01-01 12:34:56 +00:00'::TIMESTAMP_TZ AS sample_timestamp_tz,

                -- Semi-structured data types
                PARSE_JSON('{"key":"value"}')::VARIANT AS sample_variant,
                OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(OBJECT_CONSTRUCT(), 'key1', 'value1'), 
                    'key2', 'value2'), 'key3', 'value3') AS sample_object,
                ARRAY_CONSTRUCT(1, 2, 3, 4) AS sample_array,

                -- Geospatial data types
                TO_GEOGRAPHY('LINESTRING(30 10, 10 30, 40 40)') AS sample_geometry,

                -- Vector data types
                [1.1,2.2,3]::VECTOR(FLOAT,3) AS sample_vector
            """

            # Create table in both systems
            snowflake_result = execute_query(snowflake_connection, create_query)
            universql_result = execute_query(universql_connection, create_query)

            assert compare_results(snowflake_result, universql_result)

            # Test querying and comparing all columns
            verify_query = f"SELECT * FROM {test_table}"
            snowflake_result = execute_query(snowflake_connection, verify_query)
            universql_result = execute_query(universql_connection, verify_query)

            assert compare_results(snowflake_result, universql_result)

            # Test each data type individually
            for column in [
                'sample_number', 'sample_decimal', 'sample_int', 'sample_bigint',
                'sample_smallint', 'sample_tinyint', 'sample_byteint', 'sample_float',
                'sample_double', 'sample_varchar', 'sample_char', 'sample_string',
                'sample_text', 'sample_binary', 'sample_varbinary', 'sample_boolean',
                'sample_date', 'sample_datetime', 'sample_time', 'sample_timestamp',
                'sample_timestamp_ltz', 'sample_timestamp_ntz', 'sample_timestamp_tz',
                'sample_variant', 'sample_object', 'sample_array', 'sample_geometry',
                'sample_vector'
            ]:
                verify_query = f"SELECT {column}, TYPEOF({column}) as type FROM {test_table}"
                snowflake_result = execute_query(snowflake_connection, verify_query)
                universql_result = execute_query(universql_connection, verify_query)

                assert compare_results(snowflake_result, universql_result)

    def test_select_all_types(self, snowflake_connection, universql_connection):
        create_query = """
        SELECT
            -- Numeric conversions
            TO_NUMBER('123.45') as str_to_number,
            TO_DECIMAL('123.45', 10, 2) as str_to_decimal,

            -- String conversions
            TO_VARCHAR(123.45) as num_to_varchar,
            TO_CHAR(CURRENT_DATE()) as date_to_char,

            -- Date/Time conversions
            TO_DATE('2023-01-01') as str_to_date,
            TO_TIMESTAMP('2023-01-01 12:34:56') as str_to_timestamp,
            TO_TIME('12:34:56') as str_to_time,

            -- Boolean conversions
            TO_BOOLEAN('true') as str_to_bool,

            -- Binary conversions
            TO_BINARY('Hello', 'UTF-8') as str_to_binary,

            -- Variant conversions
            TO_VARIANT(123) as num_to_variant,
            TO_OBJECT(PARSE_JSON('{"key":"value"}')) as json_to_object,
            TO_ARRAY(PARSE_JSON('[1,2,3]')) as json_to_array
        """

        snowflake_result = execute_query(snowflake_connection, create_query)
        universql_result = execute_query(universql_connection, create_query)

        assert compare_results(snowflake_result, universql_result)
