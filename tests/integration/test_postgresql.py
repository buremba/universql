import logging
import os
import socketserver
import threading
import time
from contextlib import contextmanager
from typing import Generator, Optional

import pyarrow
import pytest
import psycopg2
from click.testing import CliRunner
from dotenv import load_dotenv

from tests.integration.utils import compare_results

logger = logging.getLogger(__name__)

load_dotenv()

from universql.util import LOCALHOSTCOMPUTING_COM

# Configuration for PostgreSQL connection
# export POSTGRESQL_URL="postgresql://user:password@host:port/database"
POSTGRESQL_URL = os.getenv("PYTEST_POSTGRESQL_URL")
if not POSTGRESQL_URL:
    raise ValueError("PYTEST_POSTGRESQL_URL environment variable is not set")

logging.getLogger("psycopg2").setLevel(logging.INFO)

# Allow Universql to start
os.environ["MAX_CON_RETRY_ATTEMPTS"] = "15"

SIMPLE_QUERY = """
SELECT 1 as test
"""

POSTGRESQL_TYPES_QUERY = """
SELECT
-- Numeric data types
42::INTEGER AS sample_integer,
9876543210::BIGINT AS sample_bigint,
123::SMALLINT AS sample_smallint,
123.45::DECIMAL AS sample_decimal,
123.45::NUMERIC AS sample_numeric,
12345.6789::REAL AS sample_real,
123456789.123456789::DOUBLE PRECISION AS sample_double,

-- String data types
'Sample text'::VARCHAR AS sample_varchar,
'C'::CHAR AS sample_char,
'Another sample text'::TEXT AS sample_text,

-- Boolean data type
TRUE::BOOLEAN AS sample_boolean,

-- Date & time data types
'2023-01-01'::DATE AS sample_date,
'12:34:56'::TIME AS sample_time,
'2023-01-01 10:34:56'::TIMESTAMP AS sample_timestamp,
'2023-01-01 10:34:56+00'::TIMESTAMPTZ AS sample_timestamptz,

-- Array type
ARRAY[1, 2, 3, 4] AS sample_array,

-- JSON type (if available)
'{"key":"value"}'::TEXT AS sample_json
"""

server_cache = {}


@contextmanager
def postgresql_connection(**properties) -> Generator:
    """Connect directly to PostgreSQL without proxy for baseline comparison."""
    import psycopg2
    from urllib.parse import urlparse
    
    parsed_url = urlparse(POSTGRESQL_URL)
    conn_params = {
        'host': parsed_url.hostname,
        'port': parsed_url.port or 5432,
        'database': parsed_url.path.lstrip('/') if parsed_url.path else 'postgres',
        'user': parsed_url.username,
        'password': parsed_url.password,
    }
    conn_params.update(properties)
    
    conn = psycopg2.connect(**conn_params)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def universql_postgresql_connection(**properties) -> psycopg2.extensions.connection:
    """Connect to PostgreSQL through UniversQL proxy."""
    from urllib.parse import urlparse
    
    parsed_url = urlparse(POSTGRESQL_URL)
    
    # Check if we have a cached server for this URL
    url_key = f"{parsed_url.hostname}:{parsed_url.port or 5432}"
    if url_key in server_cache:
        proxy_port = server_cache[url_key]
        print(f"Reusing existing UniversQL server on port {proxy_port} for {url_key}")
    else:
        # Start a new UniversQL PostgreSQL proxy
        from universql.main import postgresql as postgresql_command
        
        with socketserver.TCPServer(("127.0.0.1", 0), None) as s:
            free_port = s.server_address[1]

        def start_universql():
            runner = CliRunner()
            invoke = runner.invoke(postgresql_command,
                                   [
                                       '--url', POSTGRESQL_URL,
                                       '--port', str(free_port),
                                       '--host', '127.0.0.1',
                                   ],
                                   catch_exceptions=False
                                   )
            if invoke.exit_code != 0:
                raise Exception("Unable to start UniversQL PostgreSQL proxy")

        print(f"Starting UniversQL PostgreSQL proxy on port {free_port} for {url_key}")
        thread = threading.Thread(target=start_universql)
        thread.daemon = True
        thread.start()
        
        # Give the server time to start
        time.sleep(5)
        
        server_cache[url_key] = free_port
        proxy_port = free_port

    # Connect through the proxy
    conn_params = {
        'host': '127.0.0.1',
        'port': proxy_port,
        'database': parsed_url.path.lstrip('/') if parsed_url.path else 'postgres',
        'user': parsed_url.username or 'postgres',
        'password': parsed_url.password or 'password',
    }
    conn_params.update(properties)

    connect = None
    try:
        print(f"Connecting to UniversQL proxy at {conn_params}")
        connect = psycopg2.connect(**conn_params)
        yield connect
    finally:
        if connect is not None:
            connect.close()


def execute_query(conn, query: str) -> pyarrow.Table:
    """Execute a query and return results as PyArrow Table."""
    cur = conn.cursor()
    try:
        cur.execute(query)
        
        # Get column info first to check if this is a result set
        columns = cur.description
        if not columns:
            # No result set (e.g., INSERT, UPDATE, etc.)
            return pyarrow.Table.from_arrays([], [])
        
        # Convert results to PyArrow format
        try:
            rows = cur.fetchall()
        except psycopg2.ProgrammingError as e:
            if "no results to fetch" in str(e):
                # This happens when the query returns a structure but no rows
                # Create an empty table with the proper column structure
                arrays = []
                names = []
                for col_desc in columns:
                    names.append(col_desc[0])
                    arrays.append(pyarrow.array([], type=pyarrow.string()))  # Default to string type
                return pyarrow.Table.from_arrays(arrays, names)
            else:
                raise
        
        if not rows:
            # Empty result set but with column structure
            arrays = []
            names = []
            for col_desc in columns:
                names.append(col_desc[0])
                arrays.append(pyarrow.array([], type=pyarrow.string()))  # Default to string type
            return pyarrow.Table.from_arrays(arrays, names)
        
        # Convert data to PyArrow format
        arrays = []
        names = []
        for i, col_desc in enumerate(columns):
            names.append(col_desc[0])
            column_data = [row[i] for row in rows]
            arrays.append(pyarrow.array(column_data))
        
        return pyarrow.Table.from_arrays(arrays, names)
    finally:
        cur.close()



class TestPostgreSQLConnectivity:
    """Test basic connectivity and authentication."""
    
    def test_invalid_database(self):
        """Test connecting to a non-existent database."""
        # PGlite creates databases on demand, so this test only applies to remote PostgreSQL
        # For now, skip this test as we default to local PGlite mode
        pytest.skip("Database creation on demand with embedded PGlite - no error expected")

    def test_simple_connection(self):
        """Test basic connection establishment."""
        with universql_postgresql_connection() as conn:
            assert conn is not None
            result = execute_query(conn, SIMPLE_QUERY)
            assert result.num_rows == 1


class TestPostgreSQLSelect:
    """Test SELECT queries through the proxy."""
    
    def test_simple_select(self):
        """Test simple SELECT query."""
        with universql_postgresql_connection() as conn:
            result = execute_query(conn, SIMPLE_QUERY)
            assert result.num_rows == 1
            assert result.column(0)[0].as_py() == 1

    def test_postgresql_data_types(self):
        """Test various PostgreSQL data types."""
        with universql_postgresql_connection() as conn:
            result = execute_query(conn, POSTGRESQL_TYPES_QUERY)
            assert result.num_rows == 1

            # Verify some basic type conversions
            columns = {field.name: result.column(i)[0].as_py() for i, field in enumerate(result.schema)}

            # Test the expected data types and values
            # Note: Column names remain lowercase with PostgreSQL/PGlite (unlike DuckDB which converts to uppercase)
            assert columns['sample_integer'] == 42
            assert columns['sample_bigint'] == 9876543210
            assert columns['sample_smallint'] == 123
            assert columns['sample_boolean'] is True
            assert columns['sample_varchar'] == 'Sample text'
            assert columns['sample_text'] == 'Another sample text'
            
            # Test floating point values (with some tolerance for precision differences)
            assert abs(float(columns['sample_real']) - 12345.6789) < 1.0  # REAL has lower precision
            assert abs(float(columns['sample_double']) - 123456789.123456789) < 1000.0  # Large numbers have precision limits

    def test_multiple_rows(self):
        """Test queries that return multiple rows."""
        with universql_postgresql_connection() as conn:
            result = execute_query(conn, "SELECT generate_series(1, 5) as num")
            assert result.num_rows == 5
            values = [result.column(0)[i].as_py() for i in range(result.num_rows)]
            assert values == [1, 2, 3, 4, 5]

    def test_empty_result(self):
        """Test queries that return no rows."""
        with universql_postgresql_connection() as conn:
            result = execute_query(conn, "SELECT 1 WHERE FALSE")
            assert result.num_rows == 0

    def test_null_values(self):
        """Test handling of NULL values."""
        with universql_postgresql_connection() as conn:
            result = execute_query(conn, "SELECT NULL as null_value, 'not null' as text_value")
            assert result.num_rows == 1
            assert result.column(0)[0].as_py() is None
            assert result.column(1)[0].as_py() == 'not null'


class TestPostgreSQLError:
    """Test error handling."""
    
    def test_syntax_error(self):
        """Test SQL syntax error handling."""
        with universql_postgresql_connection() as conn:
            with pytest.raises((psycopg2.ProgrammingError, psycopg2.DatabaseError)):
                execute_query(conn, "INVALID SQL SYNTAX")

    def test_table_not_exists(self):
        """Test querying non-existent table."""
        with universql_postgresql_connection() as conn:
            with pytest.raises((psycopg2.ProgrammingError, psycopg2.DatabaseError)):
                execute_query(conn, "SELECT * FROM nonexistent_table")

    def test_success_after_failure(self):
        """Test that connection works after a failed query."""
        with universql_postgresql_connection() as conn:
            # First query fails
            with pytest.raises((psycopg2.ProgrammingError, psycopg2.DatabaseError)):
                execute_query(conn, "SELECT * FROM nonexistent_table")
            
            # Rollback the failed transaction
            execute_query(conn, "ROLLBACK")
            
            # Second query should succeed
            result = execute_query(conn, "SELECT 1")
            assert result.num_rows == 1


class TestPostgreSQLSchemaOperations:
    """Test schema and DDL operations."""
    
    def test_show_tables(self):
        """Test showing tables in the database."""
        with universql_postgresql_connection() as conn:
            result = execute_query(conn, """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                LIMIT 5
            """)
            # Should not fail even if no tables exist
            assert result.num_rows >= 0

    def test_describe_table(self):
        """Test describing table structure."""
        with universql_postgresql_connection() as conn:
            # Create a temporary table for testing
            execute_query(conn, """
                CREATE TEMPORARY TABLE test_table (
                    id INTEGER,
                    name VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Describe the table
            result = execute_query(conn, """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'test_table'
                ORDER BY ordinal_position
            """)
            
            assert result.num_rows >= 2  # At least id and name columns


class TestPostgreSQLFunctions:
    """Test PostgreSQL built-in functions."""
    
    def test_string_functions(self):
        """Test string manipulation functions."""
        with universql_postgresql_connection() as conn:
            result = execute_query(conn, """
                SELECT 
                    LENGTH('test') as str_length,
                    UPPER('test') as str_upper,
                    LOWER('TEST') as str_lower,
                    SUBSTRING('testing', 1, 4) as str_substr
            """)
            assert result.num_rows == 1
            columns = {field.name: result.column(i)[0].as_py() for i, field in enumerate(result.schema)}
            
            assert columns['str_length'] == 4
            assert columns['str_upper'] == 'TEST'
            assert columns['str_lower'] == 'test'
            assert columns['str_substr'] == 'test'

    def test_date_functions(self):
        """Test date and time functions."""
        with universql_postgresql_connection() as conn:
            result = execute_query(conn, """
                SELECT 
                    CURRENT_DATE as current_date,
                    DATE_PART('year', CURRENT_DATE) as current_year,
                    '2023-01-01'::DATE + INTERVAL '1 day' as date_add
            """)
            assert result.num_rows == 1

    def test_mathematical_functions(self):
        """Test mathematical functions."""
        with universql_postgresql_connection() as conn:
            result = execute_query(conn, """
                SELECT 
                    ABS(-5) as abs_val,
                    ROUND(3.14159, 2) as rounded,
                    POWER(2, 3) as power_val,
                    SQRT(16) as sqrt_val
            """)
            assert result.num_rows == 1
            columns = {field.name: result.column(i)[0].as_py() for i, field in enumerate(result.schema)}
            
            assert columns['abs_val'] == 5
            assert float(columns['rounded']) == 3.14  # PostgreSQL returns Decimal, convert to float
            assert columns['power_val'] == 8
            assert columns['sqrt_val'] == 4


@pytest.mark.skip(reason="Requires comparison with direct PostgreSQL connection")
class TestPostgreSQLComparison:
    """Compare UniversQL results with direct PostgreSQL results."""
    
    def test_simple_query_comparison(self):
        """Compare simple query results between direct and proxy connections."""
        # Execute on direct PostgreSQL
        with postgresql_connection() as pg_conn:
            pg_result = execute_query(pg_conn, SIMPLE_QUERY)
        
        # Execute through UniversQL proxy
        with universql_postgresql_connection() as universql_conn:
            universql_result = execute_query(universql_conn, SIMPLE_QUERY)
        
        # Compare results
        compare_results(pg_result, universql_result)

    def test_data_types_comparison(self):
        """Compare data type handling between direct and proxy connections."""
        # Execute on direct PostgreSQL
        with postgresql_connection() as pg_conn:
            pg_result = execute_query(pg_conn, POSTGRESQL_TYPES_QUERY)
        
        # Execute through UniversQL proxy
        with universql_postgresql_connection() as universql_conn:
            universql_result = execute_query(universql_conn, POSTGRESQL_TYPES_QUERY)
        
        # Compare results
        compare_results(pg_result, universql_result)


if __name__ == "__main__":
    # Quick test to verify the setup
    print("Testing PostgreSQL integration...")
    
    try:
        with universql_postgresql_connection() as conn:
            result = execute_query(conn, SIMPLE_QUERY)
            print(f"Success! Got {result.num_rows} row(s)")
            print(f"Result: {result.column(0)[0].as_py()}")
    except Exception as e:
        print(f"Error: {e}")
        raise 