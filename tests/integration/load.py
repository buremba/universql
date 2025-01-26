import os

import pytest
from snowflake.connector import ProgrammingError

from tests.integration.utils import execute_query, universql_connection, SIMPLE_QUERY


class TestCreate:
    def test_create_iceberg_table(self):
        EXTERNAL_VOLUME_NAME = os.getenv("EXTERNAL_VOLUME_NAME")

        with universql_connection(warehouse=None) as conn:
            execute_query(conn, f"""
            CREATE OR REPLACE ICEBERG TABLE test_iceberg_table
            external_volume = {EXTERNAL_VOLUME_NAME}
            catalog = 'SNOWFLAKE'
            BASE_LOCATION = 'test_iceberg_table'
            AS {SIMPLE_QUERY}
            """)
            universql_result = execute_query(conn, f"SELECT * FROM test_iceberg_table LIMIT 1")
            assert universql_result.num_rows == 1

    def test_create_temp_table(self):
        with universql_connection(warehouse=None) as conn:
            execute_query(conn, f"CREATE TEMP TABLE test_temp_table AS {SIMPLE_QUERY}")
            universql_result = execute_query(conn, "SELECT * FROM test_temp_table")
            assert universql_result.num_rows == 1

    # we can potentially run {SIMPLE_QUERY} on DuckDB and then CREATE TABLE on Snowflake with PyArrow (data upload, no processing)
    # but we need a mechanism to analyze the query and make sure it's WORTH running query locally as we need a running warehouse anyways
    def test_create_native_table(self):
        with universql_connection(warehouse=None) as conn:
            with pytest.raises(ProgrammingError, match="DuckDB can't create native Snowflake tables"):
                execute_query(conn, f"CREATE TABLE test_native_table AS {SIMPLE_QUERY}")

    @pytest.mark.skip(reason="not implemented")
    def test_create_stage(self):
        pass
