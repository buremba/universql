import pytest

from tests.integration.utils import execute_query, universql_connection, SIMPLE_QUERY


class TestCreate:
    def test_create_iceberg_table(self):
        with universql_connection("local()") as conn:
            universql_result = execute_query(conn, f"""
            CREATE OR REPLACE ICEBERG TABLE test_table
            external_volume = iceberg_external_volume
            catalog = 'SNOWFLAKE'
            BASE_LOCATION = 'dim_devices'
            AS {SIMPLE_QUERY}
            """)
            print(universql_result)

    def test_create_temp_table(self):
        with universql_connection("local()") as conn:
            execute_query(conn, f"CREATE TEMP TABLE test_table AS {SIMPLE_QUERY}")
            execute_query(conn, "SELECT * FROM test_table")

    @pytest.mark.skip(reason="not implemented")
    def test_create_stage(self):
        pass
