import pytest

from tests.integration.utils import execute_query, universql_connection, SIMPLE_QUERY


class TestCreate:
    def test_create_iceberg_table(self):
        with universql_connection(snowflake_connection_name="jinjat_aws_us_east", warehouse=None) as conn:
            execute_query(conn, f"""
            CREATE OR REPLACE ICEBERG TABLE test_iceberg_table
            external_volume = ICEBERG_JINJAT
            catalog = 'SNOWFLAKE'
            BASE_LOCATION = 'dim_devices'
            AS {SIMPLE_QUERY}
            """)
            universql_result = execute_query(conn, f"SELECT * FROM test_iceberg_table LIMIT 1")
            print(universql_result)

    def test_create_temp_table(self):
        with universql_connection(warehouse=None) as conn:
            execute_query(conn, f"CREATE TEMP TABLE test_table AS {SIMPLE_QUERY}")
            execute_query(conn, "SELECT * FROM TEST_TABLE")

    @pytest.mark.skip(reason="not implemented")
    def test_create_stage(self):
        pass
