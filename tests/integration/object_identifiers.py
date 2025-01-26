import pytest

from tests.integration.utils import execute_query, universql_connection, snowflake_connection, generate_select_statement_combos
from dotenv import load_dotenv
import os

class TestObjectIdentifiers:
    def test_setup(self):
        EXTERNAL_VOLUME_NAME = os.getenv("EXTERNAL_VOLUME_NAME")

        with snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(, f"""
            execute immediate $$     
            begin   
            CREATE DATABASE IF NOT EXISTS universql1;
            CREATE DATABASE IF NOT EXISTS universql2;
            CREATE SCHEMA IF NOT EXISTS universql1.same_schema;
            CREATE SCHEMA IF NOT EXISTS universql1.different_schema;
            CREATE SCHEMA IF NOT EXISTS universql2.another_schema;

            CREATE ICEBERG TABLE IF NOT EXISTS universql1.same_schema.dim_devices("1" int)
            external_volume = {EXTERNAL_VOLUME_NAME}
            catalog = 'SNOWFLAKE'
            BASE_LOCATION = 'universql1.same_schema.dim_devices'
            AS select 1;

            CREATE ICEBERG TABLE IF NOT EXISTS universql1.different_schema.different_dim_devices("1" int)
            external_volume = {EXTERNAL_VOLUME_NAME}
            catalog = 'SNOWFLAKE'
            BASE_LOCATION = 'universql1.different_schema.different_dim_devices'
            AS select 1;

            CREATE ICEBERG TABLE IF NOT EXISTS universql2.another_schema.another_dim_devices("1" int)
            external_volume = {EXTERNAL_VOLUME_NAME}
            catalog = 'SNOWFLAKE'
            BASE_LOCATION = ' universql2.another_schema.another_dim_devices'
            AS select 1;
            end;
            $$  
        """,


  # requires the following:
    # a connection's file ~/.snowflake/connections.toml
    # a connection in that file called "integration_test_universql" specifying that the warehouse is none
    # the connected user must be the same as for test_setup
    def test_querying_in_connected_db_and_schema(self):
        connected_db = "universql1"
        connected_schema = "same_schema"

        combos = [
            {
                "database": "universql1",
                "schema": "same_schema",
                "table": "dim_devices"
            },
            {
                "database": "universql1",
                "schema": "different_schema",
                "table": "different_dim_devices"
            },
            {
                "database": "universql2",
                "schema": "another_schema",
                "table": "another_dim_devices"
            },
        ]

        select_statements = generate_select_statement_combos(combos, connected_db, connected_schema)
        successful_queries = []
        failed_queries = []
        
        # create toml file
        with universql_connection(database=connected_db, schema=connected_schema) as conn:
            for query in select_statements:
                try:
                    execute_query(conn, query)
                    successful_queries.append(query)
                    continue
                except Exception as e:
                    failed_queries.append(f"{query} | FAILED - {str(e)}")
        if len(failed_queries) > 0:
            error_message = f"The following {len(failed_queries)} queries failed:"
            for query in failed_queries:
                error_message = f"{error_message}\n{query}"
            pytest.fail(error_message)
