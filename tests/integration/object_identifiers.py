from itertools import product

import pytest

from tests.integration.utils import execute_query, universql_connection
import os


def generate_name_variants(name):
    lowercase = name.lower()
    uppercase = name.upper()
    mixed_case = name.capitalize()
    in_quotes = '"' + name.upper() + '"'
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
        elif database is not None:
            raise Exception("You must provide a schema name if you provide a database name.")

        if database is not None:
            database_variants = generate_name_variants(database)
            object_name_combos = product(database_variants, schema_variants, table_variants)
            for db_name, schema_name, table_name in object_name_combos:
                set_of_select_statements.append(f"SELECT * FROM {db_name}.{schema_name}.{table_name}")
        select_statements = select_statements + set_of_select_statements

    return select_statements

class TestObjectIdentifiers:
    def test_querying_in_connected_db_and_schema(self):
        external_volume = os.getenv("PYTEST_EXTERNAL_VOLUME")
        if external_volume is None:
            pytest.skip("No external volume provided, set PYTEST_EXTERNAL_VOLUME")

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
        with universql_connection(database=connected_db, schema=connected_schema) as conn:
            execute_query(conn, f""" 
                                CREATE DATABASE IF NOT EXISTS universql1;
                                CREATE DATABASE IF NOT EXISTS universql2;
                                CREATE SCHEMA IF NOT EXISTS universql1.same_schema;
                                CREATE SCHEMA IF NOT EXISTS universql1.different_schema;
                                CREATE SCHEMA IF NOT EXISTS universql2.another_schema;

                                CREATE ICEBERG TABLE IF NOT EXISTS universql1.same_schema.dim_devices("1" int)
                                external_volume = {external_volume}
                                catalog = 'SNOWFLAKE'
                                BASE_LOCATION = 'universql1.same_schema.dim_devices'
                                AS select 1;

                                CREATE ICEBERG TABLE IF NOT EXISTS universql1.different_schema.different_dim_devices("1" int)
                                external_volume = {external_volume}
                                catalog = 'SNOWFLAKE'
                                BASE_LOCATION = 'universql1.different_schema.different_dim_devices'
                                AS select 1;

                                CREATE ICEBERG TABLE IF NOT EXISTS universql2.another_schema.another_dim_devices("1" int)
                                external_volume = {external_volume}
                                catalog = 'SNOWFLAKE'
                                BASE_LOCATION = 'universql2.another_schema.another_dim_devices'
                                AS select 1; 
                            """)

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