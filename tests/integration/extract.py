from itertools import product

import pytest

from tests.integration.utils import execute_query, universql_connection, SIMPLE_QUERY, ALL_COLUMNS_QUERY

def generate_name_variants(name):
    lowercase = name.lower()
    uppercase = name.upper()
    mixed_case = name.capitalize()
    in_quotes = '"' + name.upper() + '"'
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


class TestSelect:
    def test_simple_select(self):
        with universql_connection() as conn:
            universql_result = execute_query(conn, SIMPLE_QUERY)
            print(universql_result)

    def test_from_stage(self):
        with universql_connection() as conn:
            universql_result = execute_query(conn, "select count(*) from @stage/iceberg_stage")
            print(universql_result)

    def test_complex_select(self):
        with universql_connection() as conn:
            universql_result = execute_query(conn, ALL_COLUMNS_QUERY)
            print(universql_result)

    def test_switch_schema(self):
        with universql_connection() as conn:
            execute_query(conn, "USE DATABASE snowflake")
            universql_result = execute_query(conn, "SHOW SCHEMAS")
            assert universql_result.num_rows > 0, f"The query did not return any rows!"

            execute_query(conn, "USE SCHEMA snowflake.account_usage")
            universql_result = execute_query(conn, "SHOW SCHEMAS")
            assert universql_result.num_rows > 0, f"The query did not return any rows!"

            execute_query(conn, "USE snowflake")
            universql_result = execute_query(conn, "SHOW SCHEMAS")
            assert universql_result.num_rows > 0, f"The query did not return any rows!"

            execute_query(conn, "USE snowflake.account_usage")
            universql_result = execute_query(conn, "SHOW SCHEMAS")
            assert universql_result.num_rows > 0, f"The query did not return any rows!"

    def test_in_schema(self):
        with universql_connection(schema="public", warehouse="local()") as conn:
            universql_result = execute_query(conn, "select count(*) from table_in_public_schema")
            print(universql_result)

    def test_qualifiers(self):
        database = "universql1"
        # database = "UNIVERSQL1"
        schema = "same_schema"
        table = "dim_devices"

        fully_qualified_queries = generate_select_statement_combos(table, schema, database)
        no_db_queries = generate_select_statement_combos(table, schema)
        no_schema_queries = generate_select_statement_combos(table)
        all_queries = fully_qualified_queries + no_db_queries + no_schema_queries

        with universql_connection(database=database, schema=schema) as conn:
            for query in all_queries:
                result = execute_query(conn, query)
                print(result.to_pandas())
