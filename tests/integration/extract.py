from itertools import product

import pytest
from snowflake.connector import ProgrammingError

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

    @pytest.mark.skip(reason="Stages are not implemented yet")
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

    def test_success_after_failure(self):
        with universql_connection(warehouse=None) as conn:
            with pytest.raises(ProgrammingError):
                execute_query(conn, "select * from not_exists")
            result = execute_query(conn, "select 1")
            assert result.num_rows == 1

    def test_union(self):
        with universql_connection(warehouse=None) as conn:
            result = execute_query(conn, "select 1 union all select 2")
            assert result.num_rows == 2