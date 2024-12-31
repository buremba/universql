import pytest

from tests.integration.utils import execute_query, universql_connection, SIMPLE_QUERY, ALL_COLUMNS_QUERY


class TestSelect:
    def test_simple_select(self):
        with universql_connection(warehouse="local()") as conn:
            universql_result = execute_query(conn, SIMPLE_QUERY)
            print(universql_result)

    def test_from_stage(self):
        with universql_connection(warehouse="local()") as conn:
            universql_result = execute_query(conn, "select count(*) from @stage/iceberg_stage")
            print(universql_result)

    def test_complex_select(self):
        with universql_connection(warehouse="local()") as conn:
            universql_result = execute_query(conn, ALL_COLUMNS_QUERY)
            print(universql_result)

    def test_in_schema(self):
        with universql_connection(schema="public", warehouse="local()") as conn:
            universql_result = execute_query(conn, "select count(*) from table_in_public_schema")
            print(universql_result)

    def test_qualifiers(self):
        with universql_connection(schema="public", warehouse="local()") as conn:
            universql_result = execute_query(conn, 'select "col1" from "table1"')
            print(universql_result)
