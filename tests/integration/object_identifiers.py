import pytest

from tests.integration.utils import execute_query, dynamic_universql_connection, SIMPLE_QUERY, generate_select_statement_combos, generate_usql_connection_params
from dotenv import load_dotenv
import os

class TestObjectIdentifiers:

    load_dotenv()

    account = os.getenv("TEST_ACCOUNT_IDENTIFIER")
    user = os.getenv("TEST_USER")
    password = os.getenv("TEST_PASSWORD")

    def test_querying_in_connected_db_and_schema(self):
        database = "universql1"
        schema = "same_schema"
        table = "dim_devices"

        fully_qualified_queries = generate_select_statement_combos(table, schema, database)
        no_db_queries = generate_select_statement_combos(table, schema)
        no_schema_queries = generate_select_statement_combos(table)
        all_queries = fully_qualified_queries + no_db_queries + no_schema_queries
        all_queries_no_duplicates = list(set(all_queries))

        connection_params = generate_usql_connection_params(self.account, self.user, self.password, database, schema)
        
        with dynamic_universql_connection(**connection_params) as conn:
            for query in all_queries_no_duplicates:
                print(query)
                result = execute_query(conn, query)
                print(result)