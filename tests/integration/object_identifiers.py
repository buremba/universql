import pytest

from tests.integration.utils import execute_query, dynamic_universql_connection, SIMPLE_QUERY, generate_select_statement_combos, generate_usql_connection_params
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger(__name__)


class TestObjectIdentifiers:

    load_dotenv()

    account = os.getenv("TEST_ACCOUNT_IDENTIFIER")
    user = os.getenv("TEST_USER")
    password = os.getenv("TEST_PASSWORD")
    role = os.getenv("TEST_ROLE")

    def test_querying_in_connected_db_and_schema(self):
        database = "universql1"
        # database = "UNIVERSQL1"
        schema = "same_schema"
        table = "dim_devices"

        fully_qualified_queries = generate_select_statement_combos(table, schema, database)
        no_db_queries = generate_select_statement_combos(table, schema)
        no_schema_queries = generate_select_statement_combos(table)
        all_queries = fully_qualified_queries + no_db_queries + no_schema_queries
        all_queries_no_duplicates = sorted(list(set(all_queries)))
        for query in all_queries_no_duplicates:
            logger.info(f"{query}: TBE")

        connection_params = generate_usql_connection_params(self.account, self.user, self.password, self.role, database, schema)
        
        with dynamic_universql_connection(**connection_params) as conn:
            for query in all_queries_no_duplicates:
                try:
                    result = execute_query(conn, query)
                    logger.info(f"{query}: SUCCEEDED")
                    logger.info("RESULT:")
                    logger.info(result)
                    continue
                except Exception as e:
                    logger.error(f"{query}: FAILED - {str(e)}")

