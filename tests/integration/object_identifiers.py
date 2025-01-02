import pytest

from tests.integration.utils import execute_query, dynamic_universql_connection, SIMPLE_QUERY, generate_select_statement_combos, generate_usql_connection_params
from dotenv import load_dotenv
import os
import logging
import time

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
        successful_queries = []
        failed_queries = []
        counter = 0

        connection_params = generate_usql_connection_params(self.account, self.user, self.password, self.role, database, schema)
        for query in all_queries_no_duplicates:
            logger.info(f"current counter: {counter}")
            counter += 1
            # if counter > 20:
            #     break
            # without the break there are connection refused errors starting at 23.  if I add a sleep statement it waits until 25
            # output is below in comments
            with dynamic_universql_connection(**connection_params) as conn:
                try:
                    result = execute_query(conn, query)
                    successful_queries.append(query)
                    continue
                except Exception as e:
                    failed_queries.append({query: f"FAILED - {str(e)}"})
        logger.info("test_querying_in_connected_db_and_schema")
        logger.info("Successful Queries:")
        for query in successful_queries:
            logger.info(query)
        logger.info("Failed Queries:")
        for query in successful_queries:
            logger.info(failed_queries)

        # WARNING  snowflake.connector.vendored.urllib3.connectionpool:connectionpool.py:824 Retrying (Retry(total=0, connect=None, read=None, redirect=None, status=None)) after connection broken by 'NewConnectionError('<snowflake.connector.vendored.urllib3.connection.HTTPSConnection object at 0x14d169670>: Failed to establish a new connection: [Errno 61] Connection refused')': /session/v1/login-request?request_id=b806a1b2-0462-4d76-a9c8-348981837587&databaseName=universql1&schemaName=same_schema&warehouse=local%28%29&roleName=general_purpose
        # WARNING  🧵:snowflake.py:387 Failed to set signal handler for SIGINT: signal only works in main thread of the main interpreter
        # WARNING  snowflake.connector.vendored.urllib3.connectionpool:connectionpool.py:824 Retrying (Retry(total=0, connect=None, read=None, redirect=None, status=None)) after connection broken by 'NewConnectionError('<snowflake.connector.vendored.urllib3.connection.HTTPSConnection object at 0x14d147680>: Failed to establish a new connection: [Errno 61] Connection refused')': /session/v1/login-request?request_id=4171a507-63b3-4349-85d1-4a27a68650a1&databaseName=universql1&schemaName=same_schema&warehouse=local%28%29&roleName=general_purpose
        # WARNING  snowflake.connector.vendored.urllib3.connectionpool:connectionpool.py:824 Retrying (Retry(total=0, connect=None, read=None, redirect=None, status=None)) after connection broken by 'NewConnectionError('<snowflake.connector.vendored.urllib3.connection.HTTPSConnection object at 0x14d168890>: Failed to establish a new connection: [Errno 61] Connection refused')': /session/v1/login-request?request_id=ff44232e-b3a0-42f4-8a56-2d7f9d37747d&databaseName=universql1&schemaName=same_schema&warehouse=local%28%29&roleName=general_purpose
        # keeps repeating