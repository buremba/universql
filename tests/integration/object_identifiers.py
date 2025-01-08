import pytest
import snowflake.connector

from tests.integration.utils import execute_query, dynamic_universql_connection, universql_connection, snowflake_connection, SIMPLE_QUERY, generate_select_statement_combos, generate_usql_connection_params
from dotenv import load_dotenv
import os
import logging
import time
from pprint import pp

logger = logging.getLogger(__name__)

class TestObjectIdentifiers:

    load_dotenv()

    ACCOUNT = os.getenv("TEST_ACCOUNT_IDENTIFIER")
    TEST_USER = os.getenv("TEST_USER")
    TEST_USER_PASSWORD = os.getenv("TEST_PASSWORD")
    TEST_ROLE = os.getenv("TEST_ROLE")

    STORAGE_LOCATION_NAME = os.getenv("STORAGE_LOCATION_NAME")
    STORAGE_PROVIDER = os.getenv("STORAGE_PROVIDER")
    STORAGE_AWS_ROLE_ARN = os.getenv("STORAGE_AWS_ROLE_ARN")
    STORAGE_BASE_URL = os.getenv("STORAGE_BASE_URL")
    STORAGE_AWS_EXTERNAL_ID = os.getenv("STORAGE_AWS_EXTERNAL_ID")
    EXTERNAL_VOLUME_NAME = os.getenv("EXTERNAL_VOLUME_NAME")
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    TEST_WAREHOUSE = os.getenv("TEST_WAREHOUSE")

    def test_setup(self):
        raw_query = f"""
            -- CREATE OBJECTS
            USE ROLE ACCOUNTADMIN;
            CREATE OR REPLACE DATABASE universql1;
            CREATE OR REPLACE DATABASE universql2;
            CREATE SCHEMA IF NOT EXISTS universql1.same_schema;
            CREATE SCHEMA IF NOT EXISTS universql1.different_schema;
            CREATE SCHEMA IF NOT EXISTS universql2.another_schema;
            CREATE EXTERNAL VOLUME IF NOT EXISTS {self.EXTERNAL_VOLUME_NAME}
            STORAGE_LOCATIONS = (
                (
                NAME = '{self.STORAGE_LOCATION_NAME}',
                STORAGE_PROVIDER = '{self.STORAGE_PROVIDER}',
                STORAGE_AWS_ROLE_ARN = '{self.STORAGE_AWS_ROLE_ARN}',
                STORAGE_BASE_URL = '{self.STORAGE_BASE_URL}',
                STORAGE_AWS_EXTERNAL_ID = '{self.STORAGE_AWS_EXTERNAL_ID}'
                )
            )
            ALLOW_WRITES = TRUE;

            CREATE OR REPLACE ICEBERG TABLE universql1.same_schema.dim_devices
            external_volume = {self.EXTERNAL_VOLUME_NAME}
            catalog = 'SNOWFLAKE'
            BASE_LOCATION = 's3://{self.BUCKET_NAME}/tests/1/same_schema/dim_devices'
            AS select 1;

            CREATE OR REPLACE ICEBERG TABLE universql1.different_schema.different_dim_devices
            external_volume = {self.EXTERNAL_VOLUME_NAME}
            catalog = 'SNOWFLAKE'
            BASE_LOCATION = 's3://{self.BUCKET_NAME}/tests/1/different_schema/different_dim_devices'
            AS select 1;

            CREATE OR REPLACE ICEBERG TABLE universql2.another_schema.another_dim_devices
            external_volume = {self.EXTERNAL_VOLUME_NAME}
            catalog = 'SNOWFLAKE'
            BASE_LOCATION = 's3://{self.BUCKET_NAME}/tests/2/another_schema/another_dim_devices'
            AS select 1;

            CREATE OR REPLACE ROLE {self.TEST_ROLE};
            GRANT ROLE {self.TEST_ROLE} TO USER {self.TEST_USER};

            GRANT USAGE ON EXTERNAL VOLUME {self.EXTERNAL_VOLUME_NAME} TO ROLE {self.TEST_ROLE};
            GRANT USAGE ON DATABASE universql1 TO ROLE {self.TEST_ROLE};
            GRANT USAGE ON DATABASE universql2 TO ROLE {self.TEST_ROLE};
            GRANT USAGE ON ALL SCHEMAS IN DATABASE universql1 TO ROLE {self.TEST_ROLE};
            GRANT USAGE ON ALL SCHEMAS IN DATABASE universql2 TO ROLE {self.TEST_ROLE};
            GRANT SELECT ON universql1.same_schema.dim_devices TO ROLE {self.TEST_ROLE};
            GRANT SELECT ON universql1.different_schema.different_dim_devices TO ROLE {self.TEST_ROLE};
            GRANT SELECT ON universql2.another_schema.another_dim_devices TO ROLE {self.TEST_ROLE};
            GRANT USAGE ON WAREHOUSE {self.TEST_WAREHOUSE} TO ROLE {self.TEST_ROLE};
            
            USE ROLE {self.TEST_ROLE};
            USE DATABASE universql1;
            SELECT * FROM universql1.same_schema.dim_devices;
            SELECT * FROM universql1.different_schema.different_dim_devices;
            SELECT * FROM universql2.another_schema.another_dim_devices;
        """

        queries = raw_query.split(";")
        # connection_params = generate_usql_connection_params(self.ACCOUNT, self.TEST_USER, self.TEST_USER_PASSWORD, 'ACCOUNTADMIN')
        # connection_params["warehouse"] = self.TEST_WAREHOUSE
        connection_params = {}
        connection_params["snowflake_connection_name"] = "integration_test_snowflake_direct"
        with snowflake_connection(**connection_params) as conn:
            cursor = conn.cursor()
            failed_queries = []
            for query in queries:
                try:
                    cursor.execute(query)
                    logger.info(query)
                except Exception as e:
                    failed_queries.append(f"{query} | FAILED - {str(e)}")
                    logger.info(f"{query} | FAILED - {str(e)}")
            if len(failed_queries) > 0:
                error_message = "The following queries failed:"
                for query in failed_queries:
                    error_message = error_message + "\n{query}"
                raise Exception(error_message)

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
        
        
        # no_db_queries = generate_select_statement_combos(table, schema)
        # no_schema_queries = generate_select_statement_combos(table)
        # all_queries = fully_qualified_queries + no_db_queries + no_schema_queries
        # all_queries_no_duplicates = sorted(list(set(all_queries)))
        # for query in select_statements:
        #     logger.info(f"{query}: TBE")
        successful_queries = []
        failed_queries = []
        counter = 0

        connection_params = {
            "snowflake_connection_name": "integration_test_universql",
            "database": connected_db,
            "schema": connected_schema
        }
        
        # create toml file
        with universql_connection(**connection_params) as conn:
            for query in select_statements:
                logger.info(f"current counter: {counter}")
                counter += 1
                # if counter > 20:
                #     break
                # without the break there are connection refused errors starting at 23.  if I add a sleep statement it waits until 25
                # output is below in comments
                try:
                    result = execute_query(conn, query)
                    successful_queries.append(query)
                    logger.info(f"QUERY PASSED: {query}")
                    logger.info(result)
                    continue
                except Exception as e:
                    logger.info(f"QUERY FAILED: {query}")
                    failed_queries.append(f"{query} | FAILED - {str(e)}")
        logger.info("test_querying_in_connected_db_and_schema")
        logger.info("Successful Queries:")
        for query in successful_queries:
            logger.info(query)
        if len(failed_queries) > 0:
            error_message = f"The following {len(failed_queries)} queries failed:"
            for query in failed_queries:
                error_message = f"{error_message}\n{query}"
            logger.error(error_message)
            raise Exception(error_message)