from tests.integration.utils import execute_query, universql_connection, snowflake_connection, generate_select_statement_combos
import os
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)


class TestFileConversions:

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

    # def test_setup(self):
    #     file_format = "test_format"
    #     create_csv_stage_query = "create or replace stage file format universql1.same_schema.test_format type=CSV;"
    #     create_csv_stage_query = "CREATE OR REPLACE STAGE universql1.same_schema.test_stage "
    #     connection_params = {}
    #     connection_params["snowflake_connection_name"] = "integration_test_snowflake_direct"

    #     with snowflake_connection(**connection_params) as conn:
    #         try:
    #             cursor = conn.cursor()
    #             cursor.execute(create_csv_stage_query)
    #         except Exception as e:
    #             error = f"Test file conversions setup was unsuccessful: {e}"
    #             logger.error(error)
    #             raise Exception(error)
    #         finally:
    #             cursor.close()

    def test_files(self):
        queries = [
    """CREATE OR REPLACE TEMPORARY TABLE stg_device_metadata (
                device_id VARCHAR,
                device_name VARCHAR,
                device_type VARCHAR,
                manufacturer VARCHAR,
                model_number VARCHAR,
                firmware_version VARCHAR,
                installation_date DATE,
                location_id VARCHAR,
                location_name VARCHAR,
                facility_zone VARCHAR,
                is_active BOOLEAN,
                expected_lifetime_months INT,
                maintenance_interval_days INT,
                last_maintenance_date DATE
            );
    """,
    """COPY INTO stg_device_metadata
        FROM @iceberg_db.public.landing_stage/initial_objects/device_metadata.csv
        FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
    """
        ]

        connected_db = 'universql1'
        connected_schema = 'same_schema'
        connection_params = {
                "snowflake_connection_name": "integration_test_universql",
                "database": connected_db,
                "schema": connected_schema
        }

        with universql_connection(**connection_params) as conn:
            for query in queries:
                # if counter > 20:
                #     break
                # without the break there are connection refused errors starting at 23.  if I add a sleep statement it waits until 25
                # output is below in comments
                try:
                    result = execute_query(conn, query)
                    logger.info(f"QUERY PASSED: {query}")
                    logger.info(result)
                    continue
                except Exception as e:
                    error = f"QUERY FAILED: {query}\n{e}"
                    logger.error(error)
                    raise Exception(e)
