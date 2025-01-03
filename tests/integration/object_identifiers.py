import pytest

from tests.integration.utils import execute_query, dynamic_universql_connection
from dotenv import load_dotenv
import os
import logging
from itertools import product

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
        
        tables_not_found = []
        other_errors = []
        with dynamic_universql_connection(**connection_params) as conn:
            for query in all_queries_no_duplicates:
                logger.info(f"current counter: {counter}")
                counter += 1
                try:
                    result = execute_query(conn, query)
                    successful_queries.append(query)
                    continue
                except Exception as e:
                    if str(e).startswith("Unable to find location of Iceberg tables."):
                        tables_not_found.append(query)
                    else:
                        other_errors.append({query: e})
        if len(tables_not_found) > 0 or len(other_errors > 0):
            logger.info(f"When connected to database {database} and schema {schema} the following queries failed:")
            if len(tables_not_found) > 0:
                logger.info(f"Tables unable to be found:")
                for query in tables_not_found:
                    logger.info(query)
            if len(other_errors) > 0:
                logger.info(f"These queries failed for different reasons:")
                for query in tables_not_found:
                    query, error_message = next(iter(query.items()))
                    logger.info(f"Failed Query: {query}")                
                    logger.info(f"Error Message: {error_message}")
            pytest.fail("Queries failed for the above reasons.")                

def generate_name_variants(name, include_blank = False):
    lowercase = name.lower()
    uppercase = name.upper()
    mixed_case = name.capitalize()
    in_quotes = '"' + name.upper() + '"'
    print([lowercase, uppercase, mixed_case, in_quotes])
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

def generate_usql_connection_params(account, user, password, role, database = None, schema = None):
    params = {
        "account": account,
        "user": user,
        "password": password,
        "role": role,
        "warehouse": "local()",
    }
    if database is not None:
        params["database"] = database
    if schema is not None:
        params["schema"] = schema

    return params