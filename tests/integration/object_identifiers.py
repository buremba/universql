import pytest

from tests.integration.utils import execute_query, dynamic_universql_connection
from dotenv import load_dotenv
import os
import logging
from itertools import product
from _pytest.reports import TestReport

logger = logging.getLogger(__name__)

class TestObjectIdentifiers:

    load_dotenv()

    account = os.getenv("TEST_ACCOUNT_IDENTIFIER")
    user = os.getenv("TEST_USER")
    password = os.getenv("TEST_PASSWORD")
    role = os.getenv("TEST_ROLE")

    def test_run_all_tests(self):
        tests = [
            _format_test_params("universql1", "same_schema", "dim_devices", "universql1", "same_schema"),
            _format_test_params("universql1", "same_schema", "dim_devices", "UNIVERSQL1", "same_schema"),
            _format_test_params("universql1", "same_schema", "dim_devices", "UNIVERSQL1", "SAME_SCHEMA"),
            _format_test_params("universql1", "same_schema", "dim_devices", "universql1", "SAME_SCHEMA"),
            _format_test_params("universql1", "different_schema", "dim_devices", "universql1", "same_schema"),
            _format_test_params("universql1", "different_schema", "dim_devices", "UNIVERSQL1", "same_schema"),
            _format_test_params("universql1", "different_schema", "dim_devices", "UNIVERSQL1", "SAME_SCHEMA"),
            _format_test_params("universql1", "different_schema", "dim_devices", "universql1", "SAME_SCHEMA"),
            _format_test_params("universql2", "another_schema", "dim_devices", "universql1", "same_schema"),
            _format_test_params("universql2", "another_schema", "dim_devices", "UNIVERSQL1", "same_schema"),
            _format_test_params("universql2", "another_schema", "dim_devices", "UNIVERSQL1", "SAME_SCHEMA"),
            _format_test_params("universql2", "another_schema", "dim_devices", "universql1", "SAME_SCHEMA"),
        ]

        failed_tests = []
        for test in tests:
            failures = self.run_test_queries(test["table_db"], test["table_schema"], test["table_name"], test["connected_db"], test["connected_schema"], )
            if failures is not None:
                failed_tests.append(failures)

        if len(failed_tests) > 0:
            error_messages_array = []
            for failure in failed_tests:
                error_message_array = []
                error_message_array.append(f"Connection to database='{failure["connected_db"]}', schema='{failure["connected_schema"]}':")
                if len(failure["tables_not_found"]) > 0:
                    error_message_array.append(f"-Tables not found ({len(failure["tables_not_found"])} queries):")
                    for unfound_table_query in failure["tables_not_found"]:
                        error_message_array.append(f"  * {unfound_table_query}")
                if len(failure["other_errors"]) > 0:
                    error_message_array.append(f"-Other errors ({len(failure["other_errors"])} queries):")
                    for other_error_query in failure["other_errors"]:
                        query, error_message = next(iter(other_error_query.items()))
                        error_message_array.append(f"  * Query: {query}")
                        error_message_array.append(f"    Error: {error_message}")
                formatted_error_message = "\n".join(error_message_array)
                error_messages_array.append(formatted_error_message)
            formatted_error_messages = "\n\n".join(error_messages_array)
            logger.info(formatted_error_messages)
            pytest.fail(formatted_error_messages)            
       
    def run_test_queries(self, table_db, table_schema, table_name, connected_db, connected_schema):
        fully_qualified_queries = _generate_select_statement_combos(table_name, table_schema, table_db)
        all_queries = fully_qualified_queries
        if connected_db == table_db:
            no_db_queries = _generate_select_statement_combos(table_name, table_schema)
            all_queries = all_queries + no_db_queries
            if connected_schema == table_schema:
                no_schema_queries = _generate_select_statement_combos(table_name)
                all_queries = all_queries + no_schema_queries
        all_queries_no_duplicates = sorted(list(set(all_queries)))
        successful_queries = []
        counter = 0

        connection_params = _generate_usql_connection_params(self.account, self.user, self.password, self.role, connected_db, connected_schema)
        
        tables_not_found = []
        other_errors = []
        with dynamic_universql_connection(**connection_params) as conn:
            for query in all_queries_no_duplicates:
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
        if len(tables_not_found) > 0 or len(other_errors) > 0:
            failures_overview = {
                "connected_db": connected_db,
                "connected_schema": connected_schema,
                "tables_not_found": tables_not_found,
                "other_errors": other_errors
            }
            return failures_overview
        else:
            logger.info(f"Connection to database='{connected_db}', schema='{connected_schema}': All queries PASSED")
            return None   

def _generate_name_variants(name, include_blank = False):
    lowercase = name.lower()
    uppercase = name.upper()
    mixed_case = name.capitalize()
    in_quotes = '"' + name.upper() + '"'
    print([lowercase, uppercase, mixed_case, in_quotes])
    return [lowercase, uppercase, mixed_case, in_quotes]

def _generate_select_statement_combos(table, schema = None, database = None):
    select_statements = []
    table_variants = _generate_name_variants(table)

    if database is not None:
        database_variants = _generate_name_variants(database)
        schema_variants = _generate_name_variants(schema)
        object_name_combos = product(database_variants, schema_variants, table_variants)
        for db_name, schema_name, table_name in object_name_combos:
            select_statements.append(f"SELECT * FROM {db_name}.{schema_name}.{table_name}")
    else:
        if schema is not None:
            schema_variants = _generate_name_variants(schema)
            object_name_combos = product(schema_variants, table_variants)
            for schema_name, table_name in object_name_combos:
                select_statements.append(f"SELECT * FROM {schema_name}.{table_name}")
        else:
            for table_variant in table_variants:
                select_statements.append(f"SELECT * FROM {table_variant}")

    return select_statements

def _generate_usql_connection_params(account, user, password, role, database = None, schema = None):
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


def _format_test_params(table_db, table_schema, table_name, connected_db, connected_schema):
    return {
        "table_db": table_db,
        "table_schema": table_schema,
        "table_name": table_name,
        "connected_db": connected_db,
        "connected_schema": connected_schema,
    }