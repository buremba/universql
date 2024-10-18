import logging
import time
from urllib.parse import urlparse, parse_qs

import pyarrow
import pyiceberg.table
import sentry_sdk
import sqlglot
from pyiceberg.catalog import PY_CATALOG_IMPL, load_catalog, TYPE
from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError, NoSuchNamespaceError
from pyiceberg.io import PY_IO_IMPL
from sqlglot import ParseError
from sqlglot.expressions import Create

from universql.lake.cloud import CACHE_DIRECTORY_KEY
from universql.util import get_friendly_time_since, \
    prepend_to_lines, parse_compute, QueryError
from universql.warehouse import Executor, Locations
from universql.warehouse.bigquery import BigQueryCatalog
from universql.warehouse.duckdb import DuckDBCatalog
from universql.warehouse.snowflake import SnowflakeCatalog

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("💡")

COMPUTES = {"duckdb": DuckDBCatalog, "local": DuckDBCatalog, "bigquery": BigQueryCatalog, "snowflake": SnowflakeCatalog}


class UniverSQLSession:
    def __init__(self, context, token, credentials: dict, session_parameters: dict):
        self.context = context
        self.credentials = credentials
        self.session_parameters = [{"name": item[0], "value": item[1]} for item in session_parameters.items()]
        self.token = token
        self.compute_plan = parse_compute(self.credentials.get("warehouse"))
        first_catalog_compute = next(filter(lambda x: x.get("name") == 'snowflake', self.compute_plan), {}).get('args')
        self.iceberg_catalog = self._create_iceberg_catalog()
        self.catalog = SnowflakeCatalog(context, self.token, self.credentials, first_catalog_compute,
                                        self.iceberg_catalog)
        self.catalog_executor = self.catalog.executor()
        self.computes = {"snowflake": self.catalog_executor}

        self.last_executor_cursor = None
        self.processing = False

    def _create_iceberg_catalog(self):
        iceberg_catalog = self.context.get('universql_catalog')

        catalog_props = {
            PY_IO_IMPL: "universql.lake.cloud.iceberg",
            # WAREHOUSE: "gs://my-iceberg-data/custom-events/customer_iceberg_pyiceberg",
            CACHE_DIRECTORY_KEY: self.context.get('cache_directory'),
        }

        if iceberg_catalog is not None:
            parsed = urlparse(iceberg_catalog)
            catalog_name = parsed.scheme

            query_params = parse_qs(parsed.query)
            catalog_props = {k: v[0] if len(v) == 1 else v for k, v in query_params.items()}
            catalog_props['namespace'] = parsed.hostname
            catalog_props[TYPE] = "glue"
        else:
            catalog_name = None
            catalog_props |= {
                # pass duck conn
                PY_CATALOG_IMPL: "universql.warehouse.duckdb.DuckDBIcebergCatalog",
                "uri": "duckdb:///:memory:",
            }
        return load_catalog(catalog_name, **catalog_props)

    def _must_run_on_catalog(self, tables, ast):
        queries_that_doesnt_need_warehouse = ["show"]
        if ast.name in queries_that_doesnt_need_warehouse \
                or ast.key in queries_that_doesnt_need_warehouse:
            return True
        for table in tables:
            if (len(table.parts) == 1 and self.credentials.get('schema') == "information_schema"
                    or len(table.parts) > 1 and isinstance(table.parts[-2].this, str) and
                    table.parts[-2].this.lower() == 'information_schema'
                    or len(table.parts) > 2 and isinstance(table.parts[-3].this, str)
                    and table.parts[-3].this.lower() == "snowflake"):
                return True
        return False

    def _do_query(self, start_time: float, raw_query: str) -> pyarrow.Table:
        with sentry_sdk.start_span(op="sqlglot", name="Parsing query"):
            try:
                queries = sqlglot.parse(raw_query, read="snowflake")
            except ParseError as e:
                raise QueryError(f"Unable to parse query with SQLGlot: {e.args}")

        last_executor = None

        if queries is None:
            last_executor = self.perform_query(self.catalog_executor, raw_query)
        else:
            last_error = None
            for ast in queries:
                for compute in self.compute_plan:
                    last_error = None
                    compute_name = compute.get('name')
                    last_executor = self.computes.get(compute_name)
                    if last_executor is None:
                        last_executor = COMPUTES[compute_name](self.context,
                                                               self.token,
                                                               self.credentials,
                                                               compute,
                                                               self.iceberg_catalog).executor()
                        self.computes[compute_name] = last_executor
                    try:
                        last_executor = self.perform_query(last_executor, raw_query, ast=ast)
                        break
                    except QueryError as e:
                        logger.warning(f"Unable to run query on: {e.message}")
                        last_error = e

            performance_counter = time.perf_counter()
            query_duration = performance_counter - start_time

            if last_error is not None:
                raise last_error

            logger.info(
                f"[{self.token}] {last_executor.get_query_log(query_duration)} 🚀 "
                f"({get_friendly_time_since(start_time, performance_counter)})")

        return last_executor.get_as_table()

    def perform_query(self, alternative_executor: Executor, raw_query, ast=None) -> Executor:
        if (ast is not None and alternative_executor.supports(ast) and
                alternative_executor != self.catalog_executor):
            if isinstance(ast, Create):
                tables = list(ast.expression.find_all(sqlglot.exp.Table))
            else:
                tables = list(ast.find_all(sqlglot.exp.Table))

            must_run_on_catalog = self._must_run_on_catalog(tables, ast)
            if not must_run_on_catalog:
                op_name = alternative_executor.__class__.__name__
                with sentry_sdk.start_span(op=op_name, name="Get table paths"):
                    locations = self.get_table_paths(tables)
                with sentry_sdk.start_span(op=op_name, name="Execute query"):
                    new_locations = alternative_executor.execute(ast, locations)
                if new_locations is not None:
                    with sentry_sdk.start_span(op=op_name, name="Register new locations"):
                        self.catalog.register_locations(new_locations)
                return alternative_executor

        with sentry_sdk.start_span(name="Execute query on Snowflake"):
            last_executor = self.catalog_executor
            if ast is None:
                last_executor.execute_raw(raw_query)
            else:
                last_executor.execute(ast, {})
        return last_executor

    def do_query(self, raw_query: str) -> pyarrow.Table:
        start_time = time.perf_counter()
        logger.info(f"[{self.token}] Transpiling query \n{prepend_to_lines(raw_query)}")
        self.processing = True
        try:
            return self._do_query(start_time, raw_query)
        finally:
            self.processing = False

    def close(self):
        self.catalog_executor.close()

    def get_table_paths(self, tables: list[sqlglot.exp.Table]) -> Locations:
        not_existed = []
        cached_tables = {}
        namespace = self.iceberg_catalog.properties.get('namespace', "main")

        for table in tables:
            try:
                table_ref = table.sql(dialect="snowflake")
                logger.info(f"Looking up table {table_ref} in namespace {namespace}")
                iceberg_table = self.iceberg_catalog.load_table((namespace, table_ref))
                cached_tables[table] = iceberg_table
            except NoSuchTableError:
                not_existed.append(table)

        locations = self.catalog.get_table_paths(not_existed)
        for table_ast, table in locations.items():
            if isinstance(table, pyiceberg.table.Table):
                table_name = table_ast.sql(dialect="snowflake")
                try:
                    iceberg_table = self.iceberg_catalog.register_table((namespace, table_name),
                                                                        table.metadata_location)
                except TableAlreadyExistsError:
                    iceberg_table = self.iceberg_catalog.load_table((namespace, table_name))
                except NoSuchNamespaceError:
                    self.iceberg_catalog.create_namespace((namespace,))
                    iceberg_table = self.iceberg_catalog.register_table((namespace, table_name), table.metadata)
                locations[table_ast] = iceberg_table
            else:
                raise Exception(f"Unknown table type {table}")
        return locations | cached_tables
