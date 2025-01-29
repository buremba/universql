import logging
import os
import tempfile
import time
from string import Template
from urllib.parse import urlparse, parse_qs

import pyarrow
import pyiceberg.table
import sentry_sdk
import sqlglot
from pyiceberg.catalog import PY_CATALOG_IMPL, load_catalog, TYPE
from pyiceberg.exceptions import TableAlreadyExistsError, NoSuchNamespaceError
from pyiceberg.io import PY_IO_IMPL
from sqlglot import ParseError
from sqlglot.expressions import Create, Identifier, DDL, Query, Use, Var

from universql.lake.cloud import CACHE_DIRECTORY_KEY, MAX_CACHE_SIZE
from universql.util import get_friendly_time_since, \
    prepend_to_lines, parse_compute, QueryError, full_qualifier, get_profile_for_role
from universql.warehouse import Executor, Tables, ICatalog
from universql.warehouse.bigquery import BigQueryCatalog
from universql.warehouse.duckdb import DuckDBCatalog
from universql.warehouse.snowflake import SnowflakeCatalog
from universql.warehouse.utils import get_stage_name
from pprint import pp

logger = logging.getLogger("💡")

COMPUTES = {"duckdb": DuckDBCatalog, "local": DuckDBCatalog, "bigquery": BigQueryCatalog, "snowflake": SnowflakeCatalog}


class UniverSQLSession:
    def __init__(self, context, session_id, credentials: dict, session_parameters: dict):
        self.context = context
        self.credentials = credentials
        self.session_parameters = [{"name": item[0], "value": item[1]} for item in session_parameters.items()]
        self.session_id = session_id
        self.compute_plan = parse_compute(self.credentials.get("warehouse"))

        snowflake_computes = filter(lambda x: x.get("name") == 'snowflake', self.compute_plan)
        first_catalog_compute = next(snowflake_computes, {}).get('args')
        self.allowed_to_run_compute_on_catalog = first_catalog_compute is not None
        self.iceberg_catalog = self._get_iceberg_catalog()
        self.catalog = SnowflakeCatalog(context, self.session_id, self.credentials, first_catalog_compute or {},
                                        self.iceberg_catalog)
        self.catalog_executor = self.catalog.executor()
        self.computes = {"snowflake": self.catalog_executor}

        self.last_executor_cursor = None
        self.processing = False
        self.metadata_db = None

    def _get_iceberg_catalog(self):
        iceberg_catalog = self.context.get('universql_catalog')

        catalog_props = {
            PY_IO_IMPL: "universql.lake.cloud.iceberg",
            # WAREHOUSE: "gs://my-iceberg-data/custom-events/customer_iceberg_pyiceberg",
            CACHE_DIRECTORY_KEY: self.context.get('cache_directory'),
            MAX_CACHE_SIZE: self.context.get('max_cache_size'),
        }

        if iceberg_catalog is not None:
            parsed = urlparse(iceberg_catalog)
            catalog_name = parsed.scheme

            query_params = parse_qs(parsed.query)
            catalog_props |= {k: v[0] if len(v) == 1 else v for k, v in query_params.items()}
            catalog_props['namespace'] = parsed.hostname
            catalog_props[TYPE] = "glue"
            catalog = load_catalog(catalog_name, **catalog_props)
        else:
            database_path = Template(
                self.context.get('database_path') or f"_{self.session_id}_universql_session").substitute(
                {"session_id": self.session_id}) + ".sqlite"
            catalog_name = "duckdb"
            self.metadata_db = tempfile.NamedTemporaryFile(delete=False, suffix=database_path)

            catalog_props |= {
                # pass duck conn
                PY_CATALOG_IMPL: "pyiceberg.catalog.sql.SqlCatalog",
                "uri": f"sqlite:///{self.metadata_db.name}",
                "namespace": "main",
            }
            catalog = load_catalog(catalog_name, **catalog_props)
            catalog.create_namespace_if_not_exists("main")
        return catalog

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
                logger.info(f"[{self.session_id}] Skipping local execution, found {table.sql()}")
                if not self.allowed_to_run_compute_on_catalog:
                    raise QueryError(
                        f"Set warehouse (SET WAREHOUSE warehouse_name) to query {table.sql()}. Can't run the query outside of Snowflake")
                return True
        return False

    def _do_query(self, start_time: float, raw_query: str) -> pyarrow.Table:
        with sentry_sdk.start_span(op="sqlglot", name="Parsing query"):
            try:
                queries = sqlglot.parse(raw_query, read="snowflake")
            except ParseError as e:
                queries = None
                has_snowflake = any(compute.get('name') == 'snowflake' for compute in self.compute_plan)
                if not has_snowflake:
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
                                                               self.session_id,
                                                               self.credentials,
                                                               compute,
                                                               self.iceberg_catalog,
                                                               self.catalog).executor()
                        self.computes[compute_name] = last_executor
                    try:
                        last_executor = self.perform_query(last_executor, raw_query, ast=ast)
                        break
                    except QueryError as e:
                        logger.warning(f"Unable to run query: {e.message}")
                        last_error = e

            performance_counter = time.perf_counter()
            query_duration = performance_counter - start_time

            if last_error is not None:
                raise last_error

            logger.info(
                f"[{self.session_id}] {last_executor.get_query_log(query_duration)} 🚀 "
                f"({get_friendly_time_since(start_time, performance_counter)})")

        return last_executor.get_as_table()

    def _find_tables(self, ast: sqlglot.exp.Expression, cte_aliases=None):
        if cte_aliases is None:
            cte_aliases = set()
        for expression in ast.walk(bfs=True):
            # process stages first
            if (isinstance(expression, sqlglot.exp.Table) and \
                isinstance(expression.this, Var) and \
                str(expression.this.this).startswith('@')):
                continue
            if isinstance(expression, Query) or isinstance(expression, DDL):
                if expression.ctes is not None and len(expression.ctes) > 0:
                    for cte in expression.ctes:
                        cte_aliases.add(cte.alias)
            if (isinstance(expression, sqlglot.exp.Table) and \
                (isinstance(expression.this, Identifier) or isinstance(expression.this, Var)) \
                and not any(expression == parent.args.get('format') for parent in ast.walk(bfs=True))):
                if expression.catalog or expression.db or str(expression.this.this) not in cte_aliases:
                    yield full_qualifier(expression, self.credentials), cte_aliases

    def _find_files(self, ast: sqlglot.exp.Expression):
        """
        Extracts file information from a Snowflake COPY command's AST.
        
        This function specifically handles Snowflake stage references (prefixed with @) 
        in COPY commands. It processes the 'files' argument of the COPY command and 
        returns structured information about each file source.

        Args:
            ast (sqlglot.exp.Expression): The Abstract Syntax Tree of the SQL query

        Returns:
            List[Dict] | None: A list of dictionaries containing file information with keys:
                - file_qualifier: Full path/identifier of the file
                - type: Type of source (e.g., 'STAGE')
                - source_catalog: Origin catalog (e.g., 'SNOWFLAKE')
                - stage_name: Name of the stage (for stage-based files)
            Returns None if the AST is not a COPY command.

        Notes:
            Currently only supports Snowflake stage references (@stage_name).
            Direct file ingestion is not yet implemented.
        """
        # Ensure the root node is a Copy node
        if isinstance(ast, sqlglot.exp.Copy) == False:
            return None
        
        # Access the files property
        file_nodes = ast.args.get("files", [])
        files = []

        for file_node in file_nodes:
            match = False
            if isinstance(file_node, sqlglot.exp.Table) and isinstance(self.catalog, SnowflakeCatalog):
                if file_node.this.name[0] == '@':
                    files.append({
                        'file_qualifier': full_qualifier(file_node, self.credentials),
                        'type': 'STAGE',
                        'source_catalog': 'SNOWFLAKE',
                        'stage_name': get_stage_name(file_node)
                    })
                    match = True

            if isinstance(file_node, sqlglot.exp.Var):
                print("Ingesting data directly from files is not yet supported.")
                match = True

            if match != True:
                print("Unknown node type in files:", file_node)

        return files
    
    def _get_file_credentials():
        pass

    def perform_query(self, alternative_executor: Executor, raw_query, ast=None) -> Executor:
        if ast is not None and alternative_executor != self.catalog_executor:
            must_run_on_catalog = False
            files_list = None
            processed_file_data = None
            if isinstance(ast, Create):
                if ast.kind in ('TABLE', 'VIEW'):
                    tables = self._find_tables(ast.expression) if ast.expression is not None else []
                else:
                    tables = []
                    must_run_on_catalog = True
            elif isinstance(ast, Use):
                tables = []
            else:
                tables = self._find_tables(ast)
                files_list = self._find_files(ast)
            tables_list = [table[0] for table in tables]
            must_run_on_catalog = must_run_on_catalog or self._must_run_on_catalog(tables_list, ast)
            if not must_run_on_catalog:
                op_name = alternative_executor.__class__.__name__
                if files_list is not None:
                    with sentry_sdk.start_span(op=op_name, name="Get file info"):
                        processed_file_data = self.catalog.get_file_info(files_list, ast)
                        for file_name, file_config in processed_file_data["files"].items():
                            if file_config["storage_provider"] != "Amazon S3":
                                raise Exception("Universql currently only supports Amazon S3 stages.")
                            aws_role = file_config["AWS_ROLE"]
                            file_config["profile"] = get_profile_for_role(aws_role)

                with sentry_sdk.start_span(op=op_name, name="Get table paths"):
                    table_locations = self.get_table_paths_from_catalog(alternative_executor.catalog, tables_list)

                with sentry_sdk.start_span(op=op_name, name="Execute query"):
                    new_table_locations = alternative_executor.execute(ast, table_locations, processed_file_data)

                if new_table_locations is not None:
                    with sentry_sdk.start_span(op=op_name, name="Register new locations"):
                        self.catalog.register_locations(new_table_locations)

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
        logger.info(f"[{self.session_id}] Transpiling query \n{prepend_to_lines(raw_query)}")
        self.processing = True
        try:
            return self._do_query(start_time, raw_query)
        finally:
            self.processing = False

    def close(self):
        self.catalog_executor.close()
        if self.metadata_db is not None:
            self.metadata_db.close()
            if os.path.exists(self.metadata_db.name):
                os.remove(self.metadata_db.name)

    # def get_file_paths_and_credentials_from_catalog(self, alternative_catalog: ICatalog, files_list):
    #     return alternative_catalog.get_file_paths(files_list)

    def get_table_paths_from_catalog(self, alternative_catalog: ICatalog, tables: list[sqlglot.exp.Table]) -> Tables:
        not_existed = []
        cached_tables = {}

        for table in tables:
            full_qualifier_ = full_qualifier(table, self.credentials)
            table_path = alternative_catalog.get_table_paths([full_qualifier_]).get(full_qualifier_, False)
            if table_path is None or isinstance(table_path, pyarrow.Table):
                continue
            # try:
            #     namespace = self.iceberg_catalog.properties.get('namespace')
            #     table_ref = table.sql(dialect="snowflake")
            #     logger.info(f"Looking up table {table_ref} in namespace {namespace}")
            #     iceberg_table = self.iceberg_catalog.load_table((namespace, table_ref))
            #     cached_tables[table] = iceberg_table
            # except NoSuchTableError:
            not_existed.append(table)

        locations = self.catalog.get_table_paths(not_existed)
        for table_ast, table in locations.items():
            if isinstance(table, pyiceberg.table.Table):
                namespace = self.iceberg_catalog.properties.get('namespace')
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
