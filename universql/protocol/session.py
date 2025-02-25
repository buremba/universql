import logging
from universql.lake.fsspec_util import get_friendly_disk_usage
import tempfile
import time
from string import Template
from traceback import print_exc
from typing import List
from urllib.parse import urlparse, parse_qs
import os
import signal
import threading
import pyarrow
import pyiceberg.table
import sentry_sdk
import sqlglot
from pyiceberg.catalog import PY_CATALOG_IMPL, load_catalog, TYPE
from pyiceberg.exceptions import TableAlreadyExistsError, NoSuchNamespaceError
from pyiceberg.io import PY_IO_IMPL
from sqlglot import ParseError
from sqlglot.expressions import Create, Identifier, DDL, Query, Use, Semicolon, Copy

from universql.lake.cloud import CACHE_DIRECTORY_KEY, MAX_CACHE_SIZE
from universql.util import get_friendly_time_since, \
    prepend_to_lines, QueryError, full_qualifier, current_context
from universql.plugin import Executor, Tables, ICatalog, COMPUTES, PLUGINS, UniversqlPlugin, UQuery

logger = logging.getLogger("💡")
sessions = {}


class UniverSQLSession:
    def __init__(self, context, session_id, credentials: dict, session_parameters: dict):
        self.context = context
        self.credentials = credentials
        self.session_parameters = [{"name": item[0], "value": item[1]} for item in session_parameters.items()]
        self.session_id = session_id
        self.iceberg_catalog = self._get_iceberg_catalog()
        self.catalog = COMPUTES["snowflake"](self)
        self.target_compute = COMPUTES["duckdb"](self)
        self.catalog_executor = self.catalog.executor()
        self.processing = False
        self.metadata_db = None
        self.plugins: List[UniversqlPlugin] = [plugin(self) for plugin in PLUGINS]
        self.query_history = []

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
        return ast.name in queries_that_doesnt_need_warehouse \
                or ast.key in queries_that_doesnt_need_warehouse

    def _do_query(self, start_time: float, raw_query: str) -> pyarrow.Table:
        with sentry_sdk.start_span(op="sqlglot", name="Parsing query"):
            try:
                queries = sqlglot.parse(raw_query, read="snowflake")
            except ParseError as e:
                queries = None
                raise QueryError(f"Unable to parse query with SQLGlot: {e.args}")

        last_executor = None

        plugin_hooks = []
        for plugin in self.plugins:
            try:
                plugin_hooks.append(plugin.start_query(queries, raw_query))
            except Exception as e:
                print_exc(10)
                message = f"Unable to call start_query on plugin {plugin.__class__}"
                logger.error(message, exc_info=e)
                raise QueryError(f"{message}: {str(e)}")

        if queries is None:
            last_executor = self.perform_query(self.catalog_executor, raw_query, plugin_hooks)
        else:
            last_error = None

            for ast in queries:
                if isinstance(ast, Semicolon) and ast.this is None:
                    continue
                last_executor = self.target_compute.executor()
                last_executor = self.perform_query(last_executor, raw_query, plugin_hooks, ast=ast)

            performance_counter = time.perf_counter()
            query_duration = performance_counter - start_time

            if last_error is not None:
                raise last_error

            logger.info(
                f"[{self.session_id}] {last_executor.get_query_log(query_duration)} 🚀 "
                f"({get_friendly_time_since(start_time, performance_counter)})")

        table = last_executor.get_as_table()
        for hook in plugin_hooks:
            try:
                hook.end(table)
            except Exception as e:
                print_exc(10)
                message = f"Unable to end query execution on plugin {hook.__class__}"
                logger.error(message, exc_info=e)
                raise QueryError(f"{message}: {str(e)}")
        return table

    def _find_tables(self, ast: sqlglot.exp.Expression, cte_aliases=None):
        if cte_aliases is None:
            cte_aliases = set()
        for expression in ast.walk(bfs=True):
            if isinstance(expression, Query) or isinstance(expression, DDL):
                if expression.ctes is not None and len(expression.ctes) > 0:
                    for cte in expression.ctes:
                        cte_aliases.add(cte.alias)
            if isinstance(expression, sqlglot.exp.Table) and isinstance(expression.this, Identifier):
                if expression.catalog or expression.db or str(expression.this.this) not in cte_aliases:
                    yield full_qualifier(expression, self.credentials), cte_aliases

    def perform_query(self, alternative_executor: Executor, raw_query, plugin_hooks: List[UQuery],
                      ast=None) -> Executor:
        if ast is not None and alternative_executor != self.catalog_executor:
            must_run_on_catalog = False
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
            tables_list = [table[0] for table in tables]
            must_run_on_catalog = must_run_on_catalog or self._must_run_on_catalog(tables_list, ast)
            if not must_run_on_catalog:
                op_name = alternative_executor.__class__.__name__
                with sentry_sdk.start_span(op=op_name, name="Get table paths"):
                    locations = self.get_table_paths_from_catalog(alternative_executor.catalog, tables_list)
                with sentry_sdk.start_span(op=op_name, name="Execute query"):
                    current_ast = ast
                    for plugin_hook in plugin_hooks:
                        try:
                            current_ast = plugin_hook.transform_ast(ast, alternative_executor)
                        except Exception as e:
                            print_exc(10)
                            message = f"Unable to tranform_ast on plugin {plugin_hook.__class__}"
                            logger.error(message, exc_info=e)
                            raise QueryError(f"{message}: {str(e)}")
                    new_locations = alternative_executor.execute(current_ast, self.catalog_executor, locations)
                    for plugin_hook in plugin_hooks:
                        try:
                            plugin_hook.post_execute(new_locations, alternative_executor)
                        except Exception as e:
                            print_exc(10)
                            message = f"Unable to post_execute on plugin {plugin_hook.__class__}"
                            logger.error(message, exc_info=e)
                            raise QueryError(f"{message}: {str(e)}")
                if new_locations is not None:
                    with sentry_sdk.start_span(op=op_name, name="Register new locations"):
                        self.catalog.register_locations(new_locations)
                return alternative_executor

        with sentry_sdk.start_span(name="Execute query on Snowflake"):
            last_executor = self.catalog_executor
            if ast is None:
                last_executor.execute_raw(raw_query, self.catalog_executor)
            else:
                last_executor.execute(ast, self.catalog_executor, {})
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


ENABLE_DEBUG_WATCH_TOWER = False
WATCH_TOWER_SCHEDULE_SECONDS = 2
kill_event = threading.Event()


def watch_tower(cache_directory, **kwargs):
    while True:
        kill_event.wait(timeout=WATCH_TOWER_SCHEDULE_SECONDS)
        if kill_event.is_set():
            break
        processing_sessions = sum(session.processing for token, session in sessions.items())
        if ENABLE_DEBUG_WATCH_TOWER or processing_sessions > 0:
            try:
                import psutil
                process = psutil.Process()
                cpu_percent = f"[CPU: {'%.1f' % psutil.cpu_percent()}%]"
                memory_percent = f"[Memory: {'%.1f' % process.memory_percent()}%]"
            except:
                memory_percent = ""
                cpu_percent = ""

            disk_info = get_friendly_disk_usage(cache_directory, debug=ENABLE_DEBUG_WATCH_TOWER)
            logger.info(f"Currently {len(sessions)} sessions running {processing_sessions} queries "
                        f"| System: {cpu_percent} {memory_percent} [Disk: {disk_info}] ")


thread = threading.Thread(target=watch_tower, kwargs=current_context)
thread.daemon = True
thread.start()


# If the user intends to kill the server, not wait for DuckDB to gracefully shutdown.
# It's nice to treat the Duck better giving it time but user's time is more valuable than the duck's.
def harakiri(sig, frame):
    print("Killing the server, bye!")
    kill_event.set()
    os.kill(os.getpid(), signal.SIGKILL)

# last_intent_to_kill = time.time()
# def graceful_shutdown(_, frame):
#     global last_intent_to_kill
#     processing_sessions = sum(session.processing for token, session in sessions.items())
#     if processing_sessions == 0 or (time.time() - last_intent_to_kill) < 5000:
#         harakiri(signal, frame)
#     else:
#         print(f'Repeat sigint to confirm killing {processing_sessions} running queries.')
#         last_intent_to_kill = time.time()
