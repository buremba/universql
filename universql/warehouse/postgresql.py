import enum
import logging
import re
import time
from typing import Dict, Iterator, List, Optional, Tuple

import pyarrow as pa
import sqlglot
import psycopg2

from universql.plugin import register, ICatalog, Locations, Tables, Executor
from universql.util import prepend_to_lines, get_friendly_time_since, QueryError

# Add PGlite imports
try:
    from py_pglite import PGliteManager, PGliteConfig
    PGLITE_AVAILABLE = True
except ImportError:
    PGLITE_AVAILABLE = False

MAX_LIMIT = 10000

logger = logging.getLogger("🐘")

logging.getLogger('postgresql.connector').setLevel(logging.WARNING)

@register(name="postgresql")
class PostgresqlCatalog(ICatalog):

    def __init__(self, session):
        super().__init__(session)
        self._cursor = None
        self._connection = None
        
        # Check if this is a local embedded instance
        self._is_local = self.credentials.get('local', True)
        self._pglite_manager = None
        
        if self._is_local and not PGLITE_AVAILABLE:
            raise ImportError("PGlite Py is not available for local mode. Install with: pip install 'py-pglite[sqlmodel]'")

    def cursor(self):
        if self._cursor is not None:
            return self._cursor
        try:
            if self._is_local:
                # Use embedded PGlite
                if self._pglite_manager is None:
                    config = PGliteConfig(
                        timeout=30,
                        cleanup_on_exit=True,
                        log_level="INFO",
                        auto_install_deps=True
                    )
                    self._pglite_manager = PGliteManager(config)
                    self._pglite_manager.__enter__()
                
                # Get PGlite connection string and connect with psycopg2
                engine = self._pglite_manager.get_engine()
                # Extract connection parameters from SQLAlchemy engine URL
                conn_url = str(engine.url)
                logger.info(f"PGlite connection URL: {conn_url}")
                
                # Parse the connection string to get psycopg2 parameters
                if conn_url.startswith('postgresql'):
                    # Extract connection parameters from the engine URL
                    import urllib.parse
                    parsed = urllib.parse.urlparse(conn_url)
                    
                    # Parse query parameters for socket connection
                    query_params = urllib.parse.parse_qs(parsed.query)
                    
                    conn_params = {
                        'database': parsed.path.lstrip('/') or 'postgres',
                        'user': parsed.username or 'postgres'
                    }
                    
                    # Only add password if it exists
                    if parsed.password:
                        conn_params['password'] = parsed.password
                    
                    # Check if using Unix socket (host parameter in query)
                    if 'host' in query_params:
                        # Decode the URL-encoded socket path
                        socket_path = urllib.parse.unquote(query_params['host'][0])
                        conn_params['host'] = socket_path
                        logger.info(f"Using Unix socket: {socket_path}")
                    else:
                        # Use regular TCP connection
                        conn_params['host'] = parsed.hostname or 'localhost'
                        if parsed.port:
                            conn_params['port'] = parsed.port
                else:
                    # Fallback: try to get connection info from manager
                    try:
                        # Access the PGlite configuration for connection details
                        conn_params = {
                            'host': 'localhost',
                            'port': self._pglite_manager._port if hasattr(self._pglite_manager, '_port') else 5432,
                            'database': 'postgres',
                            'user': 'postgres'
                        }
                    except:
                        # Final fallback
                        conn_params = {
                            'host': 'localhost',
                            'port': 5432,
                            'database': 'postgres',
                            'user': 'postgres'
                        }
                
                self._connection = psycopg2.connect(**conn_params)
                self._cursor = self._connection.cursor()
            else:
                # Use regular PostgreSQL connection
                # Filter credentials to only include valid PostgreSQL connection parameters
                valid_pg_params = ['host', 'port', 'database', 'user', 'password', 'sslmode', 'sslcert', 'sslkey', 'sslrootcert', 'application_name']
                filtered_credentials = {k: v for k, v in self.credentials.items() if k in valid_pg_params and v is not None}
                self._connection = psycopg2.connect(**filtered_credentials)
                self._cursor = self._connection.cursor()
        except psycopg2.Error as e:
            raise QueryError(str(e), getattr(e, 'pgcode', None))
        except Exception as e:
            raise QueryError(str(e))
        return self._cursor

    def get_table_paths(self, tables: List[sqlglot.exp.Table]) -> Tables:
        native_tables = {}
        for table in tables:
            native_tables[table] = None
        return native_tables

    def register_locations(self, tables: Locations):
        start_time = time.perf_counter()
        queries = []
        for location in tables.values():
            queries.append(location.sql(dialect='postgres'))
        final_query = '\n'.join(queries)
        if final_query:
            catalog_type = "PGlite" if self._is_local else "PostgreSQL"
            logger.info(f"[{self.session_id}] Syncing {catalog_type} catalog \n{prepend_to_lines(final_query)}")
            try:
                if self._is_local:
                    # Execute each query separately for PGlite
                    cursor = self.cursor()
                    for query in queries:
                        if query.strip():
                            cursor.execute(query)
                    self._connection.commit()
                else:
                    # Execute all queries as one for regular PostgreSQL
                    self.cursor().execute(final_query)
                
                performance_counter = time.perf_counter()
                logger.info(
                    f"[{self.session_id}] Synced catalog with {catalog_type} 🐘 "
                    f"({get_friendly_time_since(start_time, performance_counter)})")
            except psycopg2.Error as e:
                raise QueryError(str(e), getattr(e, 'pgcode', None))
            except Exception as e:
                raise QueryError(str(e))

    def executor(self) -> "Executor":
        return PostgresqlExecutor(self)

    def close(self):
        """Clean up resources"""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None
        if self._pglite_manager:
            self._pglite_manager.__exit__(None, None, None)
            self._pglite_manager = None



class PostgresqlExecutor(Executor):

    def __init__(self, catalog: PostgresqlCatalog):
        super().__init__(catalog)
        self._result = None

    def execute(self, ast: sqlglot.exp.Expression, catalog_executor: Executor, locations: Tables) -> Optional[Locations]:
        try:
            query = ast.sql(dialect='postgres')
            self.catalog.cursor().execute(query)
            # Only try to fetch results if the cursor has a description (result columns)
            if self.catalog.cursor().description:
                self._result = self.catalog.cursor().fetchall()
            else:
                self._result = []
            return None
        except psycopg2.Error as e:
            raise QueryError(str(e), getattr(e, 'pgcode', None))

    def execute_raw(self, raw_query: str, catalog_executor: Optional[Executor]) -> None:
        try:
            self.catalog.cursor().execute(raw_query)
            # Only try to fetch results if the cursor has a description (result columns)
            if self.catalog.cursor().description:
                self._result = self.catalog.cursor().fetchall()
            else:
                self._result = []
        except psycopg2.Error as e:
            raise QueryError(str(e), getattr(e, 'pgcode', None))

    def get_as_table(self) -> pa.Table:
        if self._result is None:
            return pa.Table.from_arrays([], [])
        
        # Convert PostgreSQL result to PyArrow table
        if not self._result:
            return pa.Table.from_arrays([], [])
        
        # Get column descriptions from cursor
        columns = self.catalog.cursor().description
        if not columns:
            return pa.Table.from_arrays([], [])
        
        # Convert data to PyArrow format
        arrays = []
        names = []
        for i, col_desc in enumerate(columns):
            names.append(col_desc[0])
            column_data = [row[i] for row in self._result]
            arrays.append(pa.array(column_data))
        
        return pa.Table.from_arrays(arrays, names)

    def get_query_log(self, total_duration) -> str:
        catalog_type = "PGlite" if self.catalog._is_local else "PostgreSQL"
        return f"{catalog_type} query completed in {total_duration:.2f}s"

    def close(self):
        if hasattr(self.catalog, 'close'):
            self.catalog.close()
        self._result = None



class PyPgType(enum.Enum):
    NULL = 0
    BIGINT = 1
    BOOL = 2
    BYTES = 3
    DATE = 4
    FLOAT = 5
    INTEGER = 6
    INTERVAL = 7
    JSON = 8
    DECIMAL = 9
    TEXT = 10
    TIME = 11
    TIMESTAMP = 12
    UNKNOWN = 13
    ARRAY = 14
    INTEGERARRAY = 15
    STRINGARRAY = 16

def to_pypg_type(t: pa.DataType) -> PyPgType:
    if pa.types.is_int64(t):
        return PyPgType.BIGINT
    elif pa.types.is_integer(t):
        return PyPgType.INTEGER
    elif pa.types.is_string(t) or pa.types.is_large_string(t):
        return PyPgType.TEXT
    elif pa.types.is_date(t):
        return PyPgType.DATE
    elif pa.types.is_time(t):
        return PyPgType.TIME
    elif pa.types.is_timestamp(t):
        return PyPgType.TIMESTAMP
    elif pa.types.is_floating(t):
        return PyPgType.FLOAT
    elif pa.types.is_decimal(t):
        return PyPgType.DECIMAL
    elif pa.types.is_binary(t):
        return PyPgType.BYTES
    elif pa.types.is_boolean(t):
        return PyPgType.BOOL
    elif pa.types.is_interval(t):
        return PyPgType.INTERVAL
    elif pa.types.is_list(t):
        field_type = t.field(0).type
        if pa.types.is_integer(field_type):
            return PyPgType.INTEGERARRAY
        elif pa.types.is_string(field_type):
            return PyPgType.STRINGARRAY
        else:
            # TODO: detailed nested types
            return PyPgType.JSON
    elif pa.types.is_struct(t) or pa.types.is_map(t):
        # TODO: support detailed nested types
        return PyPgType.JSON
    else:
        raise Exception("Could not convert DuckDB type: " + str(t))


class RecordBatchIterator(Iterator[List[Optional[str]]]):
    def __init__(self, rbr: pa.RecordBatchReader):
        self.rbr = rbr
        self.rb = None
        self.i = 0

    def __next__(self) -> List:
        if self.rb is None or self.i >= self.rb.num_rows:
            try:
                self.rb = self.rbr.read_next_batch()
                self.i = 0
            except StopIteration:
                raise StopIteration

        ret = []
        for col in self.rb.columns:
            ret.append(col[self.i].as_py())
        self.i += 1
        return ret


class DuckDBQueryResult():
    def __init__(
        self, rbr: Optional[pa.RecordBatchReader] = None, status: Optional[str] = None
    ):
        super().__init__()
        if rbr:
            self.rbr = rbr
            self.rbi = RecordBatchIterator(self.rbr)
            self.bvtypes = [to_pypg_type(s.type) for s in rbr.schema]
        else:
            self.rbi = None
            self.rbr = None
            self.bvtypes = []
        self._status = status

    def has_results(self) -> bool:
        return self.rbr is not None

    def column_count(self):
        if self.rbr:
            return len(self.rbr.schema)
        else:
            return 0

    def column(self, index: int) -> Tuple[str, PyPgType]:
        if self.rbr:
            s = self.rbr.schema[index]
            return s.name, self.bvtypes[index]
        else:
            raise IndexError("No column at index %d" % index)

    def rows(self) -> Iterator[List]:
        if self.rbi:
            return self.rbi
        else:
            return iter([])

    def status(self) -> str:
        return self._status


class DuckDBSession():
    def __init__(self, cursor):
        super().__init__()
        self._cursor = cursor
        self.in_txn = False
        self.refresh_config()

    def cursor(self):
        return self._cursor

    def close(self):
        self._cursor.close()

    def refresh_config(self):
        self.config_params = set(
            [
                r[0]
                for r in self._cursor.execute(
                    "SELECT name FROM duckdb_settings()"
                ).fetchall()
            ]
        )

    def load_df_function(self, table: str):
        return self._cursor.query(f"select * from {table}")

    def rewrite_sql(self, sql: str) -> str:
        """Some minimalist SQL rewrites, inspired by postlite, to make DBeaver less unhappy."""
        if match := re.search(r"PREPARE\s+(\w+)\s+FROM", sql):
            stmt = match.group(1)
            target = f"PREPARE {stmt} FROM"
            replace = f"PREPARE {stmt} AS"
            return sql.replace(target, replace)
        if sql.startswith("SET "):
            tokens = sql.split()
            if tokens[1].lower() in self.config_params:
                return sql
            else:
                return ""
        elif sql == "SHOW search_path":
            return "SELECT current_setting('search_path') as search_path"
        elif sql == "SHOW TRANSACTION ISOLATION LEVEL":
            return "SELECT 'read committed' as transaction_isolation"
        elif sql == "BEGIN READ ONLY":
            return "BEGIN"
        elif (
            sql
            == "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'"
        ):
            return "SELECT 32 as setting"
        elif m2 := re.search(r"CAST\('([^']+)' AS REGCLASS\)", sql):
            name = m2.group(1)
            target = f"CAST('{name}' AS REGCLASS)"
            replace = f"'{name}'"
            return sql.replace(target, replace)
        elif "::regclass" in sql:
            return sql.replace("::regclass", "")
        elif "::regtype" in sql:
            return sql.replace("::regtype", "")
        elif "::regproc" in sql:
            return sql.replace("::regproc", "")
        elif "pg_get_expr(ad.adbin, ad.adrelid, true)" in sql:
            return sql.replace(
                "pg_get_expr(ad.adbin, ad.adrelid, true)",
                "pg_get_expr(ad.adbin, ad.adrelid)",
            )
        elif "pg_catalog.current_schemas" in sql:
            return sql.replace("pg_catalog.current_schemas", "current_schemas")
        elif "pg_catalog.generate_series" in sql:
            return sql.replace("pg_catalog.generate_series", "generate_series")
        elif "pg_catalog.PG_GET_USERBYID" in sql:
            # allow psql's \d to work
            # just display the owner's oid
            return sql.replace("pg_catalog.PG_GET_USERBYID", "")
        return sql

    def in_transaction(self) -> bool:
        return self.in_txn

    def execute_sql(self, sql: str, params=None):
        status = ""
        try:
            lsql = sqlglot.parse_one(sql).sql(comments=False)
        except:
            # TODO: log this
            lsql = sql

        lsql = lsql.lower()
        if self.in_txn:
            if "commit" in lsql:
                self.in_txn = False
                status = "COMMIT"
            elif "rollback" in lsql:
                self.in_txn = False
                status = "ROLLBACK"
            elif "begin" in lsql or "start transaction" in lsql:
                return DuckDBQueryResult(status="BEGIN")
        elif "begin" in lsql or "start transaction" in lsql:
            lsql = "begin"
            self.in_txn = True
            status = "BEGIN"

        logger.debug("Original SQL: %s", sql)
        sql = self.rewrite_sql(sql)
        logger.debug("Rewritten SQL: %s", sql)
        if params:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)

        if status:
            return DuckDBQueryResult(status=status)

        rb = None
        if self._cursor.description:
            if "load " in lsql:
                self.refresh_config()
                status = "LOAD"
            elif not ("insert " in lsql or "update " in lsql or "delete " in lsql):
                rb = self._cursor.fetch_record_batch()
        return DuckDBQueryResult(rb, status)


class DuckDBConnection():
    def __init__(self, db):
        super().__init__()
        self.db = db

    def parameters(self) -> Dict[str, str]:
        return {
            "server_version": "9.3.duckdb",
            "client_encoding": "UTF8",
            "DateStyle": "ISO",
        }

    def new_session(self) :
        cursor = self.db.cursor()
        cursor.execute("SET search_path='main'")
        return DuckDBSession(cursor)