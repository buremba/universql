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

MAX_LIMIT = 10000

logger = logging.getLogger("🐘")

logging.getLogger('postgresql.connector').setLevel(logging.WARNING)

@register(name="postgresql")
class PostgresqlCatalog(ICatalog):

    def __init__(self, session):
        super().__init__(session)
        self._cursor = None
        self._connection = None
        
        
    def cursor(self):
        if self._cursor is not None:
            return self._cursor
        try:
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
            logger.info(f"[{self.session_id}] Syncing PostgreSQL catalog \n{prepend_to_lines(final_query)}")
            try:
                self.cursor().execute(final_query)
                
                performance_counter = time.perf_counter()
                logger.info(
                    f"[{self.session_id}] Synced catalog with Postgresql 🐘 "
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
        return f"PostgreSQL query completed in {total_duration:.2f}s"

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