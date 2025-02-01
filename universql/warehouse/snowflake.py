import json
import logging
import time
import typing
from typing import List
from uuid import uuid4

import pyarrow as pa
import pyiceberg.table
import sentry_sdk
import snowflake.connector
import sqlglot
from pyarrow import ArrowInvalid
from pyiceberg.catalog import Catalog
from pyiceberg.table import StaticTable
from pyiceberg.table.snapshots import Summary, Operation
from pyiceberg.typedef import IcebergBaseModel
from snowflake.connector import NotSupportedError, DatabaseError
from snowflake.connector.constants import FIELD_TYPES, FieldType
from sqlglot.expressions import Literal, Var, Property, IcebergProperty, Properties, ColumnDef, DataType, \
    Schema, TransientProperty, TemporaryProperty, Select, Column, Alias, Anonymous, parse_identifier, Subquery, Show, \
    Use

from universql.warehouse import ICatalog, Executor, Locations, Tables
from universql.util import SNOWFLAKE_HOST, QueryError, prepend_to_lines, get_friendly_time_since
from universql.protocol.utils import get_field_for_snowflake
from pprint import pp
from .utils import get_stage_info

MAX_LIMIT = 10000

logger = logging.getLogger("❄️")

logging.getLogger('snowflake.connector').setLevel(logging.WARNING)


def summary_init(summary, **kwargs):
    operation = kwargs.get('operation', Operation.APPEND)
    if "operation" in kwargs:
        del kwargs["operation"]
    super(IcebergBaseModel, summary).__init__(operation=operation, **kwargs)
    summary._additional_properties = kwargs


Summary.__init__ = summary_init


class SnowflakeCatalog(ICatalog):

    def __init__(self, context: dict, session_id: str, credentials: dict, compute, iceberg_catalog: Catalog):
        super().__init__(context, session_id, credentials, compute, iceberg_catalog)
        if context.get('account') is not None:
            credentials["account"] = context.get('account')
        if SNOWFLAKE_HOST is not None:
            credentials["host"] = SNOWFLAKE_HOST

        self.databases = {}
        credentials["warehouse"] = compute.get('warehouse', str(uuid4()))
        # lazily create
        self._cursor = None

    def clear_cache(self):
        self._cursor = None

    def cursor(self, create_if_not_exists=True):
        if self._cursor is not None or not create_if_not_exists:
            return self._cursor
        with sentry_sdk.start_span(op="snowflake", name="Initialize Snowflake Connection"):
            try:
                self._cursor = snowflake.connector.connect(**self.credentials).cursor()
            except DatabaseError as e:
                raise QueryError(e.msg, e.sqlstate)

        return self._cursor

    def register_locations(self, tables: Locations):
        start_time = time.perf_counter()
        queries = []
        for location in tables.values():
            queries.append(location.sql(dialect='snowflake'))
        final_query = '\n'.join(queries)
        if final_query:
            logger.info(f"[{self.session_id}] Syncing Snowflake catalog \n{prepend_to_lines(final_query)}")
            try:
                self.cursor().execute(final_query)
                performance_counter = time.perf_counter()
                logger.info(
                    f"[{self.session_id}] Synced catalog with Snowflake ❄️ "
                    f"({get_friendly_time_since(start_time, performance_counter)})")
            except snowflake.connector.Error as e:
                raise QueryError(e.msg, e.sqlstate)

    def executor(self) -> Executor:
        return SnowflakeExecutor(self)

    def _get_ref(self, table_information) -> pyiceberg.table.Table:
        location = table_information.get('metadataLocation')
        try:
            return StaticTable.from_metadata(location, self.iceberg_catalog.properties)
        except PermissionError as e:
            raise QueryError(f"Unable to access Iceberg metadata {location}. Cause: \n" + str(e))

    def get_table_paths(self, tables: List[sqlglot.exp.Table]) -> Tables:
        if len(tables) == 0:
            return {}
        sqls = ["SYSTEM$GET_ICEBERG_TABLE_INFORMATION(%s)" for _ in tables]
        values = [table.sql(comments=False, dialect="snowflake") for table in tables]
        final_query = f"SELECT {(', '.join(sqls))}"
        try:
            self.cursor().execute(final_query, values)
            result = self.cursor().fetchall()
            return {table: self._get_ref(json.loads(result[0][idx])) for idx, table in
                    enumerate(tables)}
        except DatabaseError as e:
            err_message = f"Unable to find location of Iceberg tables. See: https://github.com/buremba/universql#cant-query-native-snowflake-tables. Cause: \n {e.msg} \n{final_query}"
            raise QueryError(err_message, e.sqlstate)
        
    def get_file_info(self, files, ast):
        copy_data_copy = {
            "files": {},
            "file_parameters": {}
        }


        if len(files) == 0:
            return {}
        copy_params = self._extract_copy_params(ast)
        print("copy_params INCOMING")
        pp(copy_params)
        file_format_params = copy_params.get("FILE_FORMAT")
        cursor = self.cursor()
        for file in files:
            print("file INCOMING")
            pp(file)
            if file.get("type") == 'STAGE':
                
                stage_info = get_stage_info(file, file_format_params, cursor)
                stage_info["METADATA"] = stage_info["METADATA"] | file
                copy_data_copy["files"][file["stage_name"]] = stage_info["METADATA"]
                if copy_data_copy["file_parameters"] == {}:
                    del stage_info["METADATA"]
                    copy_data_copy["file_parameters"] = stage_info
                
        return copy_data_copy
        
    def _extract_copy_params(self, ast):
        params = {}
        for param in ast.args.get('params', []):
            param_name = param.args['this'].args['this']
            
            # Handle single expression case
            if 'expression' in param.args:
                expr = param.args['expression']
                params[param_name] = expr.args['this'].args['this']
                
            # Handle multiple expressions case
            elif 'expressions' in param.args:
                params[param_name] = {}
                for expr in param.args['expressions']:
                    property_name = expr.args['this'].args['this']
                    property_value = expr.args['value'].args['this']
                    params[param_name][property_name] = property_value
                    
        return params

    def get_volume_lake_path(self, volume: str) -> str:
        cursor = self.cursor()
        cursor.execute("DESC EXTERNAL VOLUME identifier(%s)", [volume])
        volume_location = cursor.fetchall()

        # Find the active storage location name
        active_storage_name = next(
            (item[3] for item in volume_location if item[1] == 'ACTIVE' and item[0] == 'STORAGE_LOCATIONS'), None)

        # Extract the STORAGE_BASE_URL from the corresponding storage location
        storage_base_url = None
        if active_storage_name:
            for item in volume_location:
                if item[1].startswith('STORAGE_LOCATION_'):
                    storage_data = json.loads(item[3])
                    if storage_data.get('NAME') == active_storage_name:
                        storage_base_url = storage_data.get('STORAGE_BASE_URL')
                        break

        if storage_base_url is None:
            raise QueryError(f"Unable to find storage location for volume {volume}.")

        return storage_base_url
    

class SnowflakeExecutor(Executor):

    def __init__(self, catalog: SnowflakeCatalog):
        super().__init__(catalog)

    def _convert_snowflake_to_iceberg_type(self, snowflake_type: FieldType) -> str:
        if snowflake_type.name == 'TIMESTAMP_LTZ':
            return 'TIMESTAMP'
        if snowflake_type.name == 'VARIANT':
            # No support for semi-structured data. Maybe we should try OBJECT([SCHEMA])?
            return 'TEXT'
        if snowflake_type.name == 'ARRAY':
            # Relies on TO_VARIANT transformation
            return 'TEXT'
        if snowflake_type.name == 'OBJECT':
            # The schema is not available
            return 'TEXT'
        return snowflake_type.name

    def execute_raw(self, compiled_sql: str, run_on_warehouse=None) -> None:
        try:
            emoji = "☁️(user cloud services)" if not run_on_warehouse else "💰(used warehouse)"
            logger.info(f"[{self.catalog.session_id}] Running on Snowflake.. {emoji} \n {compiled_sql}")
            self.catalog.cursor().execute(compiled_sql)
        except DatabaseError as e:
            message = f"{e.sfqid}: {e.msg} \n{compiled_sql}"
            raise QueryError(message, e.sqlstate)

    def execute(self, ast: sqlglot.exp.Expression, locations: Tables, file_data = None) -> \
            typing.Optional[typing.Dict[sqlglot.exp.Table, str]]:
        compiled_sql = (ast
                        # .transform(self.default_create_table_as_iceberg)
                        .sql(dialect="snowflake", pretty=True))
        self.execute_raw(compiled_sql, run_on_warehouse=not isinstance(ast, Show) and not isinstance(ast, Use))
        return None

    def get_query_log(self, total_duration) -> str:
        return "Run on Snowflake"

    def close(self):
        cursor = self.catalog.cursor(create_if_not_exists=False)
        if cursor is not None:
            cursor.close()

    def get_as_table(self) -> pa.Table:
        try:
            arrow_all = self.catalog.cursor().fetch_arrow_all(force_return_table=True)
            for idx, column in enumerate(self.catalog.cursor()._description):
                (field, value) = get_field_for_snowflake(column, arrow_all[idx])
                arrow_all = arrow_all.set_column(idx, field, value)
            return arrow_all
        # return from snowflake is not using arrow
        except NotSupportedError:
            row = self.catalog.cursor().fetchone()
            values = [[] for _ in range(len(self.catalog.cursor()._description))]

            while row is not None:
                for idx, column in enumerate(row):
                    values[idx].append(column)
                row = self.catalog.cursor().fetchone()

            fields = []
            for idx, column in enumerate(self.catalog.cursor()._description):
                (field, _) = get_field_for_snowflake(column)
                fields.append(field)
            schema = pa.schema(fields)

            result_data = pa.Table.from_arrays([pa.array(value) for value in values], names=schema.names)

            for idx, column in enumerate(self.catalog.cursor()._description):
                (field, value) = get_field_for_snowflake(column, result_data[idx])
                try:
                    result_data = result_data.set_column(idx, field, value)
                except ArrowInvalid as e:
                    # TODO: find a better approach (maybe casting?)
                    if any(value is not None for value in values):
                        result_data = result_data.set_column(idx, field, pa.nulls(len(result_data), field.type))
                    else:
                        raise QueryError(f"Unable to transform response: {e}")

            return result_data
