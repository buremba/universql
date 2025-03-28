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
from pyiceberg.table import StaticTable
from pyiceberg.table.snapshots import Summary, Operation
from pyiceberg.typedef import IcebergBaseModel
from snowflake.connector import NotSupportedError, DatabaseError, Error
from snowflake.connector.constants import FieldType
from universql.protocol.session import UniverSQLSession
from universql.protocol.utils import get_field_for_snowflake
from universql.util import SNOWFLAKE_HOST, QueryError, prepend_to_lines, get_friendly_time_since
from universql.plugin import ICatalog, Executor, Locations, Tables, register

MAX_LIMIT = 10000

logger = logging.getLogger("❄️")

logging.getLogger('snowflake.connector').setLevel(logging.WARNING)


# temporary workaround until pyiceberg bug is resolved
def summary_init(summary, **kwargs):
    operation = kwargs.get('operation', Operation.APPEND)
    if "operation" in kwargs:
        del kwargs["operation"]
    super(IcebergBaseModel, summary).__init__(operation=operation, **kwargs)
    summary._additional_properties = kwargs


Summary.__init__ = summary_init

@register(name="snowflake")
class SnowflakeCatalog(ICatalog):

    def __init__(self, session : UniverSQLSession):
        super().__init__(session)
        if session.context.get('account') is not None:
            session.credentials["account"] = session.context.get('account')
        if SNOWFLAKE_HOST is not None:
            session.credentials["host"] = SNOWFLAKE_HOST

        self.databases = {}
        # lazily create
        self._cursor = None

    def clear_cache(self):
        self._cursor = None

    def executor(self) -> Executor:
        return SnowflakeExecutor(self)

    def cursor(self):
        if self._cursor is not None:
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

    def _get_ref(self, table_information) -> pyiceberg.table.Table:
        location = table_information.get('metadataLocation')
        try:
            return StaticTable.from_metadata(location, self.iceberg_catalog.properties)
        except PermissionError as e:
            raise QueryError(f"Unable to access Iceberg metadata {location}. Cause: \n" + str(e))

    def get_table_paths(self, tables: List[sqlglot.exp.Table]) -> Tables:
        if len(tables) == 0:
            return {}
        cursor = self.cursor()
        sqls = ["SYSTEM$GET_ICEBERG_TABLE_INFORMATION(%s)" for _ in tables]
        values = [table.sql(comments=False, dialect="snowflake") for table in tables]
        final_query = f"SELECT {(', '.join(sqls))}"
        try:
            cursor.execute(final_query, values)
            result = cursor.fetchall()
            return {table: self._get_ref(json.loads(result[0][idx])) for idx, table in
                    enumerate(tables)}
        except DatabaseError as e:
            err_message = f"Unable to find location of Iceberg tables. See: https://github.com/buremba/universql#cant-query-native-snowflake-tables. Cause: \n {e.msg} \n{final_query}"
            raise QueryError(err_message, e.sqlstate)

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
    # def find_table_location(self, database: str, schema: str, table_name: str, lazy_check: bool = True) -> str:
    #     table_location = self.databases.get(database, {}).get(schema, {}).get(table_name)
    #     if table_location is None:
    #         if lazy_check:
    #             self.load_database_schema(database, schema)
    #             return self.find_table_location(database, schema, table_name, lazy_check=False)
    #         else:
    #             raise Exception(f"Table {table_name} not found in {database}.{schema}")
    #     return table_location
    # def load_external_volumes_for_tables(self, tables: pd.DataFrame) -> pd.DataFrame:
    #     volumes = tables["external_volume_name"].unique()
    #
    #     volume_mapping = {}
    #     for volume in volumes:
    #         volume_location = pd.read_sql("DESC EXTERNAL VOLUME identifier(%s)", self.connection, params=[volume])
    #         active_storage = duckdb.sql("""select property_value from volume_location
    #                     where parent_property = 'STORAGE_LOCATIONS' and property = 'ACTIVE'
    #                    """).fetchall()[0][0]
    #         all_properties = duckdb.execute("""select property_value from volume_location
    #                 where parent_property = 'STORAGE_LOCATIONS' and property like 'STORAGE_LOCATION_%'""").fetchall()
    #         for properties in all_properties:
    #             loads = json.loads(properties[0])
    #             if loads.get('NAME') == active_storage:
    #                 volume_mapping[volume] = loads
    #                 break
    #     return volume_mapping

    # def load_database_schema(self, database: str, schema: str):
    #     tables = self.load_iceberg_tables(database, schema)
    #     external_volumes = self.load_external_volumes_for_tables(tables)
    #
    #     tables["external_location"] = tables.apply(
    #         lambda x: (external_volumes[x["external_volume_name"]].get('STORAGE_BASE_URL')
    #                    + x["base_location"]), axis=1)
    #     if database not in self.databases:
    #         self.databases[database] = {}
    #
    #     self.databases[database][schema] = dict(zip(tables.name, tables.external_location))

    # def load_iceberg_tables(self, database: str, schema: str, after: Optional[str] = None) -> pd.DataFrame:
    #     query = "SHOW ICEBERG TABLES IN SCHEMA IDENTIFIER(%s) LIMIT %s", [database + '.' + schema, MAX_LIMIT]
    #     if after is not None:
    #         query[0] += " AFTER %s"
    #         query[1].append(after)
    #     tables = pd.read_sql(query[0], self.connection, params=query[1])
    #     if len(tables.index) >= MAX_LIMIT:
    #         after = tables.iloc[-1, :]["name"]
    #         return tables + self.load_iceberg_tables(database, schema, after=after)
    #     else:
    #         return tables


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

    def test(self):
        return self.catalog.cursor()

    def execute_raw(self, raw_query: str, catalog_executor: Executor, run_on_warehouse=None) -> None:
        try:
            emoji = "☁️(user cloud services)" if not run_on_warehouse else "💰(used warehouse)"
            logger.info(f"[{self.catalog.session_id}] Running on Snowflake.. {emoji} \n {raw_query}")
            self.catalog.cursor().execute(raw_query)
        except Error as e:
            message = f"{e.sfqid}: {e.msg} \n{raw_query}"
            raise QueryError(message, e.sqlstate)

    def execute(self, ast: sqlglot.exp.Expression, catalog_executor: Executor, locations: Tables) -> \
            typing.Optional[typing.Dict[sqlglot.exp.Table, str]]:
        compiled_sql = (ast
                        # .transform(self.default_create_table_as_iceberg)
                        .sql(dialect="snowflake", pretty=True))
        self.execute_raw(compiled_sql, catalog_executor)
        return None

    def get_query_log(self, total_duration) -> str:
        return "Run on Snowflake"

    def close(self):
        cursor = self.catalog._cursor
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