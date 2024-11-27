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
    Schema, TransientProperty, TemporaryProperty, Select, Column, Alias, Anonymous, parse_identifier, Subquery

from universql.warehouse import ICatalog, Executor, Locations, Tables
from universql.util import SNOWFLAKE_HOST, QueryError, prepend_to_lines, get_friendly_time_since
from universql.protocol.utils import get_field_for_snowflake

MAX_LIMIT = 10000

logging.basicConfig(level=logging.INFO)
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
        for table, location in tables.items():
            if location is not None:
                queries.append(table.sql(dialect='snowflake'))
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
        return StaticTable.from_metadata(location, self.iceberg_catalog.properties)

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

    # def get_volume_lake_path(self, volume : str) -> str:
    #     volume_location = pd.read_sql("DESC EXTERNAL VOLUME identifier(%s)", self.connection, params=[volume])
    #     active_storage = duckdb.sql("""select property_value from volume_location
    #                 where parent_property = 'STORAGE_LOCATIONS' and property = 'ACTIVE'
    #                """).fetchall()[0][0]
    #     all_properties = duckdb.execute("""select property_value from volume_location
    #             where parent_property = 'STORAGE_LOCATIONS' and property like 'STORAGE_LOCATION_%'""").fetchall()
    #     for properties in all_properties:
    #         loads = json.loads(properties[0])
    #         if loads.get('NAME') == active_storage:
    #             volume_mapping[volume] = loads
    #             break
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
        self.catalog = catalog

    def supports(self, ast: sqlglot.exp.Expression) -> bool:
        return True

    def default_create_table_as_iceberg(self, expression: sqlglot.exp.Expression):
        prefix = self.catalog.iceberg_catalog.properties.get("location")

        if isinstance(expression, sqlglot.exp.Create):
            if expression.kind == 'TABLE':
                properties = expression.args.get('properties') or Properties()
                is_transient = TransientProperty() in properties.expressions
                is_temp = TemporaryProperty() in properties.expressions
                is_iceberg = IcebergProperty() in properties.expressions
                if is_transient or len(properties.expressions) == 0:
                    properties__set = Properties()
                    external_volume = Property(this=Var(this='EXTERNAL_VOLUME'),
                                               value=Literal.string(
                                                   self.catalog.context.get('snowflake_iceberg_volume')))
                    snowflake_catalog = self.catalog.iceberg_catalog or "snowflake"
                    catalog = Property(this=Var(this='CATALOG'), value=Literal.string(snowflake_catalog))
                    if snowflake_catalog == 'snowflake':
                        base_location = Property(this=Var(this='BASE_LOCATION'),
                                                 value=Literal.string(location.metadata_location[len(prefix):]))
                    elif snowflake_catalog == 'glue':
                        base_location = Property(this=Var(this='CATALOG_TABLE_NAME'),
                                                 value=Literal.string(expression.this.sql()))
                    create_table_props = [IcebergProperty(), external_volume, catalog, base_location]
                    properties__set.set('expressions', create_table_props)

                    metadata_query = expression.expression.sql(dialect="snowflake")
                    try:
                        self.catalog.cursor().describe(metadata_query)
                    except Exception as e:
                        logger.error(f"Unable fetching schema for metadata query {e.args} \n" + metadata_query)
                        return expression
                    columns = [(column.name, FIELD_TYPES[column.type_code]) for column in
                               self.catalog.cursor().description]
                    unsupported_columns = [(column[0], column[1].name) for column in columns if column[1].name not in (
                        'BOOLEAN', 'TIME', 'BINARY', 'TIMESTAMP_TZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP',
                        'DATE', 'FIXED',
                        'TEXT', 'REAL')]
                    if len(unsupported_columns) > 0:
                        logger.error(
                            f"Unsupported columns {unsupported_columns} in {expression.expression.sql(dialect='snowflake')}")
                        return expression

                    column_definitions = [ColumnDef(
                        this=sqlglot.exp.parse_identifier(column[0]),
                        kind=DataType.build(self._convert_snowflake_to_iceberg_type(column[1]), dialect="snowflake"))
                        for
                        column in
                        columns]
                    schema = Schema()
                    schema.set('this', expression.this)
                    schema.set('expressions', column_definitions)
                    expression.set('this', schema)
                    select = Select().from_(Subquery(this=expression.expression))
                    for column in columns:
                        col_ast = Column(this=parse_identifier(column[0]))
                        if column[1].name in ('ARRAY', 'OBJECT'):
                            alias = Alias(this=Anonymous(this="to_variant", expressions=[col_ast]),
                                          alias=parse_identifier(column[0]))
                            select = select.select(alias)
                        else:
                            select = select.select(col_ast)

                    expression.set('expression', select)
                    expression.set('properties', properties__set)
        return expression

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

    def execute_raw(self, compiled_sql: str) -> None:
        run_on_warehouse = self.catalog.compute.get('warehouse') is not None
        try:
            emoji = "☁️(user cloud services)" if not run_on_warehouse else "💰(used warehouse)"
            logger.info(f"[{self.catalog.session_id}] Running on Snowflake.. {emoji} \n {compiled_sql}")
            self.catalog.cursor().execute(compiled_sql)
        except DatabaseError as e:
            message = f"{e.sfqid}: {e.msg} \n{compiled_sql}"
            raise QueryError(message, e.sqlstate)

    def execute(self, ast: sqlglot.exp.Expression, locations: Tables) -> \
            typing.Optional[typing.Dict[sqlglot.exp.Table, str]]:
        compiled_sql = (ast
                        # .transform(self.default_create_table_as_iceberg)
                        .sql(dialect="snowflake", pretty=True))
        self.execute_raw(compiled_sql)
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
