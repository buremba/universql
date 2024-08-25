import json
import logging
import typing
from traceback import print_exc
from typing import List

import duckdb
import pyarrow as pa
import snowflake.connector
import sqlglot
from pyarrow import ArrowInvalid
from snowflake.connector import NotSupportedError, DatabaseError
from snowflake.connector.constants import FIELD_TYPES, FIELD_ID_TO_NAME
from snowflake.connector.cursor import ResultMetadataV2, SnowflakeCursor

from universql.catalog import IcebergCatalog, Cursor
from universql.util import SnowflakeError
from universql.warehouse.duckdb.utils import should_run_on_catalog

MAX_LIMIT = 10000

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("❄️")


class SnowflakeIcebergCursor(Cursor):
    def __init__(self, cant_use_warehouse: bool, query_id: str, cursor: SnowflakeCursor):
        self.query_id = query_id
        self.cursor = cursor
        self.cant_use_warehouse = cant_use_warehouse

    def execute(self, asts: typing.Optional[List[sqlglot.exp.Expression]], raw_query: str) -> None:
        if asts is None:
            compiled_sql = raw_query
        else:
            compiled_sql = ";".join([ast.sql(dialect="snowflake", pretty=True) for ast in asts])
        try:
            run_on_warehouse = not self.cant_use_warehouse and not all(should_run_on_catalog(ast) for ast in asts)
            emoji = "☁️(user cloud services)" if not run_on_warehouse else "💰(used warehouse)"
            logger.info(f"[{self.query_id}] Running on Snowflake.. {emoji}")
            self.cursor.execute(compiled_sql)
        except DatabaseError as e:
            message = f"Unable to run Snowflake query {e.sfqid}: \n {e.msg}"
            raise SnowflakeError(e.sfqid, message, e.sqlstate)

    def close(self):
        self.cursor.close()

    def get_v1_columns(self):
        columns = []
        for idx, col in enumerate(self.cursor._description):
            sf_type = FIELD_ID_TO_NAME[col.type_code]
            columns.append({
                "name": col.name,
                "database": "",
                "schema": "",
                "table": "",
                "nullable": True,
                "type": sf_type.lower(),
                "length": 16777216,
                "scale": 0,
                "precision": 38,
                "byteLength": 16777216,
                "collation": None
            })
        return columns

    def get_as_table(self) -> pa.Table:
        try:
            arrow_all = self.cursor.fetch_arrow_all(force_return_table=True)
            for idx, column in enumerate(self.cursor._description):
                (field, value) = self.get_field_for_snowflake(column, arrow_all[idx])
                arrow_all = arrow_all.set_column(idx, field, value)
            return arrow_all
        # return from snowflake is not using arrow
        except NotSupportedError:
            values = [[] for _ in range(len(self.cursor._description))]

            row = self.cursor.fetchone()
            while row is not None:
                for idx, column in enumerate(row):
                    values[idx].append(column)
                row = self.cursor.fetchone()

            fields = []
            for idx, column in enumerate(self.cursor._description):
                (field, _) = self.get_field_for_snowflake(column)
                fields.append(field)
            schema = pa.schema(fields)

            result_data = pa.Table.from_arrays([pa.array(value) for value in values], names=schema.names)

            for idx, column in enumerate(self.cursor._description):
                (field, value) = self.get_field_for_snowflake(column, result_data[idx])
                try:
                    result_data = result_data.set_column(idx, field, value)
                except ArrowInvalid as e:
                    # TODO: find a better approach (maybe casting?)
                    if any(value is not None for value in values):
                        result_data = result_data.set_column(idx, field, pa.nulls(len(result_data), field.type))
                    else:
                        raise SnowflakeError(self.query_id, f"Unable to transform response: {e}")

            return result_data

    def get_field_for_snowflake(self, column: ResultMetadataV2, value: typing.Optional[pa.Array] = None) -> \
            typing.Tuple[
                pa.Field, pa.Array]:
        arrow_field = FIELD_TYPES[column.type_code]

        metadata = {
            "logicalType": arrow_field.name,
            "charLength": "8388608",
            "byteLength": "8388608",
        }

        if arrow_field.name == "GEOGRAPHY":
            metadata["logicalType"] = "OBJECT"

        if arrow_field.name == 'FIXED':
            pa_type = pa.decimal128(38, column.scale)
            if value is not None:
                value = value.cast(pa_type)
        elif arrow_field.name == 'DATE':
            pa_type = pa.date32()
            if value is not None:
                value = value.cast(pa_type)
        elif arrow_field.name == 'TIME':
            pa_type = pa.int64()
            if value is not None:
                value = value.cast(pa_type)
        elif arrow_field.name == 'TIMESTAMP_LTZ' or arrow_field.name == 'TIMESTAMP_NTZ' or arrow_field.name == 'TIMESTAMP':
            metadata["final_type"] = "T"
            timestamp_fields = [
                pa.field("epoch", nullable=False, type=pa.int64(), metadata=metadata),
                pa.field("fraction", nullable=False, type=pa.int32(), metadata=metadata),
            ]
            pa_type = pa.struct(timestamp_fields)
            if value is not None:
                epoch = pa.compute.divide(value.cast(pa.int64()), 1_000_000_000).combine_chunks()
                value = pa.StructArray.from_arrays(arrays=[epoch, pa.nulls(len(value), type=pa.int32())],
                                                   fields=timestamp_fields)
        elif arrow_field.name == 'TIMESTAMP_TZ':
            metadata["final_type"] = "T"
            timestamp_fields = [
                pa.field("epoch", nullable=False, type=pa.int64(), metadata=metadata),
                pa.field("fraction", nullable=False, type=pa.int32(), metadata=metadata),
                pa.field("timezone", nullable=False, type=pa.int32(), metadata=metadata),
            ]
            pa_type = pa.struct(timestamp_fields)
            if value is not None:
                epoch = pa.compute.divide(value.cast(pa.int64()), 1_000_000_000).combine_chunks()

                value = pa.StructArray.from_arrays(
                    arrays=[epoch,
                            # TODO: modulos 1_000_000_000 to get the fraction of a second, pyarrow doesn't support the operator yet
                            pa.nulls(len(value), type=pa.int32()),
                            # TODO: reverse engineer the timezone conversion
                            pa.nulls(len(value), type=pa.int32()),
                            ],
                    fields=timestamp_fields)
        else:
            pa_type = arrow_field.pa_type(column)

        if column.precision is not None:
            metadata["precision"] = str(column.precision)
        if column.scale is not None:
            metadata["scale"] = str(column.scale)

        field = pa.field(column.name, type=pa_type, nullable=column.is_nullable, metadata=metadata)
        try:
            return (field, value)
        except Exception as e:
            print_exc()
            raise SnowflakeError(self.query_id,
                                 f"Unable to convert Snowflake data to Arrow, please create a Github issue with the stacktrace above: {e}")


class SnowflakeShowIcebergTables(IcebergCatalog):
    def __init__(self, cant_use_warehouse: bool, query_id: str, credentials: dict):
        super().__init__(query_id, credentials)
        self.databases = {}
        self.cant_use_warehouse = cant_use_warehouse
        try:
            self.connection = snowflake.connector.connect(**credentials)
        except DatabaseError as e:
            raise SnowflakeError(self.query_id, e.msg, e.sqlstate)

    def cursor(self) -> Cursor:
        return SnowflakeIcebergCursor(self.cant_use_warehouse, self.query_id, self.connection.cursor())

    def get_table_references(self, cursor: duckdb.DuckDBPyConnection, tables: List[sqlglot.exp.Table]) -> typing.Dict[
        sqlglot.exp.Table, sqlglot.exp.Expression]:
        if len(tables) == 0:
            return {}
        sqls = ["SYSTEM$GET_ICEBERG_TABLE_INFORMATION(%s)" for _ in tables]
        values = [table.sql(comments=False, dialect="snowflake") for table in tables]
        cur = self.connection.cursor()
        final_query = f"SELECT {(', '.join(sqls))}"
        try:
            cur.execute(final_query, values)
            result = cur.fetchall()
            return {table: SnowflakeShowIcebergTables._get_ref(json.loads(result[0][idx])) for idx, table in
                    enumerate(tables)}
        except DatabaseError as e:
            err_message = f"Unable to find location of Iceberg tables. See: https://github.com/buremba/universql#cant-query-native-snowflake-tables. Cause: {e.msg}"
            raise SnowflakeError(self.query_id, err_message, e.sqlstate)

    @staticmethod
    def _get_ref(table_information):
        location = table_information.get('metadataLocation')
        return sqlglot.exp.func("iceberg_scan",
                                sqlglot.exp.Literal.string(location))

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
