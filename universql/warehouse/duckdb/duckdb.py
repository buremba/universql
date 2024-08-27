import logging
import os
import time
import typing
from typing import List, Optional

import duckdb
import pyarrow
import pyarrow as pa
import sqlglot
from fakesnow.fakes import FakeSnowflakeCursor, FakeSnowflakeConnection
from pyarrow import Table
from pyarrow.lib import ChunkedArray
from snowflake.connector import DatabaseError
from sqlglot import ParseError
from sqlglot.optimizer.simplify import simplify

from universql.catalog import get_catalog
from universql.catalog.snowflake import logger as cloud_logger
from universql.lake.cloud import s3, gcs
from universql.util import get_columns_for_duckdb, SnowflakeError, Compute, get_friendly_time_since, \
    prepend_to_lines, calculate_script_cost
from universql.warehouse.duckdb.utils import DuckDBFunctions, should_run_on_catalog

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("🐥")


class UniverSQLSession:
    def __init__(self, context, token, credentials: dict, session_parameters: dict):
        self.context = context
        self.credentials = credentials
        self.session_parameters = [{"name": item[0], "value": item[1]} for item in session_parameters.items()]
        self.token = token
        self.catalog = get_catalog(context, self.token,
                                   self.credentials)
        duckdb_path = context.get('database_path')
        self.duckdb = duckdb.connect(duckdb_path, config={
            'max_memory': context.get('max_memory'),
            'temp_directory': os.path.join(context.get('cache_directory'), "duckdb-staging"),
            'max_temp_directory_size': context.get('max_cache_size'),
        })
        DuckDBFunctions.register(self.duckdb)
        self.duckdb.install_extension("iceberg")
        self.duckdb.load_extension("iceberg")

        fake_snowflake_conn = FakeSnowflakeConnection(self.duckdb, "main", "public", False, False)
        fake_snowflake_conn.database_set = True
        fake_snowflake_conn.schema_set = True
        self.duckdb_emulator = FakeSnowflakeCursor(fake_snowflake_conn, self.duckdb)

        self.catalog_cursor = self.catalog.cursor()
        self.register_data_lake(context)
        self.processing = False
        self.compute = self.context.get('compute')

    def register_data_lake(self, args: dict):
        self.duckdb.register_filesystem(s3(args.get('cache_directory'), args.get('aws_profile')))
        self.duckdb.register_filesystem(gcs(args.get('cache_directory'), args.get('gcp_project')))

    def sync_duckdb_catalog(self, locations: typing.Dict[
        sqlglot.exp.Table, sqlglot.exp.Expression], ast: sqlglot.exp.Expression) -> Optional[
        sqlglot.exp.Expression]:

        schemas = set((table.catalog, table.db) for table in locations.keys())
        databases = set(table[0] for table in schemas)

        databases_sql = [f"ATTACH IF NOT EXISTS ':memory:' AS {sqlglot.exp.parse_identifier(database).sql()}" for database in
                         databases]
        schemas_sql = [
            f"CREATE SCHEMA IF NOT EXISTS {sqlglot.exp.parse_identifier(db).sql()}.{sqlglot.exp.parse_identifier(schema).sql()}" for
            (db, schema) in schemas]
        views_sql = [f"CREATE OR REPLACE VIEW {table.sql()} AS SELECT * FROM {expression.sql()};" for table, expression
                     in locations.items()]
        views_sql = ";\n".join(databases_sql + schemas_sql + views_sql)

        if views_sql != "":
            self.duckdb.execute(views_sql)
            logger.info(
                f"[{self.token}] DuckDB environment is setting up:\n{prepend_to_lines(views_sql)}")

        def replace_icebergs_with_duckdb_reference(
                expression: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
            if isinstance(expression, sqlglot.exp.Table):
                if expression.name != "":
                    new_table = sqlglot.exp.to_table(f"main.{sqlglot.exp.parse_identifier(expression.sql())}")
                    return new_table
                    # return locations[expression]
                else:
                    return expression

            return expression

        return (ast
                # .transform(replace_icebergs_with_duckdb_reference)
                .transform(fix_snowflake_to_duckdb_types))

    def _do_query(self, raw_query: str) -> (str, List, pyarrow.Table):
        start_time = time.perf_counter()
        local_error_message = None

        try:
            queries = sqlglot.parse(raw_query, read="snowflake")
        except ParseError as e:
            local_error_message = f"Unable to parse query with SQLGlot: {e.args}"
            queries = None

        should_run_locally = self.compute != Compute.SNOWFLAKE.value
        last_compute = Compute.LOCAL if queries is not None else Compute.SNOWFLAKE

        if last_compute == Compute.LOCAL and should_run_locally:
            for ast in queries:
                if should_run_on_catalog(ast) or not should_run_locally:
                    self.do_snowflake_query(queries, raw_query, start_time, local_error_message)
                    last_compute = Compute.SNOWFLAKE
                else:
                    tables = list(ast.find_all(sqlglot.exp.Table))
                    for table in tables:
                        if (len(table.parts) == 1 and self.credentials.get('schema') == "information_schema"
                                or len(table.parts) > 1 and isinstance(table.parts[-2].this, str) and
                                table.parts[-2].this.lower() == 'information_schema'
                                or len(table.parts) > 2 and isinstance(table.parts[-3].this, str)
                                and table.parts[-3].this.lower() == "snowflake"):
                            last_compute = None
                            break

                    if last_compute is not None:
                        locations = {}
                        try:
                            locations = self.catalog.get_table_references(self.duckdb, tables)
                        except SnowflakeError as e:
                            local_error_message = e.message

                        try:
                            transformed_ast = self.sync_duckdb_catalog(locations,
                                                                       simplify(ast)) if locations is not None else None
                        except Exception as e:
                            transformed_ast = e
                            local_error_message = f"Unable to sync DuckDB catalog. {str(e)}"

                        if transformed_ast is None:
                            last_compute = None
                            break

                        sql = transformed_ast.sql(dialect="duckdb", pretty=True)
                        try:
                            logger.info(f"[{self.token}] executing DuckDB query:\n{prepend_to_lines(sql)}")
                            self.duckdb_emulator.execute(sql)
                            last_compute = Compute.LOCAL
                        except duckdb.Error as e:
                            local_error_message = f"Unable to run the parse locally on DuckDB. {e.args}"
                            last_compute = None
                            break
                        except DatabaseError as e:
                            local_error_message = f"Unable to run the query locally on DuckDB. {e.msg}"
                            last_compute = None
                            break

        if last_compute == Compute.SNOWFLAKE:
            return self.get_snowflake_result()
        elif last_compute == Compute.LOCAL:
            performance_counter = time.perf_counter()
            cost = calculate_script_cost(performance_counter - start_time)
            logger.info(f"[{self.token}] Run locally 🚀 ({get_friendly_time_since(start_time, performance_counter)}, {cost})")
            return self.get_duckdb_result()
        elif last_compute is None:
            if local_error_message is not None:
                logger.error(f"[{self.token}] {local_error_message}")
            self.do_snowflake_query(queries, raw_query, start_time, local_error_message)
            return self.get_snowflake_result()
        else:
            raise SnowflakeError(self.token, f"Unsupported compute type. {last_compute}")

    def do_snowflake_query(self, queries, raw_query, start_time, local_error_message):
        try:
            self.catalog_cursor.execute(queries, raw_query)
            logger.info(f"[{self.token}] Query is done. ({get_friendly_time_since(start_time, time.perf_counter())})")
        except SnowflakeError as e:
            final_error = f"[Snowflake]: {e.message}"
            if local_error_message:
                final_error += f"\n[DuckDB]: {local_error_message}"
            if self.compute == Compute.LOCAL.value:
                final_error = "The query without warehouse failed to run remotely, and the compute is set to LOCAL.\n" + final_error

            cloud_logger.error(f"[{self.token}] {final_error}")
            raise SnowflakeError(self.token, final_error, e.sql_state)

    def do_query(self, raw_query: str) -> (str, List, pyarrow.Table):
        logger.info(f"[{self.token}] Executing \n{prepend_to_lines(raw_query)}")
        self.processing = True
        try:
            return self._do_query(raw_query)
        finally:
            self.processing = False

    def close(self):
        self.duckdb_emulator.close()
        self.catalog_cursor.close()

    def get_field_from_duckdb(self, column: list[str], arrow_table: Table, idx: int) -> typing.Tuple[
        Optional[ChunkedArray], pa.Field]:
        (field_name, field_type) = column[0], column[1]
        pa_type = arrow_table.schema[idx].type

        metadata = {}
        value = arrow_table[idx]

        if field_type == 'NUMBER':

            if (  # no harm for int types
                    pa_type != pa.int64() and
                    pa_type != pa.int32() and
                    pa_type != pa.int16() and
                    pa_type != pa.int8()):
                pa_type = pa.decimal128(getattr(value.type, 'precision', 38), getattr(value.type, 'scale', 0))
            value = value.cast(pa_type)
            metadata["logicalType"] = "FIXED"
            metadata["precision"] = "1"
            metadata["scale"] = "0"
            metadata["physicalType"] = "SB1"
            metadata["final_type"] = "T"
        elif field_type == 'Date':
            pa_type = pa.date32()
            value = value.cast(pa_type)
            metadata["logicalType"] = "DATE"
        elif field_type == 'Time':
            pa_type = pa.int64()
            value = value.cast(pa_type)
            metadata["logicalType"] = "TIME"
        elif field_type == "BINARY":
            pa_type = pa.binary()
            metadata["logicalType"] = "BINARY"
        elif field_type == "TIMESTAMP" or field_type == "DATETIME" or field_type == "TIMESTAMP_LTZ":
            metadata["logicalType"] = "TIMESTAMP_LTZ"
            metadata["precision"] = "0"
            metadata["scale"] = "9"
            metadata["physicalType"] = "SB16"
            metadata["final_type"] = "T"
            timestamp_fields = [
                pa.field("epoch", nullable=False, type=pa.int64(), metadata=metadata),
                pa.field("fraction", nullable=False, type=pa.int32(), metadata=metadata),
            ]
            pa_type = pa.struct(timestamp_fields)
            epoch = pa.compute.divide(value.cast(pa.int64()), 1_000_000_000).combine_chunks()
            value = pa.StructArray.from_arrays(arrays=[epoch, pa.nulls(len(value), type=pa.int32())],
                                               fields=timestamp_fields)
        elif field_type == "TIMESTAMP_NTZ":
            metadata["logicalType"] = "TIMESTAMP_NTZ"
            metadata["precision"] = "0"
            metadata["scale"] = "9"
            metadata["physicalType"] = "SB16"
            timestamp_fields = [
                pa.field("epoch", nullable=False, type=pa.int64(), metadata=metadata),
                pa.field("fraction", nullable=False, type=pa.int32(), metadata=metadata),
            ]
            pa_type = pa.struct(timestamp_fields)
            epoch = pa.compute.divide(value.cast(pa.int64()), 1_000_000_000).combine_chunks()
            value = pa.StructArray.from_arrays(arrays=[epoch, pa.nulls(len(value), type=pa.int32())],
                                               fields=timestamp_fields)
        elif field_type == "TIMESTAMP_TZ":
            timestamp_fields = [
                pa.field("epoch", nullable=False, type=pa.int64(), metadata=metadata),
                pa.field("fraction", nullable=False, type=pa.int32(), metadata=metadata),
                pa.field("timezone", nullable=False, type=pa.int32(), metadata=metadata),
            ]
            pa_type = pa.struct(timestamp_fields)
            epoch = pa.compute.divide(value.cast(pa.int64()), 1_000_000_000).combine_chunks()

            value = pa.StructArray.from_arrays(
                arrays=[epoch,
                        # TODO: modulos 1_000_000_000 to get the fraction of a second, pyarrow doesn't support the operator yet
                        pa.nulls(len(value), type=pa.int32()),
                        # TODO: reverse engineer the timezone conversion
                        pa.nulls(len(value), type=pa.int32()),
                        ],
                fields=timestamp_fields)
            metadata["logicalType"] = "TIMESTAMP_TZ"
            metadata["precision"] = "0"
            metadata["scale"] = "9"
            metadata["physicalType"] = "SB16"
        elif field_type == "JSON":
            pa_type = pa.utf8()
            metadata["logicalType"] = "OBJECT"
            metadata["charLength"] = "16777216"
            metadata["byteLength"] = "16777216"
            metadata["scale"] = "0"
            metadata["precision"] = "38"
            metadata["finalType"] = "T"
        elif pa_type == pa.bool_():
            metadata["logicalType"] = "BOOLEAN"
        elif field_type == 'list':
            pa_type = pa.utf8()
            arrow_to_project = self.duckdb.from_arrow(arrow_table.select([field_name]))
            metadata["logicalType"] = "ARRAY"
            metadata["charLength"] = "16777216"
            metadata["byteLength"] = "16777216"
            metadata["scale"] = "0"
            metadata["precision"] = "38"
            metadata["finalType"] = "T"
            value = (arrow_to_project.project(f"to_json({field_name})").arrow())[0]
        elif pa_type == pa.string():
            metadata["logicalType"] = "TEXT"
            metadata["charLength"] = "16777216"
            metadata["byteLength"] = "16777216"
        else:
            raise Exception()

        field = pa.field(field_name, type=pa_type, nullable=True, metadata=metadata)
        return value, field

    def get_duckdb_result(self):
        arrow_table = self.duckdb_emulator._arrow_table
        if arrow_table is None:
            raise SnowflakeError(self.token, "No result returned from DuckDB")
        for idx, column in enumerate(self.duckdb.description):
            array, schema = self.get_field_from_duckdb(column, arrow_table, idx)
            arrow_table = arrow_table.set_column(idx, schema, array)
        return "arrow", get_columns_for_duckdb(arrow_table.schema), arrow_table

    def get_snowflake_result(self):
        arrow = self.catalog_cursor.get_as_table()
        columns = self.catalog_cursor.get_v1_columns()
        return "arrow", columns, arrow


def fix_snowflake_to_duckdb_types(
        expression: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
    if isinstance(expression, sqlglot.exp.DataType):
        if expression.this.value in ["TIMESTAMPLTZ", "TIMESTAMPTZ"]:
            return sqlglot.exp.DataType.build("TIMESTAMPTZ")
        if expression.this.value in ["VARIANT"]:
            return sqlglot.exp.DataType.build("JSON")

    return expression
