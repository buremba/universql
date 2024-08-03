import datetime
import json
import logging
import os
import time
from typing import List

import click
import duckdb
import pyarrow
import pyarrow as pa
import sqlglot
from pyarrow import DataType
from snowflake.connector import DatabaseError

from universql.catalog import get_catalog
from universql.catalog.snow.show_iceberg_tables import cloud_logger
from universql.lake.cloud import register_data_lake
from universql.util import get_columns_for_duckdb, SnowflakeError, Compute, Catalog

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("🐥")

context = click.get_current_context()
COMPUTE = context.params.get('compute')
CATALOG = context.params.get('catalog')
con = duckdb.connect(read_only=False, config={
    'max_memory': context.params.get('max_memory'),
    'temp_directory': os.path.join(context.params.get('cache_directory'), "duckdb-staging"),
    'max_temp_directory_size': context.params.get('max_cache_size'),
})
con.install_extension("iceberg")
con.load_extension("iceberg")


def apply_transformation(arrow):
    for idx, field in enumerate(arrow.schema):
        if pyarrow.types.is_int64(field.type):
            new_type = pa.decimal128(38, 0)
            pa_field = pa.field(field.name, type=new_type, nullable=field.nullable,
                                metadata=field.metadata)
            arrow = arrow.set_column(idx, pa_field, arrow[idx].cast(new_type))
        if pyarrow.types.is_timestamp(field.type):
            pa_field = pa.field(field.name, type=pa.int64(), nullable=field.nullable,
                                metadata=field.metadata)
            cast = pa.compute.divide(arrow[idx].cast(pa.int64()), 1000000)
            arrow = arrow.set_column(idx, pa_field, cast)
        if pyarrow.types.is_time(field.type):
            pa_field = pa.field(field.name, type=pa.int64(), nullable=field.nullable,
                                metadata=field.metadata)
            cast = arrow[idx].cast(pa.int64())
            arrow = arrow.set_column(idx, pa_field, cast)
    return arrow


locally_supported_queries = ["select", "union", "join"]


class UniverSQLSession:
    def __init__(self, token, credentials: dict, session_parameters: dict):
        self.credentials = credentials
        self.session_parameters = [{"name": item[0], "value": item[1]} for item in session_parameters.items()]
        self.token = token
        self.catalog = get_catalog(context.params, self.token,
                                   self.credentials)
        self.duckdb = con.cursor()
        self.snowflake = self.catalog.cursor()
        register_data_lake(self.duckdb, context.params)
        self.processing = False

    def get_duckdb_transformer(self, tables: List[sqlglot.exp.Expression]):
        locations = self.catalog.get_table_references(self.duckdb, tables)

        def replace_icebergs_with_duckdb_reference(
                expression: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
            if isinstance(expression, sqlglot.exp.Table):
                if expression.name != "":
                    return locations[expression]
                else:
                    return expression

            return expression

        return replace_icebergs_with_duckdb_reference

    def do_query(self, query: str) -> (str, List, pyarrow.Table):
        self.processing = True
        try:
            logger.info("[%s] Executing \n%s" % (self.token, query))
            start_time = time.perf_counter()
            queries = sqlglot.parse(query, read="snowflake")
            last_run_on_duckdb = False

            for ast in queries:
                can_run_locally = ast.key in locally_supported_queries

                passthrough_message = f"Can't run the query locally. " \
                                      f"Only {', '.join(locally_supported_queries)} queries are supported."
                if COMPUTE == Compute.LOCAL.value and not not can_run_locally:
                    raise SnowflakeError(self.token, passthrough_message)

                if can_run_locally:
                    tables = list(ast.find_all(sqlglot.exp.Table))
                    try:
                        transformer = self.get_duckdb_transformer(tables)
                        duckdb_query = ast.transform(transformer)

                        try:
                            sql = duckdb_query.sql(dialect="duckdb")
                            planned_duration = time.perf_counter() - start_time
                            timedelta = datetime.timedelta(seconds=planned_duration)

                            logger.info("[%s] Re-written as: (%s)\n%s" % (self.token, timedelta, sql))
                            self.duckdb.execute(sql)
                            last_run_on_duckdb = True
                            continue
                        except duckdb.Error as e:
                            if COMPUTE == Compute.LOCAL.value:
                                raise SnowflakeError(self.token, json.dumps(e.args),
                                                     getattr(e, 'sqlstate', None))
                            logger.warning("Unable to run DuckDB query locally. ")
                    except DatabaseError as e:
                        error_message = (f"[{self.token}] Unable to find location of Iceberg tables. "
                                         f"See: https://github.com/buremba/universql#cant-query-native-snowflake-tables. Cause: {e.msg}")
                        if COMPUTE == Compute.LOCAL.value:
                            raise SnowflakeError(self.token, error_message, e.sqlstate)
                        else:
                            cloud_logger.warning(error_message)
                else:
                    logger.info(f"[{self.token}] {passthrough_message}")

                if CATALOG == Catalog.POLARIS.value:
                    raise SnowflakeError(self.token,
                                         "Polaris catalog only supports read-only queries and DuckDB query is failed. "
                                         "Unable to run the query.")
                elif CATALOG == Catalog.SNOWFLAKE.value:
                    try:
                        self.snowflake.execute(ast)
                        last_run_on_duckdb = False
                    except SnowflakeError as e:
                        cloud_logger.error(f"[{self.token}] {e.message}")
                        raise SnowflakeError(self.token, e.message, e.sql_state)

            end_time = time.perf_counter() - start_time
            formatting = (self.token, datetime.timedelta(seconds=end_time))
            if last_run_on_duckdb:
                result = self.get_duckdb_result()
                logger.info("[%s] Run locally 🚀 (%s)" % formatting)
            else:
                result = self.get_snowflake_result()
                logger.info("[%s] Query is done. (%s)" % formatting)
        finally:
            self.processing = False

        return result

    def close(self):
        self.duckdb.close()
        self.snowflake.close()

    @staticmethod
    def get_field_for_duckdb(column: list[str], arrow_type: DataType) -> pa.Field:
        (field_name, field_type) = column[0], column[1]
        pa_type = arrow_type

        metadata = {}

        if field_type == 'NUMBER':
            metadata["logicalType"] = "FIXED"
            metadata["precision"] = "1"
            metadata["scale"] = "0"
            metadata["physicalType"] = "SB1"
        elif field_type == 'Date':
            pa_type = pa.date32()
            metadata["logicalType"] = "DATE"
        elif field_type == pa.binary():
            metadata["logicalType"] = "BINARY"
        elif field_type == "TIMESTAMP" or field_type == "DATETIME" or field_type == "TIMESTAMP_LTZ":
            metadata["logicalType"] = "TIMESTAMP_LTZ"
            metadata["precision"] = "0"
            metadata["scale"] = "9"
            metadata["physicalType"] = "SB16"
        elif field_type == "TIMESTAMP_NTZ":
            metadata["logicalType"] = "TIMESTAMP_NTZ"
            metadata["precision"] = "0"
            metadata["scale"] = "9"
            metadata["physicalType"] = "SB16"
        elif field_type == "TIMESTAMP_TZ":
            metadata["logicalType"] = "TIMESTAMP_TZ"
            metadata["precision"] = "0"
            metadata["scale"] = "9"
            metadata["physicalType"] = "SB16"
        elif arrow_type == pa.string():
            metadata["logicalType"] = "TEXT"
            metadata["charLength"] = "8388608"
            metadata["byteLength"] = "8388608"
        elif arrow_type == pa.bool_():
            metadata["logicalType"] = "BOOLEAN"
        else:
            raise Exception()

        return pa.field(field_name, type=pa_type, nullable=True, metadata=metadata)

    def get_duckdb_result(self):
        arrow_table = self.duckdb.fetch_arrow_table()
        schema = pa.schema(
            [self.get_field_for_duckdb(column, arrow_table.schema[idx].type)
             for idx, column in enumerate(self.duckdb.description)])
        table = arrow_table.cast(schema)
        return "arrow", get_columns_for_duckdb(table.schema), apply_transformation(table)

    def get_snowflake_result(self):
        arrow = self.snowflake.get_as_table()
        columns = self.snowflake.get_v1_columns()
        transformed_arrow = apply_transformation(arrow)
        return "arrow", columns, transformed_arrow
