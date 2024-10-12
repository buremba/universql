import logging
import os
import typing
from typing import List

import duckdb
import snowflake
import sqlglot
from fakesnow.conn import FakeSnowflakeConnection
from fakesnow.cursor import FakeSnowflakeCursor
from pyiceberg.catalog import load_catalog, PY_CATALOG_IMPL
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import PY_IO_IMPL, WAREHOUSE
from snowflake.connector.options import pyarrow
from sqlglot.expressions import Select, Insert, Create, Drop, Properties, TemporaryProperty, Schema, Table

from universql.warehouse import ICatalog, Executor, IcebergTable, Locations, CreateRelation, Location
from universql.lake.cloud import s3, gcs, CACHE_DIRECTORY_KEY
from universql.util import prepend_to_lines, QueryError, calculate_script_cost
from universql.protocol.utils import DuckDBFunctions, get_field_from_duckdb
from sqlglot.optimizer.simplify import simplify

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("🐥")


class DuckDBCatalog(ICatalog):

    def register_locations(self, tables: Locations):
        pass

    def __init__(self, context: dict, query_id: str, credentials: dict, compute: dict):
        super().__init__(context, query_id, credentials, compute)
        duckdb_path = context.get('database_path')
        duck_config = {
            'max_memory': context.get('max_memory'),
            'temp_directory': os.path.join(context.get('cache_directory'), "duckdb-staging"),
            'max_temp_directory_size': context.get('max_cache_size'),
        }
        if os.access(context.get('home_directory'), os.W_OK | os.X_OK):
            duck_config["home_directory"] = context.get('home_directory')
        else:
            duck_config["access_mode"] = 'READ_ONLY'

        try:
            self.duckdb = duckdb.connect(duckdb_path, read_only=duck_config.get("access_mode") == 'READ_ONLY' and duckdb_path != ':memory:',
                                         config=duck_config)
        except duckdb.InvalidInputException as e:
            raise QueryError(f"Unable to spin up DuckDB ({duckdb_path}) with config {duck_config}: {e}")
        DuckDBFunctions.register(self.duckdb)
        self.duckdb.install_extension("iceberg")
        self.duckdb.load_extension("iceberg")
        iceberg_catalog = context.get('iceberg_catalog')
        if iceberg_catalog is not None:
            catalog_name = iceberg_catalog
            catalog_props = {}
        else:
            catalog_name = None
            catalog_props = {
                PY_IO_IMPL: "universql.lake.cloud.iceberg",
                WAREHOUSE: "gs://my-iceberg-data/custom-events/customer_iceberg_pyiceberg",
                PY_CATALOG_IMPL: "pyiceberg.catalog.sql.SqlCatalog",
                CACHE_DIRECTORY_KEY: context.get('cache_directory'),
                "uri": "sqlite:////Users/bkabak/Code/universql/tests/test.db",
                # "echo": "true"
            }
        self.iceberg_catalog = load_catalog(catalog_name, **catalog_props)

        fake_snowflake_conn = FakeSnowflakeConnection(self.duckdb, "main", "public", False, False)
        fake_snowflake_conn.database_set = True
        fake_snowflake_conn.schema_set = True
        self.emulator = FakeSnowflakeCursor(fake_snowflake_conn, self.duckdb)
        self._register_data_lake(context)

    def _register_data_lake(self, args: dict):
        self.duckdb.register_filesystem(s3(args.get('cache_directory'), args.get('aws_profile')))
        self.duckdb.register_filesystem(gcs(args.get('cache_directory'), args.get('gcp_project')))

    def get_table_paths(self, tables: List[sqlglot.exp.Table]):
        pass

    def executor(self) -> Executor:
        return DuckDBExecutor(self)


class DuckDBExecutor(Executor):

    def __init__(self, duckdb: DuckDBCatalog):
        self.catalog = duckdb

    def get_query_log(self, total_duration) -> str:
        cost = calculate_script_cost(total_duration)
        return f"Run locally on DuckDB: {cost}"

    def supports(self, ast: sqlglot.exp.Expression) -> bool:
        return isinstance(ast, Select) or isinstance(ast, Insert) or isinstance(ast, Create)

    def execute_raw(self, raw_query: str) -> None:
        try:
            self.catalog.emulator.execute(raw_query)
        except duckdb.Error as e:
            raise QueryError(f"Unable to run the parse locally on DuckDB. {e.args}")
        except duckdb.duckdb.DatabaseError as e:
            raise QueryError(f"Unable to run the query locally on DuckDB. {e.msg}")
        except snowflake.connector.ProgrammingError as e:
            raise QueryError(f"Unable to run the query locally on DuckDB. {e.msg}")

    def _get_iceberg_ref(self, default_namespace, destination_table, ):
        namespace = '.'.join([part.sql() for part in destination_table.parts[0:-1]])
        if namespace == '':
            namespace = default_namespace
            table_ref = f"{default_namespace}.{destination_table.sql()}"
        else:
            namespace = namespace
            table_ref = destination_table.sql()
        return namespace, table_ref

    def _sync_catalog(self, ast: sqlglot.exp.Expression,
                      tables_getter: typing.Callable[[], Locations]) -> sqlglot.exp.Expression:
        locations = tables_getter()
        return self._sync_duckdb_catalog(locations,
                                         simplify(
                                             ast)) if locations is not None else None

    def execute(self, ast: sqlglot.exp.Expression, tables_getter: typing.Callable[[], Locations]) -> \
            typing.Optional[Locations]:

        if isinstance(ast, Create) or isinstance(ast, Insert):
            destination_table = ast.this
            if not isinstance(destination_table, Table):
                if isinstance(destination_table, Schema):
                    destination_table = destination_table.this
                    # filter columns
                else:
                    raise QueryError(f"CREATE {ast.kind} is not supported in DuckDB")

            (catalog, schema, table) = destination_table.parts
            catalog = sqlglot.exp.parse_identifier(catalog or self.catalog.credentials.get('database')).sql()
            schema = sqlglot.exp.parse_identifier(schema or self.catalog.credentials.get('schema')).sql()

            if isinstance(ast, Create):
                if ast.kind == 'TABLE':
                    properties = destination_table.args.get('properties') or Properties()
                    is_temp = TemporaryProperty() in properties.expressions

                    self.catalog.iceberg_catalog.create_namespace_if_not_exists((catalog, schema))
                    self.execute_raw(self._sync_catalog(ast.expression, tables_getter).sql(dialect="duckdb"))
                    arrow_table = self.get_as_table()
                    create_iceberg_table = self.catalog.iceberg_catalog.create_table_if_not_exists(table.sql(),
                                                                                                   arrow_table.schema)
                    create_iceberg_table.append(arrow_table)

                    if is_temp:
                        self.catalog.duckdb.register(destination_table.sql(), arrow_table)
                    else:
                        return {destination_table: IcebergTable(create_iceberg_table.metadata_location)}
                elif ast.kind == 'VIEW':
                    properties = destination_table.args.get('properties') or Properties()
                    is_temp = TemporaryProperty() in properties.expressions
                    duckdb_query = self._sync_catalog(ast, tables_getter).expression.sql(dialect="duckdb")
                    self.execute_raw(duckdb_query)

                    if not is_temp:
                        return {destination_table: CreateRelation(ast.args.get('properties'), ast.kind, ast.expression)}

            elif isinstance(ast, Insert):
                try:
                    iceberg_table = self.catalog.iceberg_catalog.load_table((catalog, schema, table.sql()))
                except NoSuchTableError as e:
                    raise QueryError(f"Error accessing catalog {e.args}")
                self.execute_raw(self._sync_catalog(ast.expression, tables_getter).sql(dialect="duckdb"))
                table = self.get_as_table()
                iceberg_table.append(table)

        elif isinstance(ast, Drop):
            delete_table = ast.this
            self.catalog.iceberg_catalog.drop_table(delete_table.sql())
        else:
            sql = self._sync_catalog(ast, tables_getter).sql(dialect="duckdb", pretty=True)
            self.execute_raw(sql)

        return None

    def get_as_table(self) -> pyarrow.Table:
        arrow_table = self.catalog.emulator._arrow_table
        if arrow_table is None:
            raise QueryError("No result returned from DuckDB")
        for idx, column in enumerate(self.catalog.duckdb.description):
            array, schema = get_field_from_duckdb(column, arrow_table, idx)
            arrow_table = arrow_table.set_column(idx, schema, array)
        return arrow_table

    def close(self):
        self.catalog.emulator.close()

    def _sync_duckdb_catalog(self, locations: typing.Dict[sqlglot.exp.Table, str], ast: sqlglot.exp.Expression) -> \
            typing.Optional[
                sqlglot.exp.Expression]:

        schemas = set((table.catalog, table.db) for table in locations.keys() if table.db != '')
        databases = set(table[0] for table in schemas if table[0] != '')

        databases_sql = [f"ATTACH IF NOT EXISTS ':memory:' AS {sqlglot.exp.parse_identifier(database).sql()}" for
                         database in
                         databases]
        schemas_sql = [
            f"CREATE SCHEMA IF NOT EXISTS {sqlglot.exp.parse_identifier(db).sql()}.{sqlglot.exp.parse_identifier(schema).sql()}"
            for
            (db, schema) in schemas]
        views_sql = [f"CREATE OR REPLACE VIEW {table.sql()} AS SELECT * FROM {self.get_iceberg_read(location)};" for
                     table, location
                     in locations.items()]
        views_sql = ";\n".join(databases_sql + schemas_sql + views_sql)

        if views_sql != "":
            logger.info(
                f"[{self.catalog.query_id}] DuckDB environment is setting up:\n{prepend_to_lines(views_sql)}")
            try:
                self.catalog.duckdb.execute(views_sql)
            except duckdb.Error as e:
                raise QueryError(f"Unable to sync DuckDB catalog. {e.args}")
                pass

        # def replace_icebergs_with_duckdb_reference(
        #         expression: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
        #     if isinstance(expression, sqlglot.exp.Table):
        #         if expression.name != "":
        #             new_table = sqlglot.exp.to_table(f"main.{sqlglot.exp.parse_identifier(expression.sql())}")
        #             return new_table
        #             # return locations[expression]
        #         else:
        #             return expression
        #
        #     return expression

        return (ast
                # .transform(replace_icebergs_with_duckdb_reference)
                .transform(self.fix_snowflake_to_duckdb_types))

    @staticmethod
    def fix_snowflake_to_duckdb_types(
            expression: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
        if isinstance(expression, sqlglot.exp.DataType):
            if expression.this.value in ["TIMESTAMPLTZ", "TIMESTAMPTZ"]:
                return sqlglot.exp.DataType.build("TIMESTAMPTZ")
            if expression.this.value in ["VARIANT"]:
                return sqlglot.exp.DataType.build("JSON")

        return expression

    @staticmethod
    def get_iceberg_read(location: Location) -> str:
        if isinstance(location, IcebergTable):
            return sqlglot.exp.func("iceberg_scan",
                                    sqlglot.exp.Literal.string(location.location)).sql()

        raise QueryError("Unsupported op")
