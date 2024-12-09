import logging
import os
import typing
from enum import Enum
from string import Template
from typing import List

import duckdb
import pyiceberg.table
import snowflake
import sqlglot
from fakesnow import macros, info_schema
from fakesnow.conn import FakeSnowflakeConnection
from fakesnow.cursor import FakeSnowflakeCursor
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import LOCATION
from snowflake.connector.options import pyarrow
from sqlglot.expressions import Select, Insert, Create, Drop, Properties, TemporaryProperty, Schema, Table, Property, \
    Var, Literal, IcebergProperty, Copy, Delete, Merge

from universql.warehouse import ICatalog, Executor, Locations, Tables
from universql.lake.cloud import s3, gcs, in_lambda
from universql.util import prepend_to_lines, QueryError, calculate_script_cost, parse_snowflake_account, fill_qualifier
from universql.protocol.utils import DuckDBFunctions, get_field_from_duckdb
from sqlglot.optimizer.simplify import simplify

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("🐥")


class TableType(Enum):
    ICEBERG = "iceberg"
    LOCAL = "local"


class DuckDBCatalog(ICatalog):

    def __init__(self, context: dict, session_id: str, credentials: dict, compute: dict, iceberg_catalog: Catalog):
        super().__init__(context, session_id, credentials, compute, iceberg_catalog)
        duck_config = {
            'max_memory': context.get('max_memory'),
            'temp_directory': os.path.join(context.get('cache_directory'), "duckdb-staging"),
            # 'lock_configuration': 'true',
            'enable_external_access': 'true',
        }
        if context.get('max_cache_size') != "0":
            duck_config['max_temp_directory_size'] = context.get('max_cache_size')
        self.account = parse_snowflake_account(context.get('account'))
        database_path = self.context.get('database_path')
        if database_path is not None:
            database = Template(database_path).substitute({'session_id': session_id})
        else:
            database = ":memory:"

        try:
            self.duckdb = duckdb.connect(database, config=duck_config)
        except duckdb.InvalidInputException as e:
            raise QueryError(f"Unable to spin up DuckDB with config {duck_config}: {e}")
        DuckDBFunctions.register(self.duckdb)
        self.duckdb.install_extension("iceberg")
        self.duckdb.load_extension("iceberg")
        motherduck_token = self.context.get('motherduck_token')
        if motherduck_token is not None:
            self.duckdb.execute("ATTACH 'md:'")

        fake_snowflake_conn = FakeSnowflakeConnection(self.duckdb, self.credentials.get('database'),
                                                      self.credentials.get('schema'),
                                                      False, False)
        fake_snowflake_conn.database_set = True
        fake_snowflake_conn.schema_set = True
        self.emulator = FakeSnowflakeCursor(fake_snowflake_conn, self.duckdb)
        self._register_data_lake(context)

    def _get_table_location(self, table: sqlglot.exp.Table) -> typing.Optional[TableType]:
        def get_identifier(is_quoted):
            # return '?' if is_quoted else "upper(?)"
            return "upper(?)"

        database = get_identifier(table.parts[0].quoted)
        schema = get_identifier(table.parts[1].quoted)
        table_name = get_identifier(table.parts[2].quoted)
        table_relation = self.duckdb.sql(f"""
            with selected_table as (
              select * from {table.parts[0].sql()}.information_schema.tables 
                where upper(table_catalog) = {database} and upper(table_schema) = {schema} and upper(table_name) = {table_name} 
              ) 
            select sql from selected_table 
            left join duckdb_views() on 
                view_name = selected_table.table_name and
                selected_table.table_type = 'VIEW' and internal = false
        """, params=(table.catalog, table.db, table.name))
        table_exists = table_relation.fetchone()
        if table_exists is None:
            return None
        if table_exists[0] is None:
            return TableType.LOCAL
        plan = self.duckdb.get_substrait_json(table_exists[0])

    def register_locations(self, tables: Locations):
        raise Exception("Unsupported operation")

    def _register_data_lake(self, args: dict):
        if args.get('aws_profile') is not None or self.account.cloud == 'aws':
            self.duckdb.register_filesystem(s3(args))
        if args.get('gcp_project') is not None or self.account.cloud == 'gcp':
            self.duckdb.register_filesystem(gcs(args))

    def get_table_paths(self, tables: List[sqlglot.exp.Table]) -> Tables:
        native_tables = {}
        for table in tables:
            table_location = self._get_table_location(table)
            if table_location == TableType.LOCAL:
                native_tables[table] = None
            elif table_location == TableType.ICEBERG:
                native_tables[table] = None
        return native_tables

    def executor(self) -> Executor:
        return DuckDBExecutor(self)


class DuckDBExecutor(Executor):

    def __init__(self, catalog: DuckDBCatalog):
        super().__init__(catalog)

    def get_query_log(self, total_duration) -> str:
        if in_lambda:
            memory_size = os.environ['AWS_LAMBDA_FUNCTION_MEMORY_SIZE']
            execution_env = os.environ['AWS_EXECUTION_ENV']
            return f"Run DuckDB in AWS Lambda: memory:{memory_size} platform:{execution_env}"
        else:
            cost = calculate_script_cost(total_duration)
            return f"Run locally on DuckDB: {cost}"

    def supports(self, ast: sqlglot.exp.Expression) -> bool:
        return isinstance(ast, Select) or isinstance(ast, Insert) or isinstance(ast, Create) or isinstance(ast,
                                                                                                           Copy) or isinstance(
            ast, Delete) or isinstance(ast, Merge)

    def execute_raw(self, raw_query: str) -> None:
        try:
            logger.info(
                f"[{self.catalog.session_id}] Executing query on DuckDB:\n{prepend_to_lines(raw_query)}")
            self.catalog.emulator.execute(raw_query)
        except duckdb.Error as e:
            raise QueryError(f"Unable to run the query locally on DuckDB. {e.args}")
        except duckdb.duckdb.DatabaseError as e:
            raise QueryError(f"Unable to run the query locally on DuckDB. {e.msg}")
        except snowflake.connector.ProgrammingError as e:
            raise QueryError(f"Unable to run the query locally on DuckDB. {e.msg}")

    def _register_db(self, db_name):
        if self.catalog.context.get('motherduck_token') is not None:
            return f"CREATE DATABASE IF NOT EXISTS {sqlglot.exp.parse_identifier(db_name).sql()}"
        else:
            database_path = self.catalog.context.get('database_path')
            if database_path is not None:
                duckdb_path = os.path.join(database_path, f'{db_name}.duckdb')
            else:
                duckdb_path = ':memory:'
            return f'ATTACH IF NOT EXISTS \'{duckdb_path}\' AS {sqlglot.exp.parse_identifier(db_name).sql()}'

    def _sync_catalog(self, ast: sqlglot.exp.Expression, locations: Tables) -> sqlglot.exp.Expression:
        default_database = self.catalog.credentials.get('database')
        default_schema = self.catalog.credentials.get('schema')
        schemas = set((table.catalog or default_database,
                       table.db or default_schema)
                      for table in locations.keys())
        databases = set(table[0] for table in schemas if table[0] != '')

        setup_query = []
        if default_database is not None and default_schema is not None:
            setup_query.append(
                f"SET schema={sqlglot.exp.parse_identifier(default_database).sql()}.{sqlglot.exp.parse_identifier(default_schema).sql()}")
            databases.add(default_database)
            schemas.add((default_database, default_schema))

        databases_sql = [
            f"{self._register_db(database)};" +
            info_schema.creation_sql(sqlglot.exp.parse_identifier(database).sql()) +
            macros.creation_sql(sqlglot.exp.parse_identifier(database).sql())
            for
            database in
            databases]
        schemas_sql = [
            f"CREATE SCHEMA IF NOT EXISTS {sqlglot.exp.parse_identifier(db).sql()}.{sqlglot.exp.parse_identifier(schema).sql()}"
            for
            (db, schema) in schemas]

        views_sql = [f"CREATE OR REPLACE VIEW {table.sql()} AS SELECT * FROM {self.get_iceberg_read(location)}" for
                     table, location
                     in locations.items() if location is not None]
        views_sql = ";\n".join(databases_sql + schemas_sql + setup_query + views_sql)

        if views_sql != "":
            logger.info(
                f"[{self.catalog.session_id}] DuckDB environment is setting up:\n{views_sql}")
            try:
                self.catalog.duckdb.execute(views_sql)
            except duckdb.Error as e:
                raise QueryError(f"Unable to sync DuckDB catalog. {e.args}")

        final_ast = (simplify(ast)
                     .transform(self.fix_snowflake_to_duckdb_types))

        return final_ast

    def _get_property(self, ast: sqlglot.exp.Create, name: str):
        if "properties" not in ast.args:
            return None
        return next(
            (expression.args.get('value').this for expression in ast.args['properties'].expressions
             if isinstance(expression, Property) and isinstance(expression.this,
                                                                Var) and expression.this.this.casefold() == name.casefold()),
            None)

    def execute(self, ast: sqlglot.exp.Expression, tables: Tables) -> typing.Optional[Locations]:
        if isinstance(ast, Create) or isinstance(ast, Insert):
            if isinstance(ast.this, Schema):
                destination_table = ast.this.this
                # filter columns
            elif not isinstance(ast.this, Table):
                raise QueryError("Query type is not supported in DuckDB")
            else:
                destination_table = ast.this

            destination_table = fill_qualifier(destination_table, self.catalog.credentials)
            full_table = destination_table.sql()

            if isinstance(ast, Create):
                namespace = self.catalog.iceberg_catalog.properties.get('namespace')
                raw_table = destination_table.parts[-1].sql()
                properties = ast.args.get('properties') or Properties()
                is_temp = TemporaryProperty() in properties.expressions
                is_iceberg = IcebergProperty() in properties.expressions

                if ast.kind == 'TABLE':
                    is_replace = ast.args.get('replace')
                    if_exists = ast.args.get('exists')
                    final_query = self._sync_catalog(ast, tables | {destination_table: None})

                    if is_iceberg:
                        if final_query.expression is not None:
                            self.execute_raw(final_query.expression.sql(dialect="duckdb"))
                            arrow_table = self.get_as_table()
                        else:
                            arrow_table = None
                        database_location = self.catalog.iceberg_catalog.properties.get(LOCATION)
                        base_location = self._get_property(ast, 'base_location')

                        if database_location is not None:
                            database_location = database_location.rstrip("/")
                            table_location = str(os.path.join(database_location, base_location))
                        elif base_location is not None:
                            table_location = str(os.path.join(raw_table, base_location))
                        else:
                            table_location = None
                        if if_exists:
                            create_iceberg_table = self.catalog.iceberg_catalog.create_table_if_not_exists(
                                (namespace, full_table),
                                arrow_table.schema,
                                location=table_location)
                        else:
                            if is_replace:
                                try:
                                    self.catalog.iceberg_catalog.drop_table((namespace, full_table))
                                except NoSuchTableError:
                                    pass
                            create_iceberg_table = self.catalog.iceberg_catalog.create_table((namespace, full_table),
                                                                                             arrow_table.schema,
                                                                                             location=table_location)
                        create_iceberg_table.overwrite(arrow_table)
                        if not create_iceberg_table.metadata_location.startswith(table_location):
                            raise QueryError("Unable to determine location")
                        metadata_file_path = create_iceberg_table.metadata_location[len(table_location):].strip('/')
                        catalog = self._get_property(ast, 'catalog')

                        if catalog is not None and catalog.lower() != 'snowflake':
                            ast.set('expression', None)
                            properties = ast.args.get('properties') or Properties()

                            # adds column definitions to DDL
                            # column_definitions = [ColumnDef(
                            #     this=sqlglot.exp.parse_identifier(column.name),
                            #     kind=DataType.build(str(column.field_type)))
                            #     for column in create_iceberg_table.metadata.schema().columns]
                            # schema = Schema()
                            # schema.set('this', ast.this)
                            # schema.set('expressions', column_definitions)
                            # ast.set('this', schema)
                            properties.expressions.append(
                                Property(this=Var(this='METADATA_FILE_PATH'), value=Literal.string(metadata_file_path)))
                            return {destination_table: ast}
                    else:
                        if is_temp:
                            ast.args['properties'] = Properties(
                                expressions=[expression for expression in properties.expressions if
                                             not isinstance(expression, TemporaryProperty)])

                            self.execute_raw(ast.sql(dialect="duckdb"))
                            return None
                        else:
                            raise QueryError(
                                "DuckDB can't create native Snowflake tables, please use Iceberg or External tables")
                elif ast.kind == 'VIEW':
                    duckdb_query = self._sync_catalog(ast, tables).sql(dialect="duckdb")
                    self.execute_raw(duckdb_query)

                    if not is_temp:
                        return {destination_table: ast.expression}
            elif isinstance(ast, Insert):
                table_type = self.catalog._get_table_location(destination_table)
                if table_type == TableType.ICEBERG:
                    namespace = self.catalog.iceberg_catalog.properties.get('namespace')
                    try:
                        iceberg_table = self.catalog.iceberg_catalog.load_table((namespace, full_table))
                    except NoSuchTableError as e:
                        raise QueryError(f"Error accessing catalog {e.args}")
                    self.execute_raw(self._sync_catalog(ast.expression, tables).sql(dialect="duckdb"))
                    table = self.get_as_table()
                    iceberg_table.append(table)
                elif table_type.LOCAL:
                    self.execute_raw(self._sync_catalog(ast, tables).sql(dialect="duckdb"))
                else:
                    raise QueryError("Unable to determine table type")

        elif isinstance(ast, Drop):
            delete_table = ast.this
            self.catalog.iceberg_catalog.drop_table(delete_table.sql())
        else:
            sql = self._sync_catalog(ast, tables).sql(dialect="duckdb", pretty=True)
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
    def get_iceberg_read(location: pyiceberg.table.Table) -> str:
        return sqlglot.exp.func("iceberg_scan",
                                sqlglot.exp.Literal.string(location.metadata_location)).sql()
