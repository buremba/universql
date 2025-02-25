import logging
import os
import re
import typing
from enum import Enum
from string import Template
from typing import List, Sequence, Any

import duckdb
import fsspec
import pyiceberg.table
import snowflake
import sqlglot
from duckdb.duckdb import NotSupportedError
from fakesnow import macros, info_schema
from fakesnow.conn import FakeSnowflakeConnection
from fakesnow.cursor import FakeSnowflakeCursor
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import LOCATION
from snowflake.connector.options import pyarrow
from sqlglot.expressions import Insert, Create, Drop, Properties, TemporaryProperty, Schema, Table, Property, \
    Var, Literal, IcebergProperty, Use, ColumnDef, DataType, Copy

from universql.lake.cloud import s3, gcs, in_lambda
from universql.protocol.session import UniverSQLSession
from universql.protocol.utils import DuckDBFunctions, get_field_from_duckdb
from universql.util import prepend_to_lines, QueryError, calculate_script_cost, parse_snowflake_account, full_qualifier
from universql.plugin import ICatalog, Executor, Locations, Tables, register

logger = logging.getLogger("🐥")


class TableType(Enum):
    ICEBERG = "iceberg"
    LOCAL = "local"


@register(name="duckdb")
class DuckDBCatalog(ICatalog):

    def __init__(self, session: UniverSQLSession):
        super().__init__(session)
        duck_config = {
            'temp_directory': os.path.join(session.context.get('cache_directory'), "duckdb-staging"),
            # 'lock_configuration': 'true',
            'enable_external_access': 'true',
        }
        if 'max_memory' in session.context:
            duck_config["max_memory"] = session.context.get('max_memory')
        if session.context.get('max_cache_size') != "0" and "max_temp_directory_size" in session.context:
            duck_config['max_temp_directory_size'] = session.context.get('max_cache_size')
        self.account = parse_snowflake_account(session.context.get('account'))
        database_path = self.context.get('database_path')
        if database_path is not None:
            database = Template(database_path).substitute({'session_id': session.session_id})
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
        self.filesystems = self.get_filesystems(session.context)
        for filesystem in self.filesystems:
            self.duckdb.register_filesystem(filesystem)


    def _get_table_location(self, table: sqlglot.exp.Table) -> typing.Optional[TableType]:
        def get_identifier(is_quoted):
            # return '?' if is_quoted else "upper(?)"
            return "upper(?)"

        database = get_identifier(table.parts[0].quoted)
        schema = get_identifier(table.parts[1].quoted)
        table_name = get_identifier(table.parts[2].quoted)
        try:
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
        except duckdb.BinderException as e:
            return None

        if table_exists is None:
            return None
        if table_exists[0] is None:
            return TableType.LOCAL

        match = re.search(r"CREATE VIEW (?:[\"']?[\w]+\.){1,3}[\w]+ AS SELECT \* FROM iceberg_scan\((['\"])([a-zA-Z0-9+.-]+:\/\/[a-zA-Z0-9\/_.-]+)\1\);",
                          table_exists[0])
        if match is not None:
            return TableType.ICEBERG

        raise NotSupportedError(f"The table reference in DuckDB is not supported: {table_exists[0]}")

    def register_locations(self, tables: Locations):
        raise Exception("Unsupported operation")

    def get_filesystems(self, args: dict) -> List[fsspec.AbstractFileSystem]:
        filesystems = []
        if args.get('aws_profile') is not None or self.account.cloud == 'aws':
            filesystems.append(s3(args))
        if args.get('gcp_project') is not None or self.account.cloud == 'gcp':
            filesystems.append(gcs(args))
        return filesystems

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
        self.is_last_raw = False

    def get_query_log(self, total_duration) -> str:
        if in_lambda:
            memory_size = os.environ['AWS_LAMBDA_FUNCTION_MEMORY_SIZE']
            execution_env = os.environ['AWS_EXECUTION_ENV']
            return f"Run DuckDB in AWS Lambda: memory:{memory_size} platform:{execution_env}"
        else:
            cost = calculate_script_cost(total_duration)
            return f"Run locally on DuckDB: {cost}"

    def execute_raw(self, raw_query: str, catalog_executor: Executor, is_raw: bool = False) -> None:
        try:
            logger.info(
                f"[{self.catalog.session_id}] Executing query on DuckDB:\n{prepend_to_lines(raw_query)}")

            if is_raw:
                self.catalog.duckdb.execute(raw_query)
            else:
                self.catalog.emulator.execute(raw_query)
            self.is_last_raw = is_raw
        except duckdb.HTTPException as e:
            if e.status_code == 403:
                raise QueryError(f"Access denied: {e.args[0]}")
            else:
                raise QueryError(f"Error when pulling data from filesystem in DuckDB: {e.args[0]}")
        except duckdb.Error as e:
            raise QueryError(f"Unable to run the query locally on DuckDB. {e.args}")
        except duckdb.duckdb.DatabaseError as e:
            raise QueryError(f"Unable to run the query locally on DuckDB. {str(e)}")
        except snowflake.connector.ProgrammingError as e:
            raise QueryError(f"Unable to run the query locally on DuckDB. {e.msg}")

    def _register_db_sql(self, db_name):
        if self.catalog.context.get('motherduck_token') is not None:
            return f"CREATE DATABASE IF NOT EXISTS {sqlglot.exp.parse_identifier(db_name).sql()}"
        else:
            database_path = self.catalog.context.get('database_path')
            if database_path is not None:
                duckdb_path = os.path.join(database_path, f'{db_name}.duckdb')
            else:
                duckdb_path = ':memory:'
            return f'ATTACH IF NOT EXISTS \'{duckdb_path}\' AS {sqlglot.exp.parse_identifier(db_name).sql()}'

    def _sync_and_transform_query(self, ast: sqlglot.exp.Expression, locations: Tables) -> sqlglot.exp.Expression:
        self._sync_catalog(locations)
        final_ast = (ast
                     .transform(self.fix_snowflake_to_duckdb_types)
                     .transform(self.remove_snowflake_table_references))
        return final_ast

    def _sync_catalog(self, locations: Tables):
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
            f"{self._register_db_sql(database)};" +
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
            logger.debug(
                f"[{self.catalog.session_id}] DuckDB environment is setting up:\n{prepend_to_lines(views_sql, max=1000)}")
            try:
                self.catalog.duckdb.execute(views_sql)
            except duckdb.Error as e:
                raise QueryError(f"Unable to sync DuckDB catalog. {e.args}")

    def _get_property(self, ast: sqlglot.exp.Create, name: str):
        if "properties" not in ast.args:
            return None
        return next(
            (expression.args.get('value').this for expression in ast.args['properties'].expressions
             if isinstance(expression, Property) and isinstance(expression.this,
                                                                Var) and expression.this.this.casefold() == name.casefold()),
            None)

    def execute(self, ast: sqlglot.exp.Expression, catalog_executor: Executor, locations: Tables) -> typing.Optional[
        Locations]:
        if not catalog_executor.is_warm():
            # since duckdb doesn't implement auth layer,
            # force the catalog to perform auth before executing any query
            catalog_executor.test()

        if isinstance(ast, Create) or isinstance(ast, Insert):
            if isinstance(ast.this, Schema):
                destination_table = ast.this.this
                # filter columns
            elif not isinstance(ast.this, Table):
                raise QueryError("Query type is not supported in DuckDB")
            else:
                destination_table = ast.this

            destination_table = full_qualifier(destination_table, self.catalog.credentials)
            full_table = destination_table.sql()

            if isinstance(ast, Create):
                properties = ast.args.get('properties') or Properties()
                is_temp = TemporaryProperty() in properties.expressions
                is_iceberg = IcebergProperty() in properties.expressions

                if ast.kind == 'TABLE':
                    is_replace = ast.args.get('replace')
                    if_exists = ast.args.get('exists')
                    final_query = self._sync_and_transform_query(ast, locations | {destination_table: None})

                    if is_iceberg:
                        iceberg_catalog = self.catalog.iceberg_catalog
                        namespace = iceberg_catalog.properties.get('namespace')
                        database_location = iceberg_catalog.properties.get(LOCATION)
                        if final_query.expression is not None:
                            self.execute_raw(final_query.expression.sql(dialect="duckdb"), catalog_executor)
                            arrow_table = self.get_as_table()
                        else:
                            arrow_table = None
                        base_location = self._get_property(ast, 'base_location')

                        if database_location is not None:
                            database_location = database_location.rstrip("/")
                            table_location = str(os.path.join(database_location, base_location))
                        elif base_location is not None:
                            external_volume = self._get_property(ast, 'external_volume')
                            lake_path = catalog_executor.catalog.get_volume_lake_path(external_volume)
                            # resolve external volume location
                            table_location = str(os.path.join(lake_path, base_location))
                        else:
                            table_location = None
                        if if_exists:
                            create_iceberg_table = iceberg_catalog.create_table_if_not_exists(
                                (namespace, full_table),
                                arrow_table.schema,
                                location=table_location)
                        else:
                            if is_replace:
                                try:
                                    iceberg_catalog.drop_table((namespace, full_table))
                                except NoSuchTableError:
                                    pass
                            create_iceberg_table = iceberg_catalog.create_table(
                                (namespace, full_table),
                                arrow_table.schema,
                                location=table_location)
                        create_iceberg_table.overwrite(arrow_table)
                        if not create_iceberg_table.metadata_location.startswith(table_location):
                            raise QueryError("Unable to determine location")
                        metadata_file_path = create_iceberg_table.metadata_location[len(table_location):].strip('/')
                        catalog = self._get_property(ast, 'catalog')

                        if catalog is not None:
                            ast.set('expression', None)
                            properties = ast.args.get('properties') or Properties()

                            if catalog.lower() == 'snowflake':
                                # adds column definitions to DDL
                                column_definitions = [ColumnDef(
                                    this=sqlglot.exp.parse_identifier(column.name),
                                    kind=DataType.build(str(column.field_type)))
                                    for column in create_iceberg_table.metadata.schema().columns]
                                if not isinstance(ast.this, Schema):
                                    # c
                                    schema = Schema()
                                    schema.set('this', ast.this)
                                    schema.set('expressions', column_definitions)
                                    ast.set('this', schema)
                            properties.expressions.append(
                                Property(this=Var(this='METADATA_FILE_PATH'), value=Literal.string(metadata_file_path)))
                            return {destination_table: ast}
                    else:
                        if is_temp:
                            ast.args['properties'] = Properties(
                                expressions=[expression for expression in properties.expressions if
                                             not isinstance(expression, TemporaryProperty)])

                            self.execute_raw(ast.sql(dialect="duckdb"), catalog_executor)
                            return None
                        else:
                            raise QueryError(
                                "DuckDB can't create native Snowflake tables, please use Iceberg or External tables")
                elif ast.kind == 'VIEW':
                    duckdb_query = self._sync_and_transform_query(ast, locations).sql(dialect="duckdb")
                    self.execute_raw(duckdb_query, catalog_executor)

                    if not is_temp:
                        return {destination_table: ast.expression}
            elif isinstance(ast, Insert):
                query = self._sync_and_transform_query(ast.expression, locations)
                table_type = self.catalog._get_table_location(destination_table)
                if table_type == TableType.ICEBERG:
                    namespace = self.catalog.iceberg_catalog.properties.get('namespace')
                    try:
                        iceberg_table = self.catalog.iceberg_catalog.load_table((namespace, full_table))
                    except NoSuchTableError as e:
                        raise QueryError(f"Error accessing catalog {e.args}")
                    self.execute_raw(query.sql(dialect="duckdb"), catalog_executor)
                    table = self.get_as_table()
                    iceberg_table.append(table)
                elif table_type == TableType.LOCAL:
                    self.execute_raw(query.sql(dialect="duckdb"),
                                     catalog_executor)
                else:
                    raise QueryError("Unable to determine table type")
        elif isinstance(ast, Drop):
            delete_table = ast.this
            self.catalog.iceberg_catalog.drop_table(delete_table.sql())
        elif isinstance(ast, Use):
            kind = ast.args['kind']
            if kind is None or kind.this == 'SCHEMA':
                if len(ast.this.parts) == 2:
                    self.catalog.credentials['schema'] = ast.this.parts[-1].sql(dialect="snowflake")
                    self.catalog.credentials['database'] = ast.this.parts[-2].sql(dialect="snowflake")
                elif len(ast.this.parts) == 1:
                    self.catalog.credentials['database'] = ast.this.parts[-1].sql(dialect="snowflake")
                else:
                    raise QueryError("Unable to parse USE statement: Unknown kind")
            elif kind.this == 'DATABASE':
                self.catalog.credentials['database'] = ast.this.sql(dialect="snowflake")
            else:
                raise QueryError("Unable to parse USE statement: Unknown kind")
            if self.catalog.credentials['database'] is not None:
                self._sync_catalog({})
            self.catalog.emulator.execute(ast.sql(dialect="snowflake"))
            catalog_executor.catalog.clear_cache()
        elif isinstance(ast, Copy):
            sql = self._sync_and_transform_query(ast, locations).sql(dialect="duckdb", pretty=True)

            insert_into_select_ast = sqlglot.parse_one(sql, dialect='duckdb')
            # refactor soon
            cache_directory = self.catalog.context.get('cache_directory')

            self.execute_raw(sql, catalog_executor, is_raw=isinstance(ast, Copy))
        else:
            sql = self._sync_and_transform_query(ast, locations).sql(dialect="duckdb", pretty=True)
            self.execute_raw(sql, catalog_executor, is_raw=isinstance(ast, Copy))
        return None

    def get_raw_table(self):
        if self.is_last_raw:
            return self.catalog.duckdb.fetch_arrow_table()
        else:
            return self.catalog.emulator._arrow_table

    def get_as_table(self) -> pyarrow.Table:
        arrow_table = self.get_raw_table()
        if arrow_table is None:
            arrow_table = self.catalog.duckdb.sql("select 'no response returned' as message").fetch_arrow_table()

        for idx, column in enumerate(self.catalog.duckdb.description):
            array, schema = get_field_from_duckdb(column, arrow_table, idx)
            arrow_table = arrow_table.set_column(idx, schema, array)
        return arrow_table

    def close(self):
        self.catalog.emulator.close()

    @staticmethod
    def remove_snowflake_table_references(
            expression: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
        # replace table(func()) with func() as it's not required in duckdb
        if isinstance(expression, sqlglot.exp.Anonymous) and expression.this == 'table':
            return expression.expressions[0]
        return expression

    @staticmethod
    def fix_snowflake_to_duckdb_types(
            expression: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
        # select cast(null as timestampltz)
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