import logging
import os
import re
import typing
from enum import Enum
from string import Template
from typing import List
import boto3

import duckdb
import pyiceberg.table
import snowflake
import sqlglot
from duckdb.duckdb import NotSupportedError
from fakesnow import macros, info_schema
from fakesnow.conn import FakeSnowflakeConnection
from fakesnow.cursor import FakeSnowflakeCursor
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import LOCATION
from snowflake.connector.options import pyarrow
from sqlglot.expressions import Select, Insert, Create, Drop, Properties, TemporaryProperty, Schema, Table, Property, \
    Var, Literal, IcebergProperty, Copy, Delete, Merge, Use, DataType, ColumnDef

from universql.warehouse import ICatalog, Executor, Locations, Tables
from universql.warehouse.utils import transform_copy, get_file_format, get_load_file_format_queries
from universql.lake.cloud import s3, gcs, in_lambda
from universql.util import prepend_to_lines, QueryError, calculate_script_cost, parse_snowflake_account, full_qualifier, get_role_credentials
from universql.protocol.utils import DuckDBFunctions, get_field_from_duckdb
from sqlglot.optimizer.simplify import simplify
from pprint import pp

from functools import partial

logger = logging.getLogger("🐥")


class TableType(Enum):
    ICEBERG = "iceberg"
    LOCAL = "local"


class DuckDBCatalog(ICatalog):

    def __init__(self, context: dict, session_id: str, credentials: dict, compute: dict, iceberg_catalog: Catalog,
                 base_catalog: ICatalog):
        super().__init__(context, session_id, credentials, compute, iceberg_catalog, base_catalog)
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
        self.current_connection = self.emulator
        self._register_data_lake(context)

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

        match = re.search(r'CREATE VIEW (?:["\w]+\.)?[\w.]+ AS SELECT \* FROM iceberg_scan\(\'(s3://[^\']+)\'\);',
                          table_exists[0])
        if match is not None:
            return TableType.ICEBERG

        raise NotSupportedError
    
    def _get_file_location(self, file):
        database = file.parts[0]
        schema = file.parts[1]
        table_name = file.parts[2].quoted   

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

    def arrow_table(self):
        if self.current_connection == self.emulator:
            return self.emulator._arrow_table
        elif self.current_connection == self.duckdb:
            return self.duckdb.arrow()

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

    def execute_raw(self, raw_query: str, no_emulator = False) -> None:
        """
        Executes a raw SQL query with optional Snowflake emulation.
        
        Provides direct query execution capabilities with two modes:
        1. Snowflake emulation mode (default) - translates Snowflake syntax
        2. Direct DuckDB mode - bypasses emulation for native DuckDB operations
        
        Args:
            raw_query: SQL query to execute
            no_emulator: If True, executes directly on DuckDB without Snowflake emulation
        
        Raises:
            QueryError: On any execution failure, wrapping the underlying database error
        """

        try:
            logger.info(
                f"[{self.catalog.session_id}] Executing query on DuckDB:\n{prepend_to_lines(raw_query)}")
            if no_emulator:
                self.catalog.current_connection = self.catalog.duckdb
            else:
                self.catalog.current_connection = self.catalog.emulator
            logger.info(f"Debug - Catalog connection type: {type(self.catalog.current_connection)}")
            self.catalog.current_connection.execute(raw_query)
        except duckdb.Error as e:
            raise QueryError(f"Unable to run the query locally on DuckDB. {e.args}")
        except duckdb.duckdb.DatabaseError as e:
            raise QueryError(f"Unable to run the query locally on DuckDB. {e.msg}")
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

    def _sync_and_transform_query(self, ast: sqlglot.exp.Expression, locations: Tables, file_data = None) -> sqlglot.exp.Expression:
        """
        Prepares a query for DuckDB execution by syncing catalogs and transforming syntax.
        
        Performs three key operations:
        1. Syncs catalog state to ensure required schemas/tables exist
        2. Transforms Snowflake SQL syntax to DuckDB-compatible syntax
        3. For COPY commands, transforms stage references to direct file paths
        
        Args:
            ast: SQL Abstract Syntax Tree
            locations: Table location mappings
            file_data: Optional stage/file metadata for COPY commands
        
        Returns:
            Transformed AST ready for DuckDB execution
        """
        self._sync_catalog(locations)
        transformed_ast = (simplify(ast)
                     .transform(self.fix_snowflake_to_duckdb_types))

        if isinstance(ast, Copy):
            transformed_ast = transformed_ast.transform(partial(transform_copy, file_data=file_data))
        return transformed_ast
    

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

    def prep_duckdb_for_files(self, file_data):
        file_format = get_file_format(file_data[next(iter(file_data.keys()))])
        load_file_format_queries = get_load_file_format_queries(file_format)
        print(f"Found {file_format} files to read")
        for query in load_file_format_queries:
            print(f"Executing {query}")
            self.execute_raw(query)
                
    def execute(self, ast: sqlglot.exp.Expression, tables: Tables, file_data = None) -> typing.Optional[Locations]:
        """
        Main query execution handler with specialized processing for different SQL commands.
        
        Provides specific handling for:
        - CREATE: Tables (including Iceberg) and views
        - INSERT: Data insertion for both Iceberg and local tables
        - DROP: Table removal
        - USE: Database/schema context switching
        - COPY: Stage-based data loading with file format handling
        
        For COPY commands, processes stage metadata and converts to direct file access.
        
        Args:
            ast: SQL Abstract Syntax Tree
            tables: Table location mappings
            file_data: Stage and file metadata for COPY commands
        
        Returns:
            Optional[Locations]: Updated location mappings for new tables/views
        """
        # if file_data is not None:
        #     self.prep_duckdb_for_files(file_data)

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
                    final_query = self._sync_and_transform_query(ast, tables | {destination_table: None})

                    if is_iceberg:
                        iceberg_catalog = self.catalog.iceberg_catalog
                        namespace = iceberg_catalog.properties.get('namespace')
                        database_location = iceberg_catalog.properties.get(LOCATION)
                        if final_query.expression is not None:
                            self.execute_raw(final_query.expression.sql(dialect="duckdb"))
                            arrow_table = self.get_as_table()
                        else:
                            arrow_table = None
                        base_location = self._get_property(ast, 'base_location')

                        if database_location is not None:
                            database_location = database_location.rstrip("/")
                            table_location = str(os.path.join(database_location, base_location))
                        elif base_location is not None:
                            external_volume = self._get_property(ast, 'external_volume')
                            lake_path = self.catalog.base_catalog.get_volume_lake_path(external_volume)
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

                            self.execute_raw(ast.sql(dialect="duckdb"))
                            return None
                        else:
                            raise QueryError(
                                "DuckDB can't create native Snowflake tables, please use Iceberg or External tables")
                elif ast.kind == 'VIEW':
                    duckdb_query = self._sync_and_transform_query(ast, tables).sql(dialect="duckdb")
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
                    self.execute_raw(self._sync_and_transform_query(ast.expression, tables).sql(dialect="duckdb"))
                    table = self.get_as_table()
                    iceberg_table.append(table)
                elif table_type.LOCAL:
                    self.execute_raw(self._sync_and_transform_query(ast, tables).sql(dialect="duckdb"))
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
            self.catalog.base_catalog.clear_cache()
        elif isinstance(ast, Copy):
            for file_name, file_config in file_data.items():
                urls = file_config["METADATA"]["URL"]
                # profile = file_config["METADATA"]["URL"]
                try:
                    region = get_region(urls[0], file_config["METADATA"]["storage_provider"])
                except Exception as e:
                    print(f"There was a problem accessing data for {file_name}:\n{e}")
            
            sql = self._sync_and_transform_query(ast, tables, file_data).sql(dialect="duckdb", pretty=True)
            logger.info(f"Debug - Final SQL: {sql}")
            logger.info(f"Debug - File data: {file_data}")
            self.execute_raw(sql, True)
        else:
            sql = self._sync_and_transform_query(ast, tables).sql(dialect="duckdb", pretty=True)
            self.execute_raw(sql)

        return None

    def _load_file_format(file_format):
        file_format_queries = {
            "JSON": ["INSTALL json;", "LOAD json;"],
            "AVRO": ["INSTALL avro FROM community;", "LOAD avro;"]
        }

    def get_as_table(self) -> pyarrow.Table:
        arrow_table = self.catalog.arrow_table()
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
    def convert_stage_to_s3_paths(expression: sqlglot.exp.Expression, file_data) -> sqlglot.exp.Expression:
        if file_data is None:
            return
        

    @staticmethod
    def get_iceberg_read(location: pyiceberg.table.Table) -> str:
        return sqlglot.exp.func("iceberg_scan",
                                sqlglot.exp.Literal.string(location.metadata_location)).sql()

def get_region(profile, url, storage_provider):
    """
    Resolves AWS region for an S3 bucket.
    
    Takes a bucket URL and retrieves its region using AWS SDK.
    Handles the special case where a null LocationConstraint indicates us-east-1.
    Additionally, this sets the profile to use for the copy operation.
    
    Args:
        profile: AWS credentials profile
        url: S3 URL containing bucket name
        storage_provider: Must be 'Amazon S3'
    
    Returns:
        str: AWS region identifier (e.g., 'us-east-1')
    """
    if storage_provider == 'Amazon S3':
        bucket_name = url[5:].split("/")[0]
        session = boto3.Session(profile_name=profile)
        s3 = session.client('s3')
        region_dict = s3.get_bucket_location(Bucket=bucket_name)
        return region_dict.get('LocationConstraint') or 'us-east-1'    
