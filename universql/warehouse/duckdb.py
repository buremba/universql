import logging
import os
import typing
from typing import List, Union

import duckdb
import pyiceberg.table
import snowflake
import sqlglot
from fakesnow.conn import FakeSnowflakeConnection
from fakesnow.cursor import FakeSnowflakeCursor
from pyiceberg.catalog import Catalog, PropertiesUpdateSummary
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError, CommitFailedException
from pyiceberg.io import load_file_io, LOCATION
from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table import UNSORTED_SORT_ORDER, SortOrder, CommitTableRequest, CommitTableResponse
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.typedef import Identifier, EMPTY_DICT
from snowflake.connector.options import pyarrow
from sqlglot.expressions import Select, Insert, Create, Drop, Properties, TemporaryProperty, Schema, Table, Property, \
    Var, Literal

from universql.warehouse import ICatalog, Executor, Locations, Tables
from universql.lake.cloud import s3, gcs, in_lambda
from universql.util import prepend_to_lines, QueryError, calculate_script_cost, parse_snowflake_account
from universql.protocol.utils import DuckDBFunctions, get_field_from_duckdb
from sqlglot.optimizer.simplify import simplify

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("🐥")


class DuckDBIcebergCatalog(SqlCatalog):
    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)
        self.duckdb = properties.get('duckdb')

    def _ensure_tables_exist(self) -> None:
        pass

    def create_table(
            self,
            identifier: typing.Union[str, Identifier],
            schema: typing.Union[Schema, "pa.Schema"],
            location: typing.Optional[str] = None,
            partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
            sort_order: SortOrder = UNSORTED_SORT_ORDER,
            properties: Properties = EMPTY_DICT,
    ) -> Table:
        schema: Schema = self._convert_schema_if_needed(schema)  # type: ignore

        identifier_nocatalog = self.identifier_to_tuple_without_catalog(identifier)
        namespace_identifier = Catalog.namespace_from(identifier_nocatalog)
        table_name = Catalog.table_name_from(identifier_nocatalog)
        if not self._namespace_exists(namespace_identifier):
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace_identifier}")

        namespace = Catalog.namespace_to_string(namespace_identifier)
        location = self._resolve_table_location(location, namespace, table_name)
        metadata_location = self._get_metadata_location(location=location)
        metadata = new_table_metadata(
            location=location, schema=schema, partition_spec=partition_spec, sort_order=sort_order,
            properties=properties
        )
        io = load_file_io(properties=self.properties, location=metadata_location)
        self._write_metadata(metadata, io, metadata_location)

        session.add(
            IcebergTables(
                catalog_name=self.name,
                table_namespace=namespace,
                table_name=table_name,
                metadata_location=metadata_location,
                previous_metadata_location=None,
            )
        )
        return self.load_table(identifier=identifier)

    def load_table(self, identifier: typing.Union[str, Identifier]) -> Table:
        identifier_tuple = self.identifier_to_tuple_without_catalog(identifier)
        namespace_tuple = Catalog.namespace_from(identifier_tuple)
        namespace = Catalog.namespace_to_string(namespace_tuple)
        table_name = Catalog.table_name_from(identifier_tuple)
        # io = load_file_io(properties=self.properties, location=metadata_location)
        # file = io.new_input(metadata_location)
        # metadata = FromInputFile.table_metadata(file)
        # return Table(
        #     identifier=(self.name,) + Catalog.identifier_to_tuple(table_namespace) + (table_name,),
        #     metadata=metadata,
        #     metadata_location=metadata_location,
        #     io=self._load_file_io(metadata.properties, metadata_location),
        #     catalog=self,
        # )

        raise NoSuchTableError(f"Table does not exist: {namespace}.{table_name}")

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier.
            properties (Properties): A string dictionary of properties for the given namespace.

        Raises:
            NamespaceAlreadyExistsError: If a namespace with the given name already exists.
        """
        if self._namespace_exists(namespace):
            raise NamespaceAlreadyExistsError(f"Namespace {namespace} already exists")

        if not properties:
            properties = IcebergNamespaceProperties.NAMESPACE_MINIMAL_PROPERTIES
        create_properties = properties if properties else IcebergNamespaceProperties.NAMESPACE_MINIMAL_PROPERTIES
        with Session(self.engine) as session:
            for key, value in create_properties.items():
                session.add(
                    IcebergNamespaceProperties(
                        catalog_name=self.name,
                        namespace=Catalog.namespace_to_string(namespace, NoSuchNamespaceError),
                        property_key=key,
                        property_value=value,
                    )
                )
            session.commit()

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        pass

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        pass

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        pass

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        pass

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        pass

    def register_table(self, identifier: Union[str, Identifier], metadata_location: str) -> Table:
        """Register a new table using existing metadata.

        Args:
            identifier Union[str, Identifier]: Table identifier for the table
            metadata_location str: The location to the metadata

        Returns:
            Table: The newly registered table

        Raises:
            TableAlreadyExistsError: If the table already exists
            NoSuchNamespaceError: If namespace does not exist
        """
        identifier_tuple = self.identifier_to_tuple_without_catalog(identifier)
        namespace_tuple = Catalog.namespace_from(identifier_tuple)
        namespace = Catalog.namespace_to_string(namespace_tuple)
        table_name = Catalog.table_name_from(identifier_tuple)
        if not self._namespace_exists(namespace):
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")

        with Session(self.engine) as session:
            try:
                session.add(
                    IcebergTables(
                        catalog_name=self.name,
                        table_namespace=namespace,
                        table_name=table_name,
                        metadata_location=metadata_location,
                        previous_metadata_location=None,
                    )
                )
                session.commit()
            except IntegrityError as e:
                raise TableAlreadyExistsError(f"Table {namespace}.{table_name} already exists") from e

        return self.load_table(identifier=identifier)

    def update_namespace_properties(
            self, namespace: typing.Union[str, Identifier], removals: typing.Optional[typing.Set[str]] = None,
            updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        pass

    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        """Update one or more tables.

        Args:
            table_request (CommitTableRequest): The table requests to be carried out.

        Returns:
            CommitTableResponse: The updated metadata.

        Raises:
            NoSuchTableError: If a table with the given identifier does not exist.
            CommitFailedException: Requirement not met, or a conflict with a concurrent commit.
        """
        identifier_tuple = self.identifier_to_tuple_without_catalog(
            tuple(table_request.identifier.namespace.root + [table_request.identifier.name])
        )
        namespace_tuple = Catalog.namespace_from(identifier_tuple)
        namespace = Catalog.namespace_to_string(namespace_tuple)
        table_name = Catalog.table_name_from(identifier_tuple)

        current_table: typing.Optional[Table]
        try:
            current_table = self.load_table(identifier_tuple)
        except NoSuchTableError:
            current_table = None

        updated_staged_table = self._update_and_stage_table(current_table, table_request)
        if current_table and updated_staged_table.metadata == current_table.metadata:
            # no changes, do nothing
            return CommitTableResponse(metadata=current_table.metadata,
                                       metadata_location=current_table.metadata_location)
        self._write_metadata(
            metadata=updated_staged_table.metadata,
            io=updated_staged_table.io,
            metadata_path=updated_staged_table.metadata_location,
        )

        with Session(self.engine) as session:
            if current_table:
                # table exists, update it
                if self.engine.dialect.supports_sane_rowcount:
                    stmt = (
                        update(IcebergTables)
                        .where(
                            IcebergTables.catalog_name == self.name,
                            IcebergTables.table_namespace == namespace,
                            IcebergTables.table_name == table_name,
                            IcebergTables.metadata_location == current_table.metadata_location,
                        )
                        .values(
                            metadata_location=updated_staged_table.metadata_location,
                            previous_metadata_location=current_table.metadata_location,
                        )
                    )
                    result = session.execute(stmt)
                    if result.rowcount < 1:
                        raise CommitFailedException(
                            f"Table has been updated by another process: {namespace}.{table_name}")
                else:
                    try:
                        tbl = (
                            session.query(IcebergTables)
                            .with_for_update(of=IcebergTables)
                            .filter(
                                IcebergTables.catalog_name == self.name,
                                IcebergTables.table_namespace == namespace,
                                IcebergTables.table_name == table_name,
                                IcebergTables.metadata_location == current_table.metadata_location,
                            )
                            .one()
                        )
                        tbl.metadata_location = updated_staged_table.metadata_location
                        tbl.previous_metadata_location = current_table.metadata_location
                    except NoResultFound as e:
                        raise CommitFailedException(
                            f"Table has been updated by another process: {namespace}.{table_name}") from e
                session.commit()
            else:
                # table does not exist, create it
                try:
                    session.add(
                        IcebergTables(
                            catalog_name=self.name,
                            table_namespace=namespace,
                            table_name=table_name,
                            metadata_location=updated_staged_table.metadata_location,
                            previous_metadata_location=None,
                        )
                    )
                    session.commit()
                except IntegrityError as e:
                    raise TableAlreadyExistsError(f"Table {namespace}.{table_name} already exists") from e

        return CommitTableResponse(
            metadata=updated_staged_table.metadata, metadata_location=updated_staged_table.metadata_location
        )


class DuckDBCatalog(ICatalog):

    def register_locations(self, tables: Locations):
        raise Exception("Unsupported operation")

    def __init__(self, context: dict, query_id: str, credentials: dict, compute: dict, iceberg_catalog: Catalog):
        super().__init__(context, query_id, credentials, compute, iceberg_catalog)
        duck_config = {
            'max_memory': context.get('max_memory'),
            'temp_directory': os.path.join(context.get('cache_directory'), "duckdb-staging"),
        }
        if context.get('max_cache_size') != "0":
            duck_config['max_temp_directory_size'] = context.get('max_cache_size')
        self.account = parse_snowflake_account(context.get('account'))
        database_path = self.context.get('database_path')
        if database_path is not None:
            database = os.path.join(database_path, f"{query_id}.duckdb")
        else:
            database = ':memory:'

        try:
            self.duckdb = duckdb.connect(database, config=duck_config)
        except duckdb.InvalidInputException as e:
            raise QueryError(f"Unable to spin up DuckDB with config {duck_config}: {e}")
        DuckDBFunctions.register(self.duckdb)
        self.duckdb.install_extension("iceberg")
        self.duckdb.load_extension("iceberg")

        fake_snowflake_conn = FakeSnowflakeConnection(self.duckdb, "main", "public", False, False)
        fake_snowflake_conn.database_set = True
        fake_snowflake_conn.schema_set = True
        self.emulator = FakeSnowflakeCursor(fake_snowflake_conn, self.duckdb)
        self._register_data_lake(context)

    def _register_data_lake(self, args: dict):
        if args.get('aws_profile') is not None or self.account.cloud == 'aws':
            self.duckdb.register_filesystem(s3(args))
        if args.get('gcp_project') is not None or self.account.cloud == 'gcp':
            self.duckdb.register_filesystem(gcs(args))

    def get_table_paths(self, tables: List[sqlglot.exp.Table]) -> Tables:
        pass

    def executor(self) -> Executor:
        return DuckDBExecutor(self)


class DuckDBExecutor(Executor):

    def __init__(self, duckdb: DuckDBCatalog):
        self.catalog = duckdb

    def get_query_log(self, total_duration) -> str:
        if in_lambda:
            memory_size = os.environ['AWS_LAMBDA_FUNCTION_MEMORY_SIZE']
            execution_env = os.environ['AWS_EXECUTION_ENV']
            return f"Run DuckDB in AWS Lambda: memory:{memory_size} platform:{execution_env}"
        else:
            cost = calculate_script_cost(total_duration)
            return f"Run locally on DuckDB: {cost}"

    def supports(self, ast: sqlglot.exp.Expression) -> bool:
        return isinstance(ast, Select) or isinstance(ast, Insert) or isinstance(ast, Create)

    def execute_raw(self, raw_query: str) -> None:
        try:
            logger.info(
                f"[{self.catalog.query_id}] Executing query on DuckDB:\n{prepend_to_lines(raw_query)}")
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

    def _get_db_path(self, db_name):
        database_path = self.catalog.context.get('database_path')
        if database_path is not None:
            duckdb_path = os.path.join(database_path, f'{db_name}.duckdb')
        else:
            duckdb_path = ':memory:'
        return duckdb_path

    def _sync_catalog(self, ast: sqlglot.exp.Expression, locations: Tables) -> sqlglot.exp.Expression:
        schemas = set((table.catalog, table.db) for table in locations.keys() if table.db != '')
        databases = set(table[0] for table in schemas if table[0] != '')

        databases_sql = [
            f"ATTACH IF NOT EXISTS '{self._get_db_path(database)}' AS {sqlglot.exp.parse_identifier(database).sql()}"
            for
            database in
            databases]
        schemas_sql = [
            f"CREATE SCHEMA IF NOT EXISTS {sqlglot.exp.parse_identifier(db).sql()}.{sqlglot.exp.parse_identifier(schema).sql()}"
            for
            (db, schema) in schemas]
        views_sql = [f"CREATE OR REPLACE VIEW {table.sql()} AS SELECT * FROM {self.get_iceberg_read(location)}" for
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
        #     return expression          pass

        final_ast = (simplify(ast)
                     .transform(self.fix_snowflake_to_duckdb_types))

        return final_ast

    def _get_property(self, ast: sqlglot.exp.Create, name: str):
        return next(
            (expression.args.get('value').this for expression in ast.args['properties'].expressions
             if isinstance(expression, Property) and isinstance(expression.this,
                                                                Var) and expression.this.this.casefold() == name.casefold()),
            None)

    def execute(self, ast: sqlglot.exp.Expression, tables: Tables) -> typing.Optional[Locations]:
        if isinstance(ast, Create) or isinstance(ast, Insert):
            destination_table = ast.this
            if not isinstance(destination_table, Table):
                if isinstance(destination_table, Schema):
                    destination_table = destination_table.this
                    # filter columns
                else:
                    raise QueryError("Query type is not supported in DuckDB")

            raw_table = destination_table.parts[-1].sql()
            raw_schema = destination_table.parts[1].sql() if len(
                destination_table.parts) > 1 else self.catalog.credentials.get('schema')
            raw_catalog = destination_table.parts[0].sql() if len(
                destination_table.parts) > 2 else self.catalog.credentials.get('database')
            full_table = f"{raw_catalog}.{raw_schema}.{raw_table}"
            namespace = self.catalog.iceberg_catalog.properties.get('namespace')

            if isinstance(ast, Create):
                if ast.kind == 'TABLE':
                    properties = destination_table.args.get('properties') or Properties()
                    is_temp = TemporaryProperty() in properties.expressions
                    is_replace = ast.args.get('replace')
                    if_exists = ast.args.get('exists')
                    final_query = self._sync_catalog(ast, tables)
                    self.execute_raw(final_query.expression.sql(dialect="duckdb"))
                    arrow_table = self.get_as_table()
                    database_location = self.catalog.iceberg_catalog.properties.get(LOCATION)
                    database_location = database_location.rstrip("/")
                    base_location = self._get_property(ast, 'base_location')
                    catalog = self._get_property(ast, 'catalog')
                    if base_location is None:
                        base_location = f"_universql/{raw_catalog}/{raw_schema}/{raw_table}"

                    table_location = str(os.path.join(database_location, base_location))

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

                    if is_temp:
                        self.catalog.duckdb.register(destination_table.sql(), arrow_table)
                    elif catalog is not None and catalog.lower() != 'snowflake':
                        ast.set('expression', None)
                        properties = ast.args.get('properties') or Properties()

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
                elif ast.kind == 'VIEW':
                    properties = destination_table.args.get('properties') or Properties()
                    is_temp = TemporaryProperty() in properties.expressions
                    duckdb_query = self._sync_catalog(ast, tables).sql(dialect="duckdb")
                    self.execute_raw(duckdb_query)

                    if not is_temp:
                        return {destination_table: ast.expression}
            elif isinstance(ast, Insert):
                try:
                    iceberg_table = self.catalog.iceberg_catalog.load_table((namespace, raw_table))
                except NoSuchTableError as e:
                    raise QueryError(f"Error accessing catalog {e.args}")
                self.execute_raw(self._sync_catalog(ast.expression, tables).sql(dialect="duckdb"))
                table = self.get_as_table()
                iceberg_table.append(table)

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
