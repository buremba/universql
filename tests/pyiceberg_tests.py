import re
from typing import Union, Optional, Set, List

import duckdb
import pyarrow
import sqlglot
from pyiceberg.catalog import WAREHOUSE_LOCATION, Catalog, PropertiesUpdateSummary, MetastoreCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchNamespaceError, TableAlreadyExistsError, NoSuchIcebergTableError
from pyiceberg.io import PY_IO_IMPL, load_file_io
from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import SortOrder, UNSORTED_SORT_ORDER, CommitTableRequest, CommitTableResponse, Table, \
    CreateTableTransaction, StaticTable
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.typedef import Identifier, Properties, EMPTY_DICT
from sqlglot.optimizer import build_scope

from universql.lake.cloud import CACHE_DIRECTORY_KEY, s3


# def get_iceberg_table_from_data_lake(metadata_file_path: str, cache_directory):
#     from_metadata = StaticTable.from_metadata(metadata_file_path, {
#         PY_IO_IMPL: "universql.lake.cloud.iceberg",
#         CACHE_DIRECTORY_KEY: cache_directory,
#     })
#     return from_metadata
# test = get_iceberg_table_from_data_lake(
#     "gs://my-iceberg-data/custom-events/customer_iceberg/metadata/v1719882827064000000.metadata.json", '')
# to_arrow = test.scan().to_arrow()
# test = test.append(to_arrow)

# catalog = load_catalog(
#     name="polaris_aws"
# )
# catalog.properties[PY_IO_IMPL] = "universql.lake.cloud.iceberg"
#
# class HeadlessCatalog(NoopCatalog):
#     def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
#         return CommitTableResponse(
#             metadata=table_request, metadata_location=updated_staged_table.metadata_location
#         )

class DuckDBCatalog(MetastoreCatalog):

    def __init__(self, conn: duckdb.DuckDBPyConnection, name: str, **properties: str):
        super().__init__(name, **properties)
        self.conn = conn
        self.conn.install_extension("iceberg")
        self.conn.install_extension("substrait")
        self.conn.load_extension("iceberg")
        self.conn.load_extension("substrait")

    def create_table(self, identifier: Union[str, Identifier], schema: Union[Schema, "pa.Schema"],
                     location: Optional[str] = None, partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
                     sort_order: SortOrder = UNSORTED_SORT_ORDER, properties: Properties = EMPTY_DICT) -> Table:
        schema: Schema = self._convert_schema_if_needed(schema)  # type: ignore

        database, schema_name, table = self._get_table_address(identifier)
        if not self._namespace_exists(database):
            raise NoSuchNamespaceError(f"Namespace does not exist: {database}")

        location = self._resolve_table_location(location, schema_name, table)
        metadata_location = self._get_metadata_location(location=location)
        metadata = new_table_metadata(
            location=location, schema=schema, partition_spec=partition_spec, sort_order=sort_order,
            properties=properties
        )
        io = load_file_io(properties=self.properties, location=metadata_location)
        self._write_metadata(metadata, io, metadata_location)
        return self.register_table(identifier, metadata_location)

    def create_table_transaction(self, identifier: Union[str, Identifier], schema: Union[Schema, "pa.Schema"],
                                 location: Optional[str] = None,
                                 partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
                                 sort_order: SortOrder = UNSORTED_SORT_ORDER,
                                 properties: Properties = EMPTY_DICT) -> CreateTableTransaction:
        raise NotImplementedError

    def _get_table_address(self, table: Union[str, Identifier]) -> tuple[str, str, str] :
        identifier = Catalog.identifier_to_tuple(table)
        if len(identifier) < 3:
            current_db, current_schema = self.conn.sql("select current_catalog(), current_setting('schema')").fetchone()
            identifier = (current_db,
                          identifier[1] if len(identifier) == 2 else current_schema,
                          identifier[-1])

        return identifier[-3], identifier[-2], identifier[-1]


    def load_table(self, table: Union[str, Identifier]) -> Table:
        database, schema, table_name = self._get_table_address(table)

        def get_identifier(is_quoted):
            # return '?' if is_quoted else "upper(?)"
            return "upper(?)"

        table_relation = self.conn.sql(f"""
            with selected_table as (
              select * from {database}.information_schema.tables 
                where upper(table_catalog) = {get_identifier(database)} 
                and upper(table_schema) = {get_identifier(schema)} 
                and upper(table_name) = {get_identifier(table)}
              ) 
            select sql from selected_table 
            left join duckdb_views() on 
                view_name = selected_table.table_name and
                selected_table.table_type = 'VIEW' and internal = false
        """, params=(database, schema, table_name))
        create_view_sql = table_relation.fetchone()
        if create_view_sql is None:
            raise NoSuchIcebergTableError(f"Table does not exist: {table}")

        match = re.search(r'CREATE VIEW (?:"?\w+"?|"[^"]+") AS SELECT \* FROM iceberg_scan\(\'(s3://[^\']+)\'\);',
                          create_view_sql[0])
        if match:
            metadata_location = match.group(1)
        else:
            raise NoSuchIcebergTableError(f"The query doesn't match Iceberg table definition: {create_view_sql[0]}")

        return StaticTable.from_metadata(metadata_location, self.properties)

    def table_exists(self, identifier: Union[str, Identifier]) -> bool:
        pass

    def _namespace_exists(self, identifier: Union[str, Identifier]) -> bool:
        namespace_tuple = Catalog.identifier_to_tuple(identifier)
        namespace = Catalog.namespace_to_string(namespace_tuple, NoSuchNamespaceError)
        connect.execute("from duckdb_databases() where database_name = ? limit 1", (namespace,))
        return connect.fetchone() != None

    def register_table(self, identifier: Union[str, Identifier], metadata_location: str) -> Table:
        identifier_tuple = self.identifier_to_tuple_without_catalog(identifier)
        namespace_tuple = Catalog.namespace_from(identifier_tuple)
        namespace = Catalog.namespace_to_string(namespace_tuple)
        table_name = Catalog.table_name_from(identifier_tuple)

        try:
            connect.execute(f"CREATE VIEW {identifier} AS FROM iceberg_scan('{metadata_location}')")
        except duckdb.IntegrityError as e:
            if not self._namespace_exists(namespace):
                raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")

            raise TableAlreadyExistsError(f"Table {namespace}.{table_name} already exists") from e

        return StaticTable.from_metadata(metadata_location, self.properties)

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        raise NotImplementedError

    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        pass

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        namespace_tuple = Catalog.identifier_to_tuple(namespace)
        namespace_str = Catalog.namespace_to_string(namespace_tuple)
        try:
            self.conn.execute(f"ATTACH ':memory:' AS {namespace_str}")
        except duckdb.Error as e:
            if "already exists" in str(e):
                raise ValueError(f"Namespace already exists: {namespace_str}")
            raise e

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        namespace_tuple = Catalog.identifier_to_tuple(namespace)
        namespace_str = Catalog.namespace_to_string(namespace_tuple)
        if not self._namespace_exists(namespace):
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace_str}")
        self.conn.execute(f"DETACH {namespace_str}")

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        pass

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        result = self.conn.execute("SELECT database_name FROM duckdb_databases()").fetchall()
        return [(row[0],) for row in result if row[0] != 'system' and row[0] != 'temp']

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        if not self._namespace_exists(namespace):
            namespace_str = Catalog.namespace_to_string(Catalog.identifier_to_tuple(namespace))
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace_str}")
        return {}  # DuckDB doesn't support namespace properties

    def update_namespace_properties(self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None,
                                    updates: Properties = EMPTY_DICT) -> PropertiesUpdateSummary:
        raise NotImplementedError


connect = duckdb.connect(":memory:")
connect.register_filesystem(s3({"cache_directory": "~/.universql/cache"}))

db_catalog = DuckDBCatalog(connect, "duckdb", **{})
# db_catalog.create_namespace("MY_CUSTOM_APP.PUBLIC", {})
db_catalog.register_table("memory.main.table",
                          "s3://universql-us-east-1/glue_tables6/ICEBERG_TESTS/PUBLIC/ttt/metadata/00001-48648dfd-9355-4808-ab2f-c9065c6ef691.metadata.json")
catalog_load_table = db_catalog.load_table("memory.main.table")

db_catalog.create_table("memory.main.newtable", schema=pyarrow.schema([]))

db_catalog.create_namespace("MY_CUSTOM_APP.PUBLIC", {})

sql_catalog = SqlCatalog("ducky", **{
    PY_IO_IMPL: "universql.lake.cloud.iceberg",
    WAREHOUSE_LOCATION: "gs://my-iceberg-data/custom-events/customer_iceberg_pyiceberg",
    CACHE_DIRECTORY_KEY: './',
    "uri": "sqlite:////Users/bkabak/Code/universql/tests/test.db",
    "echo": "true"
})
# sql_catalog.create_namespace("MY_CUSTOM_APP.PUBLIC", {})
create_table = sql_catalog.create_table('MY_CUSTOM_APP.PUBLIC.testd', schema=pyarrow.schema([]))

load_table = sql_catalog.load_table("public.taxi_dataset")

arrow = table.scan().to_arrow()
table.current_snapshot()

# df = pq.read_table("/tmp/yellow_tripdata_2023-01.parquet")

# table = catalog.create_table(
#     "public.taxi_dataset",
#     schema=df.schema,
# )

# table.append(df)
# len(table.scan().to_arrow())

# df = df.append_column("tip_per_mile", pc.divide(df["tip_amount"], df["trip_distance"]))
# with table.update_schema() as update_schema:
#     update_schema.union_by_name(df.schema)

# table.overwrite(df)
# print(table.scan().to_arrow())

df = table.scan(row_filter="tip_per_mile > 0").to_arrow()
len(df)
