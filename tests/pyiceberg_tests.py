from typing import Union, Optional, Set, List

import duckdb
import pyarrow
from pyiceberg.catalog import WAREHOUSE_LOCATION, Catalog, PropertiesUpdateSummary
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchNamespaceError
from pyiceberg.io import PY_IO_IMPL, load_file_io
from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import SortOrder, UNSORTED_SORT_ORDER, CommitTableRequest, CommitTableResponse, Table, CreateTableTransaction
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.typedef import Identifier, Properties, EMPTY_DICT

from universql.lake.cloud import CACHE_DIRECTORY_KEY


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

class DuckDBCatalog(Catalog):

    def __init__(self, conn : duckdb.DuckDBPyConnection, name: str, **properties: str):
        super().__init__(name, **properties)
        self.conn = conn

    def create_table(self, identifier: Union[str, Identifier], schema: Union[Schema, "pa.Schema"],
                     location: Optional[str] = None, partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
                     sort_order: SortOrder = UNSORTED_SORT_ORDER, properties: Properties = EMPTY_DICT) -> Table:
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

    def create_table_transaction(self, identifier: Union[str, Identifier], schema: Union[Schema, "pa.Schema"],
                                 location: Optional[str] = None,
                                 partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
                                 sort_order: SortOrder = UNSORTED_SORT_ORDER,
                                 properties: Properties = EMPTY_DICT) -> CreateTableTransaction:
        pass

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        pass

    def table_exists(self, identifier: Union[str, Identifier]) -> bool:
        pass

    def register_table(self, identifier: Union[str, Identifier], metadata_location: str) -> Table:
        pass

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        pass

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        pass

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        pass

    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        pass

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        self.conn.table_function('duckdb_databases', lambda: None)

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        pass

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        pass

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        pass

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        pass

    def update_namespace_properties(self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None,
                                    updates: Properties = EMPTY_DICT) -> PropertiesUpdateSummary:
        pass



connect = duckdb.connect(":memory:")
db_catalog = DuckDBCatalog(connect, "duckdb", **{})
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
