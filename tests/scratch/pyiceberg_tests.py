import re
from typing import Union, Optional, Set, List

import duckdb
import pyarrow
import sqlglot
from duckdb.duckdb import NotImplementedException
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
from sqlglot.expressions import Column
from sqlglot.optimizer import build_scope

from universql.lake.cloud import CACHE_DIRECTORY_KEY, s3
from universql.util import QueryError, load_computes

computes = load_computes()
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


connect = duckdb.connect(":memory:")
connect.register_filesystem(s3({"cache_directory": "~/.universql/cache"}))

# db_catalog = DuckDBIcebergCatalog(connect, **{"namespace": None})
# db_catalog.create_namespace("MY_CUSTOM_APP.PUBLIC", {})
# db_catalog.register_table("memory.main.table",
#                           "s3://universql-us-east-1/glue_tables6/ICEBERG_TESTS/PUBLIC/ttt/metadata/00001-48648dfd-9355-4808-ab2f-c9065c6ef691.metadata.json")
# catalog_load_table = db_catalog.load_table("memory.main.table")

# db_catalog.create_table((None, "memory.main.newtable"), schema=pyarrow.schema([]))
#
# db_catalog.create_namespace("MY_CUSTOM_APP", {})

sql_catalog = SqlCatalog("ducky", **{
    PY_IO_IMPL: "universql.lake.cloud.iceberg",
    CACHE_DIRECTORY_KEY: './',
    "uri": "duckdb:///:memory:",
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
