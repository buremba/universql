import re
import uuid
from typing import Union, Optional, Set, List

import duckdb
import sqlglot
from duckdb.duckdb import NotImplementedException
from google.cloud.bigquery.client import pyarrow
from pyiceberg.catalog import Catalog, PropertiesUpdateSummary, MetastoreCatalog
from pyiceberg.exceptions import NoSuchNamespaceError, TableAlreadyExistsError, NoSuchIcebergTableError
from pyiceberg.io import load_file_io
from pyiceberg.io.pyarrow import _dataframe_to_data_files
from pyiceberg.manifest import write_manifest_list, ManifestFile, write_manifest, ManifestEntryStatus, ManifestEntry
from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import SortOrder, UNSORTED_SORT_ORDER, CommitTableRequest, CommitTableResponse, Table, \
    StaticTable, update_table_metadata, StagedTable, Snapshot, _generate_manifest_list_path
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.typedef import Identifier, Properties, EMPTY_DICT

from universql.util import QueryError


class DuckDBIcebergCatalog(MetastoreCatalog):

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        raise NotImplementedException

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        raise NotImplementedException

    def __init__(self, conn: duckdb.DuckDBPyConnection, **properties: str):
        super().__init__("duckdb", **properties)
        self.conn = conn
        self.conn.install_extension("iceberg")
        self.conn.load_extension("iceberg")

    def create_table(self, identifier: Union[str, Identifier], schema: Union[Schema, "pa.Schema"],
                     location: Optional[str] = None, partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
                     sort_order: SortOrder = UNSORTED_SORT_ORDER, properties: Properties = EMPTY_DICT) -> Table:
        schema: Schema = self._convert_schema_if_needed(schema)  # type: ignore

        table_parts = self._get_table_address(identifier)
        database, schema_name, table = table_parts
        table_ref = '.'.join(table_parts)

        location = self._resolve_table_location(location, schema_name, table)
        metadata_location = self._get_metadata_location(location=location)
        metadata = new_table_metadata(
            location=location, schema=schema, partition_spec=partition_spec, sort_order=sort_order,
            properties=properties
        )
        io = load_file_io(properties=self.properties, location=metadata_location)
        self._write_metadata(metadata, io, metadata_location)

        table_ref = Table(
            identifier=(table_ref, ),
            metadata=metadata,
            metadata_location=metadata_location,
            io=io,
            catalog=self,
        )
        self.staged_table = table_ref
        # duckdb can't read tables that don't have any snapshot. this is a temporary hack until duckdb fixes the issue
        table_ref.overwrite(schema.as_arrow().empty_table())
        return self.register_table(identifier, metadata_location)

    def _get_table_address(self, table: Union[str, Identifier]) -> tuple[str, str, str]:
        if (len(table) == 2 and table[0] is not None) or len(table) > 2:
            raise QueryError("Namespace is not supported")
        identifier: List[Identifier] = sqlglot.parse_one(table[1] if len(table) == 2 else table[0], dialect="snowflake").parts
        if len(identifier) < 3:
            current_db, current_schema = self.conn.sql("select current_catalog(), current_setting('schema')").fetchone()
            identifier.catalog = sqlglot.exp.parse_identifier(current_db)
            identifier.db = identifier[1] if len(identifier) == 2 else sqlglot.exp.parse_identifier(current_schema),
            identifier.this = identifier[-1]

        return identifier[-3].sql(), identifier[-2].sql(), identifier[-1].sql()

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

    def _namespace_exists(self, identifier: Union[str, Identifier]) -> bool:
        namespace_tuple = Catalog.identifier_to_tuple(identifier)
        namespace = Catalog.namespace_to_string(namespace_tuple, NoSuchNamespaceError)
        self.conn.execute("from duckdb_databases() where database_name = ? limit 1", (namespace,))
        return self.conn.fetchone() != None

    def register_table(self, identifier: Union[str, Identifier], metadata_location: str) -> Table:
        table_parts = self._get_table_address(identifier)
        table_ref = '.'.join(table_parts)
        database, schema, table = table_parts
        try:
            self.conn.execute(f"CREATE VIEW {table_ref} AS FROM iceberg_scan('{metadata_location}')")
        except duckdb.IntegrityError as e:
            if not self._namespace_exists(database):
                raise NoSuchNamespaceError(f"Namespace does not exist: {database}")

            raise TableAlreadyExistsError(f"Table {table_ref} already exists") from e

        return StaticTable.from_metadata(metadata_location, self.properties)

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        table_ref = '.'.join(self._get_table_address(identifier))
        self.conn.execute(f"DROP TABLE IF EXISTS {table_ref}")

    def _update_and_stage_table(self, current_table: Optional[Table], table_request: CommitTableRequest) -> StagedTable:
        updated_metadata = update_table_metadata(
            base_metadata=current_table.metadata if current_table else self._empty_table_metadata(),
            updates=table_request.updates,
            enforce_validation=current_table is None,
        )

        new_metadata_version = self._parse_metadata_version(current_table.metadata_location) + 1 if current_table else 0
        new_metadata_location = self._get_metadata_location(updated_metadata.location, new_metadata_version)

        return StagedTable(
            identifier=tuple(table_request.identifier.namespace.root + [table_request.identifier.name]),
            metadata=updated_metadata,
            metadata_location=new_metadata_location,
            io=self._load_file_io(properties=updated_metadata.properties, location=new_metadata_location),
            catalog=self,
        )

    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        current_table: Optional[Table] = self.staged_table

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

        self.register_table(current_table.identifier, updated_staged_table.metadata_location)

        return CommitTableResponse(
            metadata=updated_staged_table.metadata, metadata_location=updated_staged_table.metadata_location
        )

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
