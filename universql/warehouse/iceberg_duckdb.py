import typing
from typing import Union, List

from pyiceberg.catalog import Catalog, PropertiesUpdateSummary
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, CommitFailedException
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table import SortOrder, UNSORTED_SORT_ORDER, CommitTableRequest, CommitTableResponse
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.typedef import Identifier, EMPTY_DICT
from sqlglot.expressions import Schema, Properties, Table


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
