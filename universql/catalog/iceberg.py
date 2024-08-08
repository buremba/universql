import logging
import typing
from typing import List

import duckdb
import pyiceberg
import sqlglot
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, OAuthError
from pyiceberg.io import PY_IO_IMPL
from snowflake.connector.options import pyarrow

from universql.catalog import IcebergCatalog, Cursor
from universql.lake.cloud import CACHE_DIRECTORY_KEY
from universql.util import SnowflakeError

logger = logging.getLogger("🧊")


class IcebergDuckDBCursor(Cursor):
    def __init__(self, query_id: str, catalog: pyiceberg.catalog.Catalog):
        self.query_id = query_id
        self.catalog = catalog

    def execute(self, ast: typing.Optional[sqlglot.exp.Expression], raw_query : str) -> None:
        raise SnowflakeError(self.query_id, "Polaris catalog only supports read-only queries")

    def get_as_table(self) -> pyarrow.Table:
        pass

    def get_v1_columns(self) -> typing.List[dict]:
        pass

    def close(self):
        pass


class PolarisIcebergCatalog(IcebergCatalog):
    def __init__(self, cache_directory: str, account: str, query_id: str, credentials: dict):
        super().__init__(query_id, credentials)
        current_database = credentials.get('database')
        if current_database is None:
            raise SnowflakeError(query_id, "No database/catalog provided, unable to connect to Polaris catalog")
        iceberg_rest_credentials = {
            "uri": f"https://{account}.snowflakecomputing.com/polaris/api/catalog",
            "credential": f"{credentials.get('user')}:{credentials.get('password')}",
            "warehouse": current_database,
            "scope": "PRINCIPAL_ROLE:ALL"
        }
        try:
            self.rest_catalog = load_catalog(None, **iceberg_rest_credentials)
        except OAuthError as e:
            raise SnowflakeError(self.query_id, e.args[0])
        self.rest_catalog.properties[CACHE_DIRECTORY_KEY] = cache_directory
        self.rest_catalog.properties[PY_IO_IMPL] = "universql.lake.cloud.iceberg"

    def get_table_references(self, cursor: duckdb.DuckDBPyConnection, tables: List[sqlglot.exp.Table]) -> typing.Dict[
        sqlglot.exp.Table, sqlglot.exp.Expression]:
        return {table: self._get_table(cursor, table) for table in tables}

    def _get_table(self, cursor: duckdb.DuckDBPyConnection, table: sqlglot.exp.Table) -> sqlglot.exp.Expression:
        table_ref = table.sql(dialect="snowflake")
        try:
            iceberg_table = self.rest_catalog.load_table(table_ref)
        except NoSuchTableError:
            error = f"Table {table_ref} doesn't exist in Polaris catalog `{self.credentials.get('database')}` or your role doesn't have access to the table."
            logger.error(error)
            raise SnowflakeError(self.query_id, error)
        table_ref_sql = table.sql()
        cursor.register(table_ref_sql, iceberg_table.scan().to_arrow())
        return sqlglot.exp.parse_identifier(table_ref_sql)

    def cursor(self) -> Cursor:
        return IcebergDuckDBCursor(self.query_id, self.rest_catalog)
