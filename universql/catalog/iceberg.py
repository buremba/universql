import logging
from typing import List

import sqlglot
from pyarrow import Table
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, OAuthError
from pyiceberg.io import PY_IO_IMPL

from universql.warehouse import ICatalog, Executor
from universql.lake.cloud import CACHE_DIRECTORY_KEY
from universql.util import SnowflakeError

logger = logging.getLogger("🧊")


class PolarisCatalog(ICatalog):

    def __init__(self, context: dict, query_id: str, credentials: dict, compute: dict):
        super().__init__(context, query_id, credentials, compute)
        current_database = credentials.get('database')
        if current_database is None:
            raise SnowflakeError(query_id, "No database/catalog provided, unable to connect to Polaris catalog")
        iceberg_rest_credentials = {
            "uri": f"https://{context.get('account')}.snowflakecomputing.com/polaris/api/catalog",
            "credential": f"{credentials.get('user')}:{credentials.get('password')}",
            "warehouse": current_database,
            "scope": "PRINCIPAL_ROLE:ALL"
        }
        try:
            self.rest_catalog = load_catalog(None, **iceberg_rest_credentials)
        except OAuthError as e:
            raise SnowflakeError(self.query_id, e.args[0])
        self.rest_catalog.properties[CACHE_DIRECTORY_KEY] = context.get('cache_directory')
        self.rest_catalog.properties[PY_IO_IMPL] = "universql.lake.cloud.iceberg"

    def executor(self) -> Executor:
        raise SnowflakeError(self.query_id, "Polaris doesn't support direct execution")

    def get_table_paths(self, tables: List[sqlglot.exp.Table]):
        return {table: self._get_table(table) for table in tables}

    def _get_table(self, table: sqlglot.exp.Table) -> Table:
        table_ref = table.sql(dialect="snowflake")
        try:
            iceberg_table = self.rest_catalog.load_table(table_ref)
        except NoSuchTableError:
            error = f"Table {table_ref} doesn't exist in Polaris catalog `{self.credentials.get('database')}` or your role doesn't have access to the table."
            logger.error(error)
            raise SnowflakeError(self.query_id, error)
        return iceberg_table.scan().to_arrow()
