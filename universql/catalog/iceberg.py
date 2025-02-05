import logging
from typing import List

import sqlglot
from pyarrow import Table
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, OAuthError
from pyiceberg.io import PY_IO_IMPL

from universql.plugin import Locations, ICatalog
from universql.protocol.session import UniverSQLSession
from universql.lake.cloud import CACHE_DIRECTORY_KEY
from universql.util import SnowflakeError

logger = logging.getLogger("🧊")


class PolarisCatalog(ICatalog):

    def register_locations(self, tables: Locations):
        raise SnowflakeError(self.session_id, "Polaris doesn't support direct execution")

    def __init__(self, session : UniverSQLSession, compute: dict):
        super().__init__(session, compute)
        current_database = session.credentials.get('database')
        if current_database is None:
            raise SnowflakeError(session.session_id, "No database/catalog provided, unable to connect to Polaris catalog")
        iceberg_rest_credentials = {
            "uri": f"https://{session.context.get('account')}.snowflakecomputing.com/polaris/api/catalog",
            "credential": f"{session.credentials.get('user')}:{session.credentials.get('password')}",
            "warehouse": current_database,
            "scope": "PRINCIPAL_ROLE:ALL"
        }
        try:
            self.rest_catalog = load_catalog(None, **iceberg_rest_credentials)
        except OAuthError as e:
            raise SnowflakeError(self.session_id, e.args[0])
        self.rest_catalog.properties[CACHE_DIRECTORY_KEY] = session.context.get('cache_directory')
        self.rest_catalog.properties[PY_IO_IMPL] = "universql.lake.cloud.iceberg"

    def get_table_paths(self, tables: List[sqlglot.exp.Table]):
        return {table: self._get_table(table) for table in tables}

    def _get_table(self, table: sqlglot.exp.Table) -> Table:
        table_ref = table.sql(dialect="snowflake")
        try:
            iceberg_table = self.rest_catalog.load_table(table_ref)
        except NoSuchTableError:
            error = f"Table {table_ref} doesn't exist in Polaris catalog `{self.credentials.get('database')}` or your role doesn't have access to the table."
            logger.error(error)
            raise SnowflakeError(self.session_id, error)
        return iceberg_table.scan().to_arrow()