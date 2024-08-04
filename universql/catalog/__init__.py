import typing
from abc import ABC, abstractmethod
from typing import List
import duckdb
import sqlglot
from snowflake.connector.options import pyarrow

from universql import util
from universql.util import Catalog


def get_catalog(context: dict, query_id: str, credentials: dict):
    catalog = context.get('catalog')
    account = context.get('account')
    if catalog == Catalog.SNOWFLAKE.value:
        from universql.catalog.snow.show_iceberg_tables import SnowflakeShowIcebergTables
        return SnowflakeShowIcebergTables(account, query_id, credentials)
    elif catalog == Catalog.POLARIS.value:
        from universql.catalog.snow.polaris import PolarisCatalog
        return PolarisCatalog(context.get('cache_directory'), account, query_id, credentials)

    raise ValueError(f"Unsupported catalog: {util.args.catalog}")


class Cursor(ABC):
    @abstractmethod
    def execute(self, ast: typing.Optional[sqlglot.exp.Expression], raw_query : str) -> None:
        pass

    @abstractmethod
    def get_as_table(self) -> pyarrow.Table:
        pass

    @abstractmethod
    def get_v1_columns(self) -> typing.List[dict]:
        pass

    @abstractmethod
    def close(self):
        pass


class IcebergCatalog(ABC):
    def __init__(self, query_id: str, credentials: dict):
        self.query_id = query_id
        self.credentials = credentials

    @abstractmethod
    def get_table_references(self, cursor: duckdb.DuckDBPyConnection, tables: List[sqlglot.exp.Table]) -> typing.Dict[
        sqlglot.exp.Table, sqlglot.exp.Expression]:
        pass

    @abstractmethod
    def cursor(self) -> Cursor:
        pass
