import typing
from abc import ABC, abstractmethod
from typing import List
from uuid import uuid4

import duckdb
import sqlglot
from snowflake.connector.options import pyarrow

from universql.util import Catalog, Compute, SNOWFLAKE_HOST


# for service-service communication inside Snowflake Container Service

def get_catalog(context: dict, query_id: str, credentials: dict):
    catalog = context.get('catalog')
    account = context.get('account')
    if catalog == Catalog.SNOWFLAKE.value:
        must_run_locally = context.get('compute') == Compute.LOCAL.value
        if must_run_locally:
            # make sure snowflake compute is not used
            credentials["warehouse"] = str(uuid4())

        if context.get('account') is not None:
            credentials["account"] = context.get('account')
        if SNOWFLAKE_HOST is not None:
            credentials["host"] = SNOWFLAKE_HOST
        from universql.catalog.snowflake import SnowflakeShowIcebergTables
        return SnowflakeShowIcebergTables(must_run_locally, query_id, credentials)
    elif catalog == Catalog.POLARIS.value:
        from universql.catalog.iceberg import PolarisIcebergCatalog
        return PolarisIcebergCatalog(context.get('cache_directory'), account, query_id, credentials)

    raise ValueError(f"Unsupported catalog: {catalog}")


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
