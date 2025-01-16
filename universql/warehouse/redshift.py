import typing
from typing import List

import sqlglot
from duckdb.experimental.spark.errors import UnsupportedOperationException
from snowflake.connector.options import pyarrow

from universql.protocol.session import UniverSQLSession
from universql.plugin import ICatalog, Executor, Locations, Tables, register


@register(name="redshift")
class RedshiftCatalog(ICatalog):
    def __init__(self, session: UniverSQLSession, compute: dict):
        super().__init__(session, compute)

    def get_table_paths(self, tables: List[sqlglot.exp.Table]) -> Tables:
        raise UnsupportedOperationException("BigQuery does not support registering tables")

    def register_locations(self, tables: Locations):
        pass

    def executor(self) -> Executor:
        return RedshiftExecutor(self)

class RedshiftExecutor(Executor):
    def __init__(self, catalog: RedshiftCatalog):
        super().__init__(catalog)

    def execute(self, ast: sqlglot.exp.Expression, catalog_executor: Executor, locations: Tables) -> typing.Optional[Locations]:
        return None

    def execute_raw(self, raw_query: str, catalog_executor: Executor) -> None:
        pass

    def get_as_table(self) -> pyarrow.Table:
        pass

    def get_query_log(self, total_duration) -> str:
        pass

    def close(self):
        pass
