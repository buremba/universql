import typing
from typing import List

import sqlglot
from snowflake.connector.options import pyarrow
from sqlglot.expressions import Select, Insert, Create

from universql.warehouse import ICatalog, Executor, Locations, Tables


class RedshiftCatalog(ICatalog):
    def get_table_paths(self, tables: List[sqlglot.exp.Table]) -> Tables:
        pass

    def register_locations(self, tables: Locations):
        pass

    def executor(self) -> Executor:
        pass


class RedshiftExecutor(Executor):
    def supports(self, ast: sqlglot.exp.Expression) -> bool:
        return isinstance(ast, Select) or isinstance(ast, Insert) or isinstance(ast, Create)

    def execute(self, ast: sqlglot.exp.Expression, locations: Tables) -> typing.Optional[Locations]:
        return None

    def execute_raw(self, raw_query: str) -> None:
        pass

    def get_as_table(self) -> pyarrow.Table:
        pass

    def get_query_log(self, total_duration) -> str:
        pass

    def close(self):
        pass
