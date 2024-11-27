import typing
from abc import ABC, abstractmethod
from typing import List

import pyiceberg.table
import sqlglot
from pyiceberg.catalog import Catalog
from snowflake.connector.options import pyarrow

Locations = typing.Dict[sqlglot.exp.Table, sqlglot.exp.Expression | None]
Tables = typing.Dict[sqlglot.exp.Table, pyiceberg.table.Table | None]


class Executor(ABC):
    @abstractmethod
    def supports(self, ast: sqlglot.exp.Expression) -> bool:
        pass

    @abstractmethod
    def execute(self, ast: sqlglot.exp.Expression, locations: Tables) -> \
            typing.Optional[Locations]:
        pass

    @abstractmethod
    def execute_raw(self, raw_query: str) -> None:
        pass

    @abstractmethod
    def get_as_table(self) -> pyarrow.Table:
        pass

    @abstractmethod
    def get_query_log(self, total_duration) -> str:
        pass

    @abstractmethod
    def close(self):
        pass


class ICatalog(ABC):
    def __init__(self, context, session_id: str, credentials: dict, compute: dict, iceberg_catalog: Catalog):
        self.context = context
        self.session_id = session_id
        self.credentials = credentials
        self.compute = compute
        self.iceberg_catalog = iceberg_catalog

    def create(self, ast: sqlglot.exp.Table, table: pyarrow.Table):
        pass

    @abstractmethod
    def get_table_paths(self, tables: List[sqlglot.exp.Table]) -> Tables:
        pass

    @abstractmethod
    def register_locations(self, tables: Locations):
        pass

    @abstractmethod
    def executor(self) -> Executor:
        pass
