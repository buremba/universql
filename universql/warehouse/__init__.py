import typing
from abc import ABC, abstractmethod
from typing import List

import sqlglot
from snowflake.connector.options import pyarrow
from sqlglot.expressions import Property


class IcebergTable:
    def __init__(self, location):
        self.location = location


class CreateRelation:
    def __init__(self, properties: List[Property], kind: str, query: sqlglot.exp.Expression):
        self.properties = properties
        self.kind = kind
        self.query = query


Location = CreateRelation | IcebergTable | pyarrow.Table
Locations = typing.Dict[sqlglot.exp.Table, Location]


class Executor(ABC):
    @abstractmethod
    def supports(self, ast: sqlglot.exp.Expression) -> bool:
        pass

    @abstractmethod
    def execute(self, ast: sqlglot.exp.Expression, locations: typing.Callable[[], Locations]) -> \
            typing.Optional[
                typing.Dict[sqlglot.exp.Table, str]]:
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
    def __init__(self, context, query_id: str, credentials: dict, compute: dict):
        self.context = context
        self.query_id = query_id
        self.credentials = credentials
        self.compute = compute

    def create(self, ast: sqlglot.exp.Table, table: pyarrow.Table):
        pass

    @abstractmethod
    def get_table_paths(self, tables: List[sqlglot.exp.Table]) -> typing.Dict[sqlglot.exp.Table, str]:
        pass

    @abstractmethod
    def register_locations(self, tables: Locations):
        pass

    @abstractmethod
    def executor(self) -> Executor:
        pass
