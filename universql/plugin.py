import functools
import inspect

import pyarrow
from fastapi import FastAPI
from sqlglot import Expression

import typing
from abc import ABC, abstractmethod
from typing import List

import pyiceberg.table
import sqlglot

Locations = typing.Dict[sqlglot.exp.Table, sqlglot.exp.Expression | None]
Tables = typing.Dict[sqlglot.exp.Table, pyiceberg.table.Table | None]


class ICatalog(ABC):
    def __init__(self, session: "universql.protocol.session.UniverSQLSession", compute: dict):
        self.context = session.context
        self.session_id = session.session_id
        self.credentials = session.credentials
        self.compute = compute
        self.iceberg_catalog = session.iceberg_catalog

    @abstractmethod
    def get_table_paths(self, tables: List[sqlglot.exp.Table]) -> Tables:
        pass

    @abstractmethod
    def register_locations(self, tables: Locations):
        pass

    @abstractmethod
    def executor(self) -> "Executor":
        pass


T = typing.TypeVar('T', bound=ICatalog)

def _track_call(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        # Mark that the method was called on this instance.
        self._warm = True
        return method(self, *args, **kwargs)
    return wrapper

class Executor(typing.Protocol[T]):

    def __init__(self, catalog: T):
        self.catalog = catalog

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.execute = _track_call(cls.execute)
        cls.execute_raw = _track_call(cls.execute_raw)

    def is_warm(self):
        return getattr(self, '_warm', False)

    @abstractmethod
    def execute(self, ast: sqlglot.exp.Expression, catalog_executor: "Executor", locations: Tables) -> \
            typing.Optional[Locations]:
        pass

    def test(self):
        self.execute_raw("select 1", None)

    @abstractmethod
    def execute_raw(self, raw_query: str, catalog_executor: typing.Optional["Executor"]) -> None:
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


class UniversqlPlugin:
    def __init__(self,
                 source_executor: Executor
                 ):
        self.source_executor = source_executor

    def transform_sql(self, expression: Expression, target_executor: Executor) -> Expression:
        return expression


# {"duckdb": DuckdbCatalog ..}
COMPUTES = {}
# [method]
TRANSFORMS = []
# apps to be installed
APPS = []


def register(name: typing.Optional[str] = None):
    """
    Decorator to register a Compute subclass with an optional name.
    :param name: Unique of the catalog
    :param executor: The optional executor class for the catalog
    """

    def decorator(cls):
        if inspect.isclass(cls):
            if issubclass(cls, ICatalog) and cls is not ICatalog:
                if name is None:
                    raise SystemError("name is required for catalogs")
                COMPUTES[name] = cls
            elif issubclass(cls, UniversqlPlugin) and cls is not UniversqlPlugin:
                TRANSFORMS.append(cls)
        elif inspect.isfunction(cls):
            signature = inspect.signature(cls)
            if len(signature.parameters) == 1 and signature.parameters.values().__iter__().__next__().annotation is FastAPI:
                APPS.append(cls)
        else:
            raise SystemError(f"Unknown type {cls}")
        return cls

    return decorator