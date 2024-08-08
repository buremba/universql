import duckdb
import sqlglot

queries_that_doesnt_need_warehouse = ["show"]


def should_run_on_catalog(ast: sqlglot.exp.Expression) -> bool:
    return ast.name in queries_that_doesnt_need_warehouse \
        or ast.key in queries_that_doesnt_need_warehouse


class DuckDBFunctions:
    @staticmethod
    def register(db: duckdb.DuckDBPyConnection):
        db.create_function("CURRENT_WAREHOUSE", DuckDBFunctions.current_warehouse, [], duckdb.typing.VARCHAR)
        pass

    @staticmethod
    def current_warehouse() -> str:
        return "x-duck"
