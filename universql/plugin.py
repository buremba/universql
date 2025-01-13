from sqlglot import Expression
from starlette.requests import Request
from starlette.responses import Response

from universql.util import Catalog


class Transformer:
    def __init__(self,
                 source_engine: Catalog,
                 target_engine: Catalog
                 ):
        self.source_engine = source_engine
        self.target_engine = target_engine

    def transform_sql(self, expression: Expression) -> Expression:
        return expression

    def transform_result(self, response: Response):
        return response

    def transform_request(self, request: Request):
        return request