import typing
from typing import List

import pyarrow
import sqlglot
from sqlglot import Expression
from sqlglot.expressions import TableSample

from universql.plugin import UniversqlPlugin, register, UQuery, Locations
from universql.warehouse.duckdb import DuckDBExecutor
from universql.warehouse.snowflake import SnowflakeExecutor


# when FILES is not defined
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/*' (TYPE = CSV SKIP_HEADER = 1)
# when FILES is specified:
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/device_metadata.csv' (TYPE = CSV SKIP_HEADER = 1)
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/file2.csv' (TYPE = CSV SKIP_HEADER = 1)

class StageTransformer(UQuery):
    def transform_ast(self, expression: Expression, target_executor: DuckDBExecutor) -> Expression:
        if isinstance(expression, sqlglot.exp.Var) and expression.name.startswith('@'):
            expression.args['name'] = 'myname'
            return expression

        # referenced from copy
        if isinstance(expression, sqlglot.exp.Table) and expression.alias_or_name.startswith('@'):
            self._get_stage(expression)

        return expression

    def _get_stage(self, table: sqlglot.exp.Table):
        # self.source_executor.execute_raw("DESCRIBE STAGE {}", self.source_executor)
        return

    def post_execute(self, locations: typing.Optional[Locations], target_executor: DuckDBExecutor):
        pass

    def end(self, table : pyarrow.Table):
        pass

@register()
class SnowflakeStageUniversqlPlugin(UniversqlPlugin):
    def __init__(self, session: "universql.protocol.session.UniverSQLSession"):
        super().__init__(session)

    def start_query(self, ast: typing.Optional[List[sqlglot.exp.Expression]], raw_query: str) -> UQuery:
        return StageTransformer(ast, raw_query)


# @register()
class TableSampleUniversqlPlugin(UniversqlPlugin):
    def __init__(self, source_executor: SnowflakeExecutor):
        super().__init__(source_executor)

    def transform_sql(self, expression: Expression, target_executor: DuckDBExecutor,
                      sample="reservoir(1000)") -> Expression:
        if not isinstance(target_executor, DuckDBExecutor):
            raise NotImplementedError
        if isinstance(expression, sqlglot.exp.Select) and 'sample' not in expression.args:
            sample_value = sqlglot.exp.maybe_parse(sample)
            expression.args['sample'] = sqlglot.exp.TableSample(
                method=sqlglot.exp.Var(this=sample_value.this),
                size=sample_value.expressions[0])
        return expression
