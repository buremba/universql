import ast

import sqlglot
from sqlglot import Expression

from universql.plugin import Transformer, register
from universql.warehouse.duckdb import DuckDBCatalog, DuckDBExecutor
from universql.warehouse.snowflake import SnowflakeCatalog, SnowflakeExecutor


# when FILES is not defined
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/*' (TYPE = CSV SKIP_HEADER = 1)
# when FILES is specified:
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/device_metadata.csv' (TYPE = CSV SKIP_HEADER = 1)
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/file2.csv' (TYPE = CSV SKIP_HEADER = 1)
@register()
class SnowflakeStageTransformer(Transformer):
    def __init__(self, source_executor: SnowflakeExecutor):
        super().__init__(source_executor)

    def transform_sql(self, expression: Expression, target_executor: DuckDBExecutor) -> Expression:
        if isinstance(expression, sqlglot.exp.Var) and expression.name.startswith('@'):
            expression.args['name'] = 'myname'
            return expression

        # referenced from copy
        if isinstance(expression, sqlglot.exp.Table) and expression.alias_or_name.startswith('@'):
            self._get_stage(expression)

        return expression

    def _get_stage(self, table : sqlglot.exp.Table):
        # self.source_executor.execute_raw("DESCRIBE STAGE {}", self.source_executor)
        return


