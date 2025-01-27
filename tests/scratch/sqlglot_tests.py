import time
from abc import abstractmethod

import pyarrow as pa

import duckdb
import sqlglot
from fastapi.openapi.models import Response
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from sqlglot import Expression
from sqlglot.expressions import TransientProperty, TemporaryProperty, Properties, IcebergProperty
from starlette.requests import Request

from universql.plugin import UniversqlPlugin, Executor
from universql.warehouse.duckdb import DuckDBCatalog, DuckDBExecutor
from universql.warehouse.snowflake import SnowflakeCatalog

# SELECT ascii(t.$1), ascii(t.$2) FROM 's3://fullpath' (file_format_for_duckdb => myformat) t;

SnowflakeStageTransformer(SnowflakeCatalog())
one = sqlglot.parse_one("SELECT ascii(t.$1), ascii(t.$2) FROM @mystage1 (file_format => myformat) t;", read="snowflake")
two = sqlglot.parse_one("""COPY INTO stg_device_metadata
FROM @iceberg_db.public.landing_stage/initial_objects/
--FILES = ('device_metadata.csv', 'file2.csv')
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);""", read="snowflake")


class FixTimestampTypes(UniversqlPlugin):

    def transform_sql(self, ast, target_executor: Executor):
        def fix_timestamp_types(expression):
            if isinstance(target_executor, DuckDBExecutor) and isinstance(expression, sqlglot.exp.DataType):
                if expression.this.value in ["TIMESTAMPLTZ", "TIMESTAMPTZ"]:
                    return sqlglot.exp.DataType.build("TIMESTAMPTZ")
                if expression.this.value in ["VARIANT"]:
                    return sqlglot.exp.DataType.build("JSON")

        return ast.transform(fix_timestamp_types)


class RewriteCreateAsIceberg(UniversqlPlugin):

    def transform_sql(self, expression: Expression, target_executor: Executor) -> Expression:
        prefix = self.catalog.iceberg_catalog.properties.get("location")

        if isinstance(expression, sqlglot.exp.Create):
            if expression.kind == 'TABLE':
                properties = expression.args.get('properties') or Properties()
                is_transient = TransientProperty() in properties.expressions
                is_temp = TemporaryProperty() in properties.expressions
                is_iceberg = IcebergProperty() in properties.expressions
                if is_transient or len(properties.expressions) == 0:
                    properties__set = Properties()
                    external_volume = Property(this=Var(this='EXTERNAL_VOLUME'),
                                               value=Literal.string(
                                                   self.catalog.context.get('snowflake_iceberg_volume')))
                    snowflake_catalog = self.catalog.iceberg_catalog or "snowflake"
                    catalog = Property(this=Var(this='CATALOG'), value=Literal.string(snowflake_catalog))
                    if snowflake_catalog == 'snowflake':
                        base_location = Property(this=Var(this='BASE_LOCATION'),
                                                 value=Literal.string(location.metadata_location[len(prefix):]))
                    elif snowflake_catalog == 'glue':
                        base_location = Property(this=Var(this='CATALOG_TABLE_NAME'),
                                                 value=Literal.string(expression.this.sql()))
                    create_table_props = [IcebergProperty(), external_volume, catalog, base_location]
                    properties__set.set('expressions', create_table_props)

                    metadata_query = expression.expression.sql(dialect="snowflake")
                    try:
                        self.catalog.cursor().describe(metadata_query)
                    except Exception as e:
                        logger.error(f"Unable fetching schema for metadata query {e.args} \n" + metadata_query)
                        return expression
                    columns = [(column.name, FIELD_TYPES[column.type_code]) for column in
                               self.catalog.cursor().description]
                    unsupported_columns = [(column[0], column[1].name) for column in columns if column[1].name not in (
                        'BOOLEAN', 'TIME', 'BINARY', 'TIMESTAMP_TZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP',
                        'DATE', 'FIXED',
                        'TEXT', 'REAL')]
                    if len(unsupported_columns) > 0:
                        logger.error(
                            f"Unsupported columns {unsupported_columns} in {expression.expression.sql(dialect='snowflake')}")
                        return expression

                    column_definitions = [ColumnDef(
                        this=sqlglot.exp.parse_identifier(column[0]),
                        kind=DataType.build(self._convert_snowflake_to_iceberg_type(column[1]), dialect="snowflake"))
                        for
                        column in
                        columns]
                    schema = Schema()
                    schema.set('this', expression.this)
                    schema.set('expressions', column_definitions)
                    expression.set('this', schema)
                    select = Select().from_(Subquery(this=expression.expression))
                    for column in columns:
                        col_ast = Column(this=parse_identifier(column[0]))
                        if column[1].name in ('ARRAY', 'OBJECT'):
                            alias = Alias(this=Anonymous(this="to_variant", expressions=[col_ast]),
                                          alias=parse_identifier(column[0]))
                            select = select.select(alias)
                        else:
                            select = select.select(col_ast)

                    expression.set('expression', select)
                    expression.set('properties', properties__set)
        return expression


# one = sqlglot.parse_one("create table if not exists test as select 1", read="snowflake")
one = sqlglot.parse_one("select * from @test", read="snowflake")
# one = sqlglot.parse_one("select * from 's3://test'", read="snowflake")
# one = sqlglot.parse_one("select to_variant(test) as test from (select 1)", read="snowflake")
# one = sqlglot.parse_one("create table test as select to_variant(test) as test from (select 1)", read="snowflake")
