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

from universql.warehouse.duckdb import DuckDBCatalog
from universql.warehouse.snowflake import SnowflakeCatalog


class Transformer:
    def __init__(self,
                 # allows us to call this transformer when base catalog is snowflake
                 source_engine: SnowflakeCatalog,
                 # if this is generic as Catalog, can automatically invoke
                 # .transform() no matter where it's running on
                 target_engine: DuckDBCatalog
                 ):
        self.source_engine = source_engine
        self.target_engine = target_engine

    def transform_sql(self, expression: Expression) -> Expression:
        return expression

    def transform_result(self, response: Response):
        return response

    def transform_request(self, request: Request):
        return request


class FixTimestampTypes(Transformer):

    def transform_sql(self, expression):
        if isinstance(expression, sqlglot.exp.DataType):
            if expression.this.value in ["TIMESTAMPLTZ", "TIMESTAMPTZ"]:
                return sqlglot.exp.DataType.build("TIMESTAMPTZ")
            if expression.this.value in ["VARIANT"]:
                return sqlglot.exp.DataType.build("JSON")

        return expression


class RewriteCreateAsIceberg(Transformer):

    def transform_sql(self, expression: Expression) -> Expression:
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


class SnowflakeStageTransformer(Transformer):
    def transform_sql(self, ast: Expression) -> Expression:
        if isinstance(ast, sqlglot.exp.Var) and ast.name.startswith('@'):
            # transform into full path and create secret on duckdb
            self.target_engine.duckdb.sql("select from stage information_schema.stages where ..")
            if not_exists:
                self.source_engine.executor().execute_raw("get stage info from fs")
                self.target_engine.duckdb.sql("INSERT INTO information_schema.stages ...")
            return new_ast_with_full_path
        return ast


# one = sqlglot.parse_one("create table if not exists test as select 1", read="snowflake")
one = sqlglot.parse_one("select * from @test", read="snowflake")
# one = sqlglot.parse_one("select * from 's3://test'", read="snowflake")
# one = sqlglot.parse_one("select to_variant(test) as test from (select 1)", read="snowflake")
# one = sqlglot.parse_one("create table test as select to_variant(test) as test from (select 1)", read="snowflake")

queries = one.sql(dialect="snowflake")

client = bigquery.Client()
QUERY = (
    'SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` '
    'WHERE state = "TX" '
    'LIMIT 100')
query_job = client.query(QUERY, job_config=QueryJobConfig())  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.name)

queries = sqlglot.parse("""
SELECT * FROM TABLE(
  TO_QUERY(
    'SELECT * FROM IDENTIFIER($table_name)
    WHERE deptno = TO_NUMBER(:dno)', dno => '10'
    )
  );
""", read="snowflake")

# query = sqlglot.parse_one("""
# SET stmt = $$
#     SELECT PI();
# $$;
#
# SELECT *, 1 FROM $stmt;
# """, dialect="snowflake")

fields = [
    pa.field("epoch", nullable=False, type=pa.int64()),
    pa.field("fraction", nullable=False, type=pa.int32()),
    pa.field("timezone", nullable=False, type=pa.int32()),
]
pa_type = pa.struct(fields)
pa.StructArray.from_arrays(arrays=[pa.array([1, 2, 3], type=pa.int64()), pa.array([1, 2, 3], type=pa.int32()),
                                   pa.array([1, 2, 3], type=pa.int32())], fields=fields)

query = """
SELECT
  CAST('2023-01-01 10:34:56 +00:00' AS TIMESTAMPLTZ) AS sample_timestamp_ltz,
  CAST('2023-01-01 11:34:56' AS TIMESTAMP) AS sample_timestamp_ntz,
  CAST('2023-01-01 12:34:56 +00:00' AS TIMESTAMPTZ) AS sample_timestamp_tz,
  CAST(JSON('{"key":"value"}') /* Semi-structured data types */ AS VARIANT) AS sample_variant,
"""

start = time.time()
for i in range(10):
    con = duckdb.connect(f"ali/{i}")
    con.execute("CREATE TABLE test (a int, b int)")
print(time.time() - start)
ast = sqlglot.parse_one(query, dialect="duckdb")
query = transformed_ast.sql(dialect="duckdb", pretty=True)
print(query)
response = duckdb.sql(query)
print(response.show())
