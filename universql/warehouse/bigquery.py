import typing
from typing import List

import google.api_core.exceptions
import sqlglot
from duckdb.experimental.spark.errors import UnsupportedOperationException
from google.cloud import bigquery
from google.cloud.bigquery import ExternalConfig
from snowflake.connector.cursor import ResultMetadataV2
from snowflake.connector.options import pyarrow
from sqlglot.expressions import Insert, Select

from universql.warehouse import Executor, ICatalog, Locations
from universql.util import sizeof_fmt, pprint_secs, QueryError
from universql.protocol.utils import get_field_for_snowflake, arrow_to_snowflake_type_id


class BigQueryIcebergExecutor(Executor):

    def __init__(self, query_id: str, tables):
        self.query_id = query_id
        self.tables = tables
        self.query = self.result = None
        self.client = bigquery.Client()

    def supports(self, ast: sqlglot.exp.Expression) -> bool:
        return ast is Select or ast is Insert

    @staticmethod
    def replace_full_reference_as_table(expression: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
        if isinstance(expression, sqlglot.exp.Table):
            bq_identifier = '___'.join([part.sql() for part in expression.parts])
            return sqlglot.exp.parse_identifier(bq_identifier, dialect="bigquery")
        return expression

    def execute_raw(self, raw_query: str, config: typing.Optional[bigquery.QueryJobConfig] = None) -> None:
        self.query = self.client.query(raw_query, location="europe-west2", project="jinjat-demo",
                                       job_config=config)

    def execute(self, ast: sqlglot.exp.Expression, locations: typing.Dict[sqlglot.exp.Table, str]) -> None:
        sql = ast.transform(self.replace_full_reference_as_table).sql(dialect="bigquery")

        definitions = {'___'.join([part.sql() for part in table.parts]):
                           BigQueryIcebergExecutor._get_config(location) for table, location in locations.items()}
        self.execute_raw(sql, bigquery.QueryJobConfig(table_definitions=definitions))

    @staticmethod
    def _get_config(location: str) -> ExternalConfig:
        config = ExternalConfig("ICEBERG")
        config.source_uris = location.replace('gcs://', 'gs://')
        return config

    def get_as_table(self) -> pyarrow.Table:
        try:
            self.result = self.query.result(timeout=None)
        except google.api_core.exceptions.GoogleAPIError as e:
            raise QueryError(f"Unable to run BigQuery: {e.args[0]}")
        arrow_all = self.result.to_arrow()
        for idx, column in enumerate(self.result.schema):
            column_type = arrow_all.schema.types[idx]
            try:
                type_code = arrow_to_snowflake_type_id(column_type)
            except ValueError as e:
                raise QueryError(e.args[0])

            (field, value) = get_field_for_snowflake(ResultMetadataV2(name=column.name,
                                                                      type_code=type_code,
                                                                      is_nullable=column.is_nullable,
                                                                      precision=column.precision,
                                                                      scale=column.scale,
                                                                      vector_dimension=None,
                                                                      fields=None,
                                                                      ), arrow_all[idx])
            arrow_all = arrow_all.set_column(idx, field, value)

        return arrow_all

    def get_query_log(self, query_duration) -> str:
        return f"Run on BigQuery bytes billed {sizeof_fmt(self.query.total_bytes_billed)}, slot milliseconds {pprint_secs(self.query.slot_millis)}"

    def close(self):
        self.result


class BigQueryCatalog(ICatalog):

    def __init__(self, context: dict, session_id: str, credentials: dict, compute: dict):
        super().__init__(context, session_id, credentials, compute)
        self.tables = None

    def executor(self) -> Executor:
        return BigQueryIcebergExecutor(self.session_id, self.tables)

    def register_locations(self, tables: Locations):
        self.tables = tables

    def get_table_paths(self, tables: List[sqlglot.exp.Table]):
        raise UnsupportedOperationException("BigQuery does not support registering tables")
