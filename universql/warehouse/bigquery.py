import typing
from typing import List

import google.api_core.exceptions
import sqlglot
from duckdb.experimental.spark.errors import UnsupportedOperationException
from google.cloud import bigquery
from google.cloud.bigquery import ExternalConfig
from snowflake.connector.cursor import ResultMetadataV2
from snowflake.connector.options import pyarrow
from universql.protocol.session import UniverSQLSession
from universql.plugin import Executor, ICatalog, Locations, register
from universql.util import sizeof_fmt, pprint_secs, QueryError
from universql.protocol.utils import get_field_for_snowflake, arrow_to_snowflake_type_id


class BigQueryIcebergExecutor(Executor):

    def __init__(self, catalog: "BigQueryCatalog"):
        super().__init__(catalog)
        self.query = self.result = None
        self.client = bigquery.Client()

    @staticmethod
    def replace_full_reference_as_table(expression: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
        if isinstance(expression, sqlglot.exp.Table):
            bq_identifier = '___'.join([part.sql() for part in expression.parts])
            return sqlglot.exp.parse_identifier(bq_identifier, dialect="bigquery")
        return expression

    def execute_raw(self, raw_query: str, catalog_executor, config: typing.Optional[bigquery.QueryJobConfig] = None) -> None:
        self.query = self.client.query(raw_query, location="europe-west2", project="jinjat-demo",
                                       job_config=config)

    def execute(self, ast: sqlglot.exp.Expression, catalog_executor: Executor,
                locations: typing.Dict[sqlglot.exp.Table, str]) -> None:
        sql = ast.transform(self.replace_full_reference_as_table).sql(dialect="bigquery")

        definitions = {'___'.join([part.sql() for part in table.parts]):
                           BigQueryIcebergExecutor._get_config(location) for table, location in locations.items()}
        self.execute_raw(sql, catalog_executor)

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


@register(name="bigquery")
class BigQueryCatalog(ICatalog):

    def __init__(self, session : UniverSQLSession):
        super().__init__(session)
        self.tables = None

    def register_locations(self, tables: Locations):
        self.tables = tables

    def get_table_paths(self, tables: List[sqlglot.exp.Table]):
        raise UnsupportedOperationException("BigQuery does not support registering tables")
    def executor(self) -> Executor:
        return BigQueryIcebergExecutor(self)