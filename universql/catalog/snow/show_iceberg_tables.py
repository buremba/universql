import json
import logging
import typing
from typing import Optional, List

import duckdb
import pandas as pd
import pyarrow as pa
import snowflake.connector
import sqlglot
from snowflake.connector import NotSupportedError, DatabaseError
from snowflake.connector.constants import FIELD_TYPES, FIELD_ID_TO_NAME
from snowflake.connector.cursor import ResultMetadataV2, SnowflakeCursor

from universql.catalog import IcebergCatalog, Cursor
from universql.util import SnowflakeError

MAX_LIMIT = 10000

logging.basicConfig(level=logging.INFO)
cloud_logger = logging.getLogger(f"❄️(cloud)")
class SnowflakeIcebergCursor(Cursor):
    def __init__(self, query_id, cursor: SnowflakeCursor):
        self.query_id = query_id
        self.cursor = cursor

    def execute(self, ast: sqlglot.exp.Expression) -> None:
        compiled_sql = ast.sql(dialect="snowflake")
        try:
            self.cursor.execute(compiled_sql)
            emoji = ""
            if ast.key == 'show':
                logger = cloud_logger
            else:
                emoji = "💰"
                logger = logging.getLogger(f"❄️{self.cursor.connection.warehouse}")

            logger.info(f"[{self.query_id}] Running on Snowflake.. {emoji}")
        except DatabaseError as e:
            message = f"Unable to run Snowflake query: \n {compiled_sql} \n {e.msg}"
            raise SnowflakeError(e.sfqid, message, e.sqlstate)

    def close(self):
        self.cursor.close()

    def get_v1_columns(self):
        columns = []
        for idx, col in enumerate(self.cursor._description):
            sf_type = FIELD_ID_TO_NAME[col.type_code]
            columns.append({
                "name": col.name,
                "database": "",
                "schema": "",
                "table": "",
                "nullable": True,
                "type": sf_type.lower(),
                "length": 16777216,
                "scale": 0,
                "precision": 38,
                "byteLength": 16777216,
                "collation": None
            })
        return columns

    def get_as_table(self) -> pa.Table:
        schema = pa.schema([self.get_field_for_snowflake(column) for column in self.cursor._description])
        try:
            arrow_all = self.cursor.fetch_arrow_all(force_return_table=True)
            return arrow_all.cast(schema)
        except NotSupportedError:
            values = [[] for _ in schema]

            row = self.cursor.fetchone()
            while row is not None:
                for idx, column in enumerate(row):
                    values[idx].append(column)
                row = self.cursor.fetchone()

        return pa.Table.from_pydict(dict(zip(schema.names, values)), schema=schema)

    @staticmethod
    def get_field_for_snowflake(column: ResultMetadataV2) -> pa.Field:
        arrow_field = FIELD_TYPES[column.type_code]
        if arrow_field.name == 'FIXED':
            pa_type = pa.decimal128(column.precision, column.scale)
        elif arrow_field.name == 'DATE':
            pa_type = pa.date32()
        else:
            pa_type = arrow_field.pa_type(column)

        metadata = {
            "logicalType": arrow_field.name,
            "charLength": "8388608",
            "byteLength": "8388608",
        }

        if column.precision is not None:
            metadata["precision"] = str(column.precision)
        if column.scale is not None:
            metadata["scale"] = str(column.scale)
        return pa.field(column.name, type=pa_type, nullable=column.is_nullable, metadata=metadata)


class SnowflakeShowIcebergTables(IcebergCatalog):
    def __init__(self, account : str, query_id: str, credentials: dict):
        super().__init__(query_id, credentials)
        self.databases = {}
        self.connection = snowflake.connector.connect(**credentials, account=account)

    def cursor(self) -> Cursor:
        return SnowflakeIcebergCursor(self.query_id, self.connection.cursor())

    def load_iceberg_tables(self, database: str, schema: str, after: Optional[str] = None) -> pd.DataFrame:
        query = "SHOW ICEBERG TABLES IN SCHEMA IDENTIFIER(%s) LIMIT %s", [database + '.' + schema, MAX_LIMIT]
        if after is not None:
            query[0] += " AFTER %s"
            query[1].append(after)
        tables = pd.read_sql(query[0], self.connection, params=query[1])
        if len(tables.index) >= MAX_LIMIT:
            after = tables.iloc[-1, :]["name"]
            return tables + self.load_iceberg_tables(database, schema, after=after)
        else:
            return tables
    def load_external_volumes_for_tables(self, tables: pd.DataFrame) -> pd.DataFrame:
        volumes = tables["external_volume_name"].unique()

        volume_mapping = {}
        for volume in volumes:
            volume_location = pd.read_sql("DESC EXTERNAL VOLUME identifier(%s)", self.connection, params=[volume])
            active_storage = duckdb.sql("""select property_value from volume_location 
                        where parent_property = 'STORAGE_LOCATIONS' and property = 'ACTIVE'
                       """).fetchall()[0][0]
            all_properties = duckdb.execute("""select property_value from volume_location
                    where parent_property = 'STORAGE_LOCATIONS' and property like 'STORAGE_LOCATION_%'""").fetchall()
            for properties in all_properties:
                loads = json.loads(properties[0])
                if loads.get('NAME') == active_storage:
                    volume_mapping[volume] = loads
                    break
        return volume_mapping

    def load_database_schema(self, database: str, schema: str):
        tables = self.load_iceberg_tables(database, schema)
        external_volumes = self.load_external_volumes_for_tables(tables)

        tables["external_location"] = tables.apply(
            lambda x: (external_volumes[x["external_volume_name"]].get('STORAGE_BASE_URL')
                       + x["base_location"]), axis=1)
        if database not in self.databases:
            self.databases[database] = {}

        self.databases[database][schema] = dict(zip(tables.name, tables.external_location))

    def get_table_references(self, cursor: duckdb.DuckDBPyConnection, tables: List[sqlglot.exp.Table]) -> typing.Dict[
        sqlglot.exp.Table, sqlglot.exp.Expression]:
        if len(tables) == 0:
            return {}
        sqls = ["SYSTEM$GET_ICEBERG_TABLE_INFORMATION(%s)" for _ in tables]
        values = [table.sql(dialect="snowflake") for table in tables]
        cur = self.connection.cursor()
        final_query = f"SELECT {(', '.join(sqls))}"
        cur.execute(final_query, values)

        result = cur.fetchall()
        used_tables = ",".join(set(table.sql() for table in tables))
        logging.getLogger("❄️cloud").info(f"Executed metadata query to get Iceberg table locations for tables {used_tables}")
        return {table: SnowflakeShowIcebergTables._get_ref(json.loads(result[0][idx])) for idx, table in
                enumerate(tables)}

    @staticmethod
    def _get_ref(table_information):
        location = table_information.get('metadataLocation')
        return sqlglot.exp.func("iceberg_scan",
                                sqlglot.exp.Literal.string(location))

    def find_table_location(self, database: str, schema: str, table_name: str, lazy_check: bool = True) -> str:
        table_location = self.databases.get(database, {}).get(schema, {}).get(table_name)
        if table_location is None:
            if lazy_check:
                self.load_database_schema(database, schema)
                return self.find_table_location(database, schema, table_name, lazy_check=False)
            else:
                raise Exception(f"Table {table_name} not found in {database}.{schema}")
        return table_location
