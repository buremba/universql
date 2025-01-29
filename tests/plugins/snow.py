import uuid

import sqlglot
from sqlglot import parse_one

from universql.plugins.snow import SnowflakeStageUniversqlPlugin
from universql.protocol.session import UniverSQLSession
from universql.warehouse.duckdb import DuckDBExecutor, DuckDBCatalog
from universql.warehouse.snowflake import SnowflakeExecutor, SnowflakeCatalog

one = sqlglot.parse_one("select 1", dialect="snowflake")
compute = {"warehouse": None}
session = UniverSQLSession(
    {'account': 'dhb43249.us-east-1', 'catalog': 'snowflake'}, uuid.uuid4(), {"warehouse": "duckdb()"}, {})
plugin = SnowflakeStageUniversqlPlugin(SnowflakeExecutor(SnowflakeCatalog(session, compute)))
sql = (parse_one("copy into table_name FROM @stagename/test", dialect="snowflake")
       .transform(plugin.transform_sql, DuckDBExecutor(DuckDBCatalog(session, compute))))
print(sql.sql(dialect="duckdb"))