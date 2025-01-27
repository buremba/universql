import uuid

from sqlglot import parse_one

from universql.plugins.snow import TableSampleUniversqlPlugin, SnowflakeStageUniversqlPlugin
from universql.protocol.session import UniverSQLSession
from universql.warehouse.duckdb import DuckDBExecutor, DuckDBCatalog
from universql.warehouse.snowflake import SnowflakeExecutor, SnowflakeCatalog

session = UniverSQLSession(
    {'account': 'dhb43249.us-east-1',
     'cache_directory': '/Users/bkabak/.universql/cache',
     'catalog': 'snowflake',
     'home_directory': '/Users/bkabak'}, uuid.uuid4(), {"warehouse": "duckdb()"}, {})
compute = {"warehouse": "duckdb()"}
plugin = SnowflakeStageUniversqlPlugin(SnowflakeExecutor(SnowflakeCatalog(session, compute)))
sql = (parse_one("select * from @stagename", dialect="snowflake")
       .transform(plugin.transform_sql, DuckDBExecutor(DuckDBCatalog(session, compute))))
print(sql.sql(dialect="duckdb"))