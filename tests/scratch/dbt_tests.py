import os

from dbt.cli.main import dbtRunner

os.chdir("/Users/bkabak/Code/jinjat/snowflake_admin")
dbt = dbtRunner()

cli_args = ["show", "--inline", "select 1"]
dbt.invoke(cli_args)