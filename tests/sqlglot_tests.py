import time

import pyarrow as pa

import duckdb
import sqlglot
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig

one = sqlglot.parse_one("select to_variant(test) as test from (select 1)", read="snowflake")
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
transformed_ast = ast.transform(fix_snowflake_to_duckdb_types)
query = transformed_ast.sql(dialect="duckdb", pretty=True)
print(query)
response = duckdb.sql(query)
print(response.show())
