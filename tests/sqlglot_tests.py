import duckdb
import sqlglot

# queries = sqlglot.parse("""
# SET tables = (SHOW TABLES);
#
# select * from tables
# """, read="snowflake")


query = sqlglot.parse_one("""
SET stmt = $$
    SELECT PI();
$$;

SELECT *, 1 FROM $stmt;
""", dialect="snowflake")

zsql = query.sql(dialect="duckdb")

query = duckdb.sql(query.sql())


print(sql)
