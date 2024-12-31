from string import Template

import duckdb


connect = duckdb.connect(":memory:")
connect.execute("ATTACH 'md:'")
connect.execute("select * from MD_ALL_DATABASES()")

connect = duckdb.connect(":memory:")
connect.execute("ATTACH 'md:'")
fetchdf = connect.sql("SHOW DATABASES").fetchdf()
print(fetchdf)