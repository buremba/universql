
import chdb
from chdb.udf import chdb_udf


@chdb_udf(return_type="Int32")
def sumn(n):
    n = int(n)
    return n*(n+1)//2


ret = chdb.query(
    """
    CREATE TABLE iceberg_table ENGINE=Iceberg(iceberg_conf, filename = 'config')
    
""",
    path="./clickhouse",
    udf_path="./clickhouse/udf",
)
print(ret)
