from pyiceberg.catalog import load_catalog
from pyiceberg.io import PY_IO_IMPL
from pyiceberg.table import StaticTable

catalog = load_catalog(
    name="polaris"
)
catalog.properties[PY_IO_IMPL] = "universql.lake.cloud.iceberg"

table = catalog.load_table("public.taxi_dataset")

arrow = table.scan().to_arrow()
table.current_snapshot()

# df = pq.read_table("/tmp/yellow_tripdata_2023-01.parquet")

# table = catalog.create_table(
#     "public.taxi_dataset",
#     schema=df.schema,
# )

# table.append(df)
# len(table.scan().to_arrow())

# df = df.append_column("tip_per_mile", pc.divide(df["tip_amount"], df["trip_distance"]))
# with table.update_schema() as update_schema:
#     update_schema.union_by_name(df.schema)

# table.overwrite(df)
# print(table.scan().to_arrow())

df = table.scan(row_filter="tip_per_mile > 0").to_arrow()
len(df)
