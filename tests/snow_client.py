import logging

import snowflake.connector
import urllib3

from universql.util import time_me

http = urllib3.PoolManager()

# url = 'https://127.0.0.1:8001/session/v1/login-request?request_id=d94ddf4b-4cb6-4de7-b8c5-788cc67d99af&databaseName=test'
# r = http.request('POST', url)
# print(r.data)
logging.basicConfig(level=logging.ERROR)


# connection = snowflake.connector.connect(
#     user="test",
#     password="test",
#     account="test",
#     database="test",
#     # host="127.0.0.1",
#     host="localhostcomputing.com",
#     port="8084",
# )
@time_me
def measure_all(cur, query):
    # cur.execute(
    #     "select min(convert_timezone('UTC', start_time)) from MY_CUSTOM_APP.SNOWFLAKE_COST_UNIVERSQL.stg_metering_history")
    # cur.execute("create or replace iceberg table ICEBERG_TESTS.PUBLIC.ttt EXTERNAL_VOLUME='iceberg_jinjat' CATALOG='SNOWFLAKE' BASE_LOCATION='ttt' as select * from ICEBERG_TESTS.TPCH_SF1.ORDERS  limit 1")
    cur.execute(query)
    pandas_all = cur.fetch_pandas_all()
    print(pandas_all)

@time_me
def tt(max_iteration):
    connection = snowflake.connector.connect(connection_name="jinjat_aws_us_east",
                                             port='8084', host='localhostcomputing.com',
                                             warehouse='local()'
                                             # host='4ho74nvv4nyxhqxmcxrnpiid2m0bpcet.lambda-url.us-east-1.on.aws',
                                             )
    cursor = connection.cursor()

    iteration = 0
    while True:
        # measure_all(cursor, "select o_clerk, o_shippriority, count(distinct o_clerk), count(o_orderkey), sum(o_totalprice) from ICEBERG_TESTS.TPCH_SF1.ORDERS group by all")
        # measure_all(cursor, "select count(*) from ICEBERG_TESTS.PUBLIC.GITHUB_EVENTS where cast(CREATED_AT_TIMESTAMP as date) = '2023-01-01'")
        # measure_all(cursor, "SELECT status, sum(record_count) FROM ICEBERG_METADATA('s3://universql-us-east-1/github_events/metadata/00059-769ac160-3ec9-42bc-9432-ada960817080.metadata.json') group by all")
        # measure_all(cursor, "SELECT * FROM ICEBERG_SCAN('s3://universql-us-east-1/glue_tables6/DBT_TEST/PUBLIC/example/metadata/00001-80c7021f-f25f-4187-a849-d032c2826ed4.metadata.json')")
        measure_all(cursor, """create or replace iceberg table DBT_TEST.TPCH_10.orders
           external_volume = 'iceberg_jinjat'
           catalog = 'snowflake'
           base_location = '_dbt/TPCH_10/orders'
          as
         with orders as (
                select * from DBT_TEST.TPCH_10.base_orders
            )
            select 
                o.order_key, 
                o.order_date,
                o.customer_key,
                o.order_status_code,
                o.order_priority_code,
                o.order_clerk_name,
                o.shipping_priority,
                o.order_amount
            from
                orders o
            order by
                o.order_date""")
        print(f"Done with {iteration}")
        if iteration == max_iteration:
            break
        iteration += 1

tt(1)

# cur.execute("select count(*) from MY_ICEBERG_JINJAT.PUBLIC.MY_MANAGED_ICEBERG_TABLE")
# cur.execute("SELECT seq4(), uniform(1, 10, RANDOM(12)) FROM TABLE(GENERATOR(ROWCOUNT => 100000)) v ORDER BY 1")
# cur.execute("""
# SELECT
#         -- Numeric data types
#         12345678901234567890123456789012345678::NUMBER AS sample_number,
#         123.45::DECIMAL AS sample_decimal,
#         6789::INT AS sample_int,
#         9876543210::BIGINT AS sample_bigint,
#         123::SMALLINT AS sample_smallint,
#         42::TINYINT AS sample_tinyint,
#         255::BYTEINT AS sample_byteint,
#         12345.6789::FLOAT AS sample_float,
#         123456789.123456789::DOUBLE AS sample_double,
#
#         -- String & binary data types
#         'Sample text'::VARCHAR AS sample_varchar,
#         'C'::CHAR AS sample_char,
#         'Another sample text'::STRING AS sample_string,
#         'More text'::TEXT AS sample_text,
#         TO_BINARY(HEX_ENCODE('binary_data')) AS sample_binary,
#         TO_BINARY(HEX_ENCODE('varbinary_data'))::VARBINARY AS sample_varbinary,
#
#         -- Logical data types
#         TRUE::BOOLEAN AS sample_boolean,
#
#         -- Date & time data types
#         '2023-01-01'::DATE AS sample_date,
#          '2023-01-01 12:34:56'::DATETIME AS sample_datetime,
#          '12:34:56'::TIME AS sample_time,
#          '2023-01-01 12:34:56'::TIMESTAMP AS sample_timestamp,
#          '2023-01-01 12:34:56 +00:00'::TIMESTAMP_LTZ AS sample_timestamp_ltz,
#          '2023-01-01 12:34:56'::TIMESTAMP_NTZ AS sample_timestamp_ntz,
#          '2023-01-01 12:34:56 +00:00'::TIMESTAMP_TZ AS sample_timestamp_tz,
#
#         -- Semi-structured data types
#          PARSE_JSON('{"key":"value"}')::VARIANT AS sample_variant,
#          OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(OBJECT_CONSTRUCT(), 'key1', 'value1'), 'key2', 'value2'), 'key3', 'value3') AS sample_object,
#          ARRAY_CONSTRUCT(1, 2, 3, 4) AS sample_array,
#
#         -- Geospatial data types
#          TO_GEOGRAPHY('LINESTRING(30 10, 10 30, 40 40)') AS sample_geometry,
#
#         -- Vector data types
#         [1.1,2.2,3]::VECTOR(FLOAT,3) AS sample_vector
# """)
# # cur.execute("""SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));""")
# cur.execute("""
#
# EXECUTE IMMEDIATE $$
# DECLARE
#   radius_of_circle FLOAT;
#   area_of_circle FLOAT;
# BEGIN
#   radius_of_circle := 3;
#   area_of_circle := PI() * radius_of_circle * radius_of_circle;
#   RETURN area_of_circle;
# END;
# $$
# ;
# """)

# df = pd.DataFrame(cur.fetchall(), columns=cur.description)
# df = cur.fetch_pandas_all()

# sink = pa.BufferOutputStream()
# inside = pa.BufferedInputStream()
# with pa.ipc.new_stream(sink, table.schema) as writer:
#     for batch in table.to_batches():
#         writer.write_batch(batch)
# buf = sink.getvalue()
# pybytes = buf.to_pybytes()
# b_encode = base64.b64encode(pybytes)
# encode = b_encode.decode('utf-8')

# print(df)
