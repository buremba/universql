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
def measure_all(**args):
    connection = snowflake.connector.connect(**args)
    cur = connection.cursor()
    # cur.execute(
    #     "select min(convert_timezone('UTC', start_time)) from MY_CUSTOM_APP.SNOWFLAKE_COST_UNIVERSQL.stg_metering_history")
    cur.execute("create or replace iceberg table ICEBERG_TESTS.PUBLIC.ttt EXTERNAL_VOLUME='iceberg_jinjat' CATALOG='SNOWFLAKE' BASE_LOCATION='ttt' as select * from ICEBERG_TESTS.TPCH_SF1.ORDERS  limit 1")
    cur.execute("select * from ICEBERG_TESTS.TPCH_SF1.ORDERS  limit 1")
    cur.fetchone()
    cur.close()

@time_me
def tt(max_iteration):
    # cloud = measure_all(connection_name="default") # 0.4s
    iteration = 0
    while True:
        # measure_all(connection_name="default", port='8084', host='localhostcomputing.com') # 1.4s
        measure_all(connection_name="jinjat_aws_us_east",
                    # port='8084', host='localhostcomputing.com',
                    host='4ho74nvv4nyxhqxmcxrnpiid2m0bpcet.lambda-url.us-east-1.on.aws',
                    )
        print(f"Done with {iteration}")
        if iteration == max_iteration:
            break
        iteration += 1

tt(3)

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
