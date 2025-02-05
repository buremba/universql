from itertools import product

import pytest
from snowflake.connector import ProgrammingError

from tests.integration.utils import execute_query, universql_connection, SIMPLE_QUERY, ALL_COLUMNS_QUERY

class TestConnectivity:
    def test_invalid_auth(self):
        with universql_connection(password="invalidPass") as conn:
            with pytest.raises(ProgrammingError, match="Incorrect username or password was specified"):
                execute_query(conn, "SHOW TABLES LIMIT 1")

        with universql_connection(password="invalidPass") as conn:
            with pytest.raises(ProgrammingError, match="Incorrect username or password was specified"):
                execute_query(conn, "SELECT 1")

        with universql_connection(password="invalidPass") as conn:
            with pytest.raises(ProgrammingError, match="Incorrect username or password was specified"):
                execute_query(conn, "CREATE TEMP TABLE test_table AS SELECT 1 as t; SELECT * FROM test_table;")


class TestSelect:
    def test_simple_select(self):
        with universql_connection() as conn:
            universql_result = execute_query(conn, SIMPLE_QUERY)
            print(universql_result)

    def test_complex_select(self):
        with universql_connection() as conn:
            universql_result = execute_query(conn, ALL_COLUMNS_QUERY)
            print(universql_result)

    def test_switch_schema(self):
        with universql_connection() as conn:
            execute_query(conn, "USE DATABASE snowflake")
            universql_result = execute_query(conn, "SHOW SCHEMAS")
            assert universql_result.num_rows > 0, f"The query did not return any rows!"

            execute_query(conn, "USE SCHEMA snowflake.account_usage")
            universql_result = execute_query(conn, "SHOW SCHEMAS")
            assert universql_result.num_rows > 0, f"The query did not return any rows!"

            execute_query(conn, "USE snowflake")
            universql_result = execute_query(conn, "SHOW SCHEMAS")
            assert universql_result.num_rows > 0, f"The query did not return any rows!"

            execute_query(conn, "USE snowflake.account_usage")
            universql_result = execute_query(conn, "SHOW SCHEMAS")
            assert universql_result.num_rows > 0, f"The query did not return any rows!"

    def test_success_after_failure(self):
        with universql_connection(warehouse=None) as conn:
            with pytest.raises(ProgrammingError):
                execute_query(conn, "select * from not_exists")
            result = execute_query(conn, "select 1")
            assert result.num_rows == 1

    def test_union(self):
        with universql_connection(warehouse=None) as conn:
            result = execute_query(conn, "select 1 union all select 2")
            assert result.num_rows == 2


    def test_stage(self):
        with universql_connection(warehouse=None, database="MY_ICEBERG_JINJAT", schema="TPCH_SF1") as conn:
            result = execute_query(conn, """
            select * FROM @clickhouse_public_data_stage/ limit 1
            """)
            # result = execute_query(conn, "select * from @iceberg_db.public.landing_stage/initial_objects/device_metadata.csv")
            assert result.num_rows > 0

    def test_copy_into_for_ryan(self):
        with universql_connection(snowflake_connection_name='ryan_snowflake', warehouse=None, database="ICEBERG_DB") as conn:

            result = execute_query(conn, """
                CREATE OR REPLACE TEMPORARY TABLE DEVICE_METADATA_REF (
                device_id VARCHAR,
                device_name VARCHAR,
                device_type VARCHAR,
                manufacturer VARCHAR,
                model_number VARCHAR,
                firmware_version VARCHAR,
                installation_date DATE,
                location_id VARCHAR,
                location_name VARCHAR,
                facility_zone VARCHAR,
                is_active BOOLEAN,
                expected_lifetime_months INT,
                maintenance_interval_days INT,
                last_maintenance_date DATE
            );

             COPY INTO DEVICE_METADATA_REF
             FROM @iceberg_db.public.landing_stage/initial_objects/device_metadata.csv
             FILE_FORMAT = (SKIP_HEADER = 1);
             """)
            assert result.num_rows != 0

    def test_clickbench(self):
        with universql_connection(warehouse=None) as conn:
            result = execute_query(conn, """
            CREATE TEMP TABLE hits2 AS SELECT 
                CAST(WatchID AS BIGINT) AS WatchID,
                CAST(JavaEnable AS SMALLINT) AS JavaEnable,
                CAST(Title AS TEXT) AS Title,
                CAST(GoodEvent AS SMALLINT) AS GoodEvent,
                epoch_ms(EventTime * 1000) AS EventTime,
                DATE '1970-01-01' + INTERVAL (EventDate) DAYS AS EventDate,
                CAST(CounterID AS INTEGER) AS CounterID,
                CAST(ClientIP AS INTEGER) AS ClientIP,
                CAST(RegionID AS INTEGER) AS RegionID,
                CAST(UserID AS BIGINT) AS UserID,
                CAST(CounterClass AS SMALLINT) AS CounterClass,
                CAST(OS AS SMALLINT) AS OS,
                CAST(UserAgent AS SMALLINT) AS UserAgent,
                CAST(URL AS TEXT) AS URL,
                CAST(Referer AS TEXT) AS Referer,
                CAST(IsRefresh AS SMALLINT) AS IsRefresh,
                CAST(RefererCategoryID AS SMALLINT) AS RefererCategoryID,
                CAST(RefererRegionID AS INTEGER) AS RefererRegionID,
                CAST(URLCategoryID AS SMALLINT) AS URLCategoryID,
                CAST(URLRegionID AS INTEGER) AS URLRegionID,
                CAST(ResolutionWidth AS SMALLINT) AS ResolutionWidth,
                CAST(ResolutionHeight AS SMALLINT) AS ResolutionHeight,
                CAST(ResolutionDepth AS SMALLINT) AS ResolutionDepth,
                CAST(FlashMajor AS SMALLINT) AS FlashMajor,
                CAST(FlashMinor AS SMALLINT) AS FlashMinor,
                CAST(FlashMinor2 AS TEXT) AS FlashMinor2,
                CAST(NetMajor AS SMALLINT) AS NetMajor,
                CAST(NetMinor AS SMALLINT) AS NetMinor,
                CAST(UserAgentMajor AS SMALLINT) AS UserAgentMajor,
                CAST(UserAgentMinor AS VARCHAR(255)) AS UserAgentMinor,
                CAST(CookieEnable AS SMALLINT) AS CookieEnable,
                CAST(JavascriptEnable AS SMALLINT) AS JavascriptEnable,
                CAST(IsMobile AS SMALLINT) AS IsMobile,
                CAST(MobilePhone AS SMALLINT) AS MobilePhone,
                CAST(MobilePhoneModel AS TEXT) AS MobilePhoneModel,
                CAST(Params AS TEXT) AS Params,
                CAST(IPNetworkID AS INTEGER) AS IPNetworkID,
                CAST(TraficSourceID AS SMALLINT) AS TraficSourceID,
                CAST(SearchEngineID AS SMALLINT) AS SearchEngineID,
                CAST(SearchPhrase AS TEXT) AS SearchPhrase,
                CAST(AdvEngineID AS SMALLINT) AS AdvEngineID,
                CAST(IsArtifical AS SMALLINT) AS IsArtifical,
                CAST(WindowClientWidth AS SMALLINT) AS WindowClientWidth,
                CAST(WindowClientHeight AS SMALLINT) AS WindowClientHeight,
                CAST(ClientTimeZone AS SMALLINT) AS ClientTimeZone,
                epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
                CAST(SilverlightVersion1 AS SMALLINT) AS SilverlightVersion1,
                CAST(SilverlightVersion2 AS SMALLINT) AS SilverlightVersion2,
                CAST(SilverlightVersion3 AS INTEGER) AS SilverlightVersion3,
                CAST(SilverlightVersion4 AS SMALLINT) AS SilverlightVersion4,
                CAST(PageCharset AS TEXT) AS PageCharset,
                CAST(CodeVersion AS INTEGER) AS CodeVersion,
                CAST(IsLink AS SMALLINT) AS IsLink,
                CAST(IsDownload AS SMALLINT) AS IsDownload,
                CAST(IsNotBounce AS SMALLINT) AS IsNotBounce,
                CAST(FUniqID AS BIGINT) AS FUniqID,
                CAST(OriginalURL AS TEXT) AS OriginalURL,
                CAST(HID AS INTEGER) AS HID,
                CAST(IsOldCounter AS SMALLINT) AS IsOldCounter,
                CAST(IsEvent AS SMALLINT) AS IsEvent,
                CAST(IsParameter AS SMALLINT) AS IsParameter,
                CAST(DontCountHits AS SMALLINT) AS DontCountHits,
                CAST(WithHash AS SMALLINT) AS WithHash,
                CAST(HitColor AS CHAR) AS HitColor,
                epoch_ms(LocalEventTime * 1000) AS LocalEventTime,
                CAST(Age AS SMALLINT) AS Age,
                CAST(Sex AS SMALLINT) AS Sex,
                CAST(Income AS SMALLINT) AS Income,
                CAST(Interests AS SMALLINT) AS Interests,
                CAST(Robotness AS SMALLINT) AS Robotness,
                CAST(RemoteIP AS INTEGER) AS RemoteIP,
                CAST(WindowName AS INTEGER) AS WindowName,
                CAST(OpenerName AS INTEGER) AS OpenerName,
                CAST(HistoryLength AS SMALLINT) AS HistoryLength,
                CAST(BrowserLanguage AS TEXT) AS BrowserLanguage,
                CAST(BrowserCountry AS TEXT) AS BrowserCountry,
                CAST(SocialNetwork AS TEXT) AS SocialNetwork,
                CAST(SocialAction AS TEXT) AS SocialAction,
                CAST(HTTPError AS SMALLINT) AS HTTPError,
                CAST(SendTiming AS INTEGER) AS SendTiming,
                CAST(DNSTiming AS INTEGER) AS DNSTiming,
                CAST(ConnectTiming AS INTEGER) AS ConnectTiming,
                CAST(ResponseStartTiming AS INTEGER) AS ResponseStartTiming,
                CAST(ResponseEndTiming AS INTEGER) AS ResponseEndTiming,
                CAST(FetchTiming AS INTEGER) AS FetchTiming,
                CAST(SocialSourceNetworkID AS SMALLINT) AS SocialSourceNetworkID,
                CAST(SocialSourcePage AS TEXT) AS SocialSourcePage,
                CAST(ParamPrice AS BIGINT) AS ParamPrice,
                CAST(ParamOrderID AS TEXT) AS ParamOrderID,
                CAST(ParamCurrency AS TEXT) AS ParamCurrency,
                CAST(ParamCurrencyID AS SMALLINT) AS ParamCurrencyID,
                CAST(OpenstatServiceName AS TEXT) AS OpenstatServiceName,
                CAST(OpenstatCampaignID AS TEXT) AS OpenstatCampaignID,
                CAST(OpenstatAdID AS TEXT) AS OpenstatAdID,
                CAST(OpenstatSourceID AS TEXT) AS OpenstatSourceID,
                CAST(UTMSource AS TEXT) AS UTMSource,
                CAST(UTMMedium AS TEXT) AS UTMMedium,
                CAST(UTMCampaign AS TEXT) AS UTMCampaign,
                CAST(UTMContent AS TEXT) AS UTMContent,
                CAST(UTMTerm AS TEXT) AS UTMTerm,
                CAST(FromTag AS TEXT) AS FromTag,
                CAST(HasGCLID AS SMALLINT) AS HasGCLID,
                CAST(RefererHash AS BIGINT) AS RefererHash,
                CAST(URLHash AS BIGINT) AS URLHash,
                CAST(CLID AS INTEGER) AS CLID
            FROM read_parquet('s3://clickhouse-public-datasets/hits_compatible/athena_partitioned/hits_1.*') limit 10;
            -- COPY hits2 FROM 's3://clickhouse-public-datasets/hits_compatible/athena_partitioned/*' (FORMAT PARQUET)
            -- COPY INTO test.public.hits2 FROM 's3://clickhouse-public-datasets/hits_compatible/hits.csv.gz' FILE_FORMAT = (TYPE = CSV, COMPRESSION = GZIP, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            """)

            result = execute_query(conn, "select count(*) from hits2")
            assert result.num_rows == 10