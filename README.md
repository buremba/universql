# `UniverSQL` X-Local warehouse for your Snowflake

UniverSQL is a Snowflake proxy that allows you to run SQL queries **locally** on Snowflake Iceberg tables and joining them with your local datasets, **without any need for a running warehouse**.

Your `SELECT` queries are transpiled to DuckDB and run locally on your computer, while other queries are executed on your Snowflake account.
Once you start proxy server locally, you can connect to UniverSQL from any SQL client that supports Snowflake.

> Your Snowflake account is single source of truth and the local queries are real-only data downloaded from your cloud storage, linked with Snowflake. 
> We use your local credentials for cloud storage so [make sure you configure the cloud SDKs](#your-local-credentials-are-used-to-access-the-data-in-data-lake).
> UniverSQL doesn't support writing data to Snowflake and designed to be complementary to Snowflake.

# How it works?

* Snowflake SQL API implementation to handle the Snowflake connections, acting as a proxy between DuckDB and Snowflake.
  * You can connect UniverSQL using Snowflake Python Connector, Snowflake JDBC, ODBC or any other Snowflake client.
  * UniverSQL uses Snowflake Arrow integration to fetch the data from Snowflake and convert it to DuckDB relation.
* [SQLGlot](https://sqlglot.com) for query translation from Snowflake to DuckDB,
* [Snowflake Iceberg tables]() for the data catalog.
  * Polaris integration will be added in the future once it's released.
* Your local disk for the storage with direct access to data lakes such as [S3](), [GCS](), [Azure Blob]() for the cloud storage.
* [DuckDB](https://duckdb.org) as local compute engine.
  * The data from your data lake will be cached lazily as you query the data. 

# Use Cases

* Query local files without any need to upload them to Snowflake and join them with remote Snowflake tables, downloading the data from data lake.
* Query Snowflake Iceberg tables without any need to run a warehouse, using your local computer's resources.
* Develop end-user facing applications on top Snowflake without worrying about the costs.

### Cost

The virtual warehouse concept is great for running large queries on large datasets but usually X-Small works OK for running ad-hoc queries on small datasets that has < 2B rows.
X-Small warehouse costs $2/hour and is [likely](https://stackoverflow.com/questions/58973007/what-are-the-specifications-of-a-snowflake-server) using [m5.2xlarge](https://instances.vantage.sh/aws/ec2/m5.2xlarge).
If your query runs on X-Small warehouse, it will to run on your computer if you have 8GB - 32GB memory.

If you're the one executing the Snowflake queries and your computer has enough resources, the compute is free for you. 
You will only need to pay for the egress network costs from your cloud provider but it's going to be much cheaper than running a warehouse on Snowflake.

### Performance

UniverSQL uses DuckDB as the local compute engine, which is a columnar database that is optimized for analytical queries. 
It's on par with Snowflake in terms of performance for small and medium datasets but the 

[![DuckDB vs Snowflake](resources/clickbench.png)](https://benchmark.clickhouse.com/#eyJzeXN0ZW0iOnsiQWxsb3lEQiI6ZmFsc2UsIkF0aGVuYSAocGFydGl0aW9uZWQpIjpmYWxzZSwiQXRoZW5hIChzaW5nbGUpIjpmYWxzZSwiQXVyb3JhIGZvciBNeVNRTCI6ZmFsc2UsIkF1cm9yYSBmb3IgUG9zdGdyZVNRTCI6ZmFsc2UsIkJ5Q29uaXR5IjpmYWxzZSwiQnl0ZUhvdXNlIjpmYWxzZSwiY2hEQiAoUGFycXVldCwgcGFydGl0aW9uZWQpIjpmYWxzZSwiY2hEQiI6ZmFsc2UsIkNpdHVzIjpmYWxzZSwiQ2xpY2tIb3VzZSBDbG91ZCAoYXdzKSI6ZmFsc2UsIkNsaWNrSG91c2UgQ2xvdWQgKGF3cykgUGFyYWxsZWwgUmVwbGljYXMgT04iOmZhbHNlLCJDbGlja0hvdXNlIENsb3VkIChBenVyZSkiOmZhbHNlLCJDbGlja0hvdXNlIENsb3VkIChBenVyZSkgUGFyYWxsZWwgUmVwbGljYSBPTiI6ZmFsc2UsIkNsaWNrSG91c2UgQ2xvdWQgKEF6dXJlKSBQYXJhbGxlbCBSZXBsaWNhcyBPTiI6ZmFsc2UsIkNsaWNrSG91c2UgQ2xvdWQgKGdjcCkiOmZhbHNlLCJDbGlja0hvdXNlIENsb3VkIChnY3ApIFBhcmFsbGVsIFJlcGxpY2FzIE9OIjpmYWxzZSwiQ2xpY2tIb3VzZSAoZGF0YSBsYWtlLCBwYXJ0aXRpb25lZCkiOmZhbHNlLCJDbGlja0hvdXNlIChkYXRhIGxha2UsIHNpbmdsZSkiOmZhbHNlLCJDbGlja0hvdXNlIChQYXJxdWV0LCBwYXJ0aXRpb25lZCkiOmZhbHNlLCJDbGlja0hvdXNlIChQYXJxdWV0LCBzaW5nbGUpIjpmYWxzZSwiQ2xpY2tIb3VzZSAod2ViKSI6ZmFsc2UsIkNsaWNrSG91c2UiOmZhbHNlLCJDbGlja0hvdXNlICh0dW5lZCkiOmZhbHNlLCJDbGlja0hvdXNlICh0dW5lZCwgbWVtb3J5KSI6ZmFsc2UsIkNsb3VkYmVycnkiOmZhbHNlLCJDcmF0ZURCIjpmYWxzZSwiRGF0YWJlbmQiOmZhbHNlLCJEYXRhRnVzaW9uIChQYXJxdWV0LCBwYXJ0aXRpb25lZCkiOmZhbHNlLCJEYXRhRnVzaW9uIChQYXJxdWV0LCBzaW5nbGUpIjpmYWxzZSwiQXBhY2hlIERvcmlzIjpmYWxzZSwiRHJ1aWQiOmZhbHNlLCJEdWNrREIgKFBhcnF1ZXQsIHBhcnRpdGlvbmVkKSI6dHJ1ZSwiRHVja0RCIjp0cnVlLCJFbGFzdGljc2VhcmNoIjpmYWxzZSwiRWxhc3RpY3NlYXJjaCAodHVuZWQpIjpmYWxzZSwiR2xhcmVEQiI6ZmFsc2UsIkdyZWVucGx1bSI6ZmFsc2UsIkhlYXZ5QUkiOmZhbHNlLCJIeWRyYSI6ZmFsc2UsIkluZm9icmlnaHQiOmZhbHNlLCJLaW5ldGljYSI6ZmFsc2UsIk1hcmlhREIgQ29sdW1uU3RvcmUiOmZhbHNlLCJNYXJpYURCIjpmYWxzZSwiTW9uZXREQiI6ZmFsc2UsIk1vbmdvREIiOmZhbHNlLCJNb3RoZXJkdWNrIjpmYWxzZSwiTXlTUUwgKE15SVNBTSkiOmZhbHNlLCJNeVNRTCI6ZmFsc2UsIk94bGEiOmZhbHNlLCJQYXJhZGVEQiAoUGFycXVldCwgcGFydGl0aW9uZWQpIjpmYWxzZSwiUGFyYWRlREIgKFBhcnF1ZXQsIHNpbmdsZSkiOmZhbHNlLCJQaW5vdCI6ZmFsc2UsIlBvc3RncmVTUUwgKHR1bmVkKSI6ZmFsc2UsIlBvc3RncmVTUUwiOmZhbHNlLCJRdWVzdERCIChwYXJ0aXRpb25lZCkiOmZhbHNlLCJRdWVzdERCIjpmYWxzZSwiUmVkc2hpZnQiOmZhbHNlLCJTZWxlY3REQiI6ZmFsc2UsIlNpbmdsZVN0b3JlIjpmYWxzZSwiU25vd2ZsYWtlIjp0cnVlLCJTUUxpdGUiOmZhbHNlLCJTdGFyUm9ja3MiOmZhbHNlLCJUYWJsZXNwYWNlIjpmYWxzZSwiVGVtYm8gT0xBUCAoY29sdW1uYXIpIjpmYWxzZSwiVGltZXNjYWxlREIgKGNvbXByZXNzaW9uKSI6ZmFsc2UsIlRpbWVzY2FsZURCIjpmYWxzZSwiVW1icmEiOmZhbHNlfSwidHlwZSI6eyJDIjp0cnVlLCJjb2x1bW4tb3JpZW50ZWQiOnRydWUsIlBvc3RncmVTUUwgY29tcGF0aWJsZSI6dHJ1ZSwibWFuYWdlZCI6dHJ1ZSwiZ2NwIjp0cnVlLCJzdGF0ZWxlc3MiOnRydWUsIkphdmEiOnRydWUsIkMrKyI6dHJ1ZSwiTXlTUUwgY29tcGF0aWJsZSI6dHJ1ZSwicm93LW9yaWVudGVkIjp0cnVlLCJDbGlja0hvdXNlIGRlcml2YXRpdmUiOnRydWUsImVtYmVkZGVkIjp0cnVlLCJzZXJ2ZXJsZXNzIjp0cnVlLCJhd3MiOnRydWUsInBhcmFsbGVsIHJlcGxpY2FzIjp0cnVlLCJBenVyZSI6dHJ1ZSwiUnVzdCI6dHJ1ZSwic2VhcmNoIjp0cnVlLCJkb2N1bWVudCI6dHJ1ZSwiYW5hbHl0aWNhbCI6dHJ1ZSwic29tZXdoYXQgUG9zdGdyZVNRTCBjb21wYXRpYmxlIjp0cnVlLCJ0aW1lLXNlcmllcyI6dHJ1ZX0sIm1hY2hpbmUiOnsiMTYgdkNQVSAxMjhHQiI6dHJ1ZSwiOCB2Q1BVIDY0R0IiOnRydWUsInNlcnZlcmxlc3MiOnRydWUsIjE2YWN1Ijp0cnVlLCJjNmEuNHhsYXJnZSwgNTAwZ2IgZ3AyIjp0cnVlLCJMIjp0cnVlLCJNIjp0cnVlLCJTIjp0cnVlLCJYUyI6dHJ1ZSwiYzZhLm1ldGFsLCA1MDBnYiBncDIiOnRydWUsIjE5MkdCIjp0cnVlLCIyNEdCIjp0cnVlLCIzNjBHQiI6dHJ1ZSwiNDhHQiI6dHJ1ZSwiNzIwR0IiOnRydWUsIjk2R0IiOnRydWUsIjE0MzBHQiI6dHJ1ZSwiZGV2Ijp0cnVlLCI3MDhHQiI6dHJ1ZSwiYzVuLjR4bGFyZ2UsIDUwMGdiIGdwMiI6dHJ1ZSwiYzUuNHhsYXJnZSwgNTAwZ2IgZ3AyIjp0cnVlLCJjNmEuNHhsYXJnZSwgMTUwMGdiIGdwMiI6dHJ1ZSwiY2xvdWQiOnRydWUsImRjMi44eGxhcmdlIjp0cnVlLCJyYTMuMTZ4bGFyZ2UiOnRydWUsInJhMy40eGxhcmdlIjp0cnVlLCJyYTMueGxwbHVzIjp0cnVlLCJTMiI6dHJ1ZSwiUzI0Ijp0cnVlLCIyWEwiOnRydWUsIjNYTCI6dHJ1ZSwiNFhMIjp0cnVlLCJYTCI6dHJ1ZSwiTDEgLSAxNkNQVSAzMkdCIjp0cnVlLCJjNmEuNHhsYXJnZSwgNTAwZ2IgZ3AzIjp0cnVlfSwiY2x1c3Rlcl9zaXplIjp7IjEiOnRydWUsIjIiOnRydWUsIjQiOmZhbHNlLCI4IjpmYWxzZSwiMTYiOmZhbHNlLCIzMiI6ZmFsc2UsIjY0IjpmYWxzZSwiMTI4IjpmYWxzZSwic2VydmVybGVzcyI6dHJ1ZSwiZGVkaWNhdGVkIjp0cnVlfSwibWV0cmljIjoiY29sZCIsInF1ZXJpZXMiOlt0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWUsdHJ1ZSx0cnVlXX0=)

### Latency

Since UniverSQL runs the queries locally, the latency relies on your network bandwidth. Your local disk is used for caching the data in data lake and the cache is populated lazily as you query the data and persisted across multiple runs.
Cold starts will likely to be slower than running the query on Snowflake as the data needs to be downloaded from the data lake with UniverSQL whereas Snowflake runs the compute in the same cloud region.

Iceberg supports predicate pushdown, which helps with partitioned tables.

# Getting Started

Install UniverSQL from PyPI as follows:

```bash
pip install universql
```

You can start Universql with the passing your account identifier:

```bash
universql --account-url lt51601.europe-west2.gcp
```

```
> universql --help
Usage: universql [OPTIONS]

Options:
  --account TEXT                  The account to use (ex: rt21601.europe-
                                  west2.gcp)
  --port INTEGER                  Port for proxy server (default: 8084)
  --host TEXT                     Host for proxy server (default: 127.0.0.1)
  --compute [local|hybrid|cloud]  The compute strategy to use (default: hybrid)
  --help                          Show this message and exit.

```

## Install data lake SDKs

UniverSQL uses the native cloud SDKs to download the data from your data lake. You should install the your cloud's SDK and configure it with your credentials.

### AWS

Install AWS CLI and [configure it](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html#sso-configure-profile-token-auto-sso) based on the IAM settings in your organization. 
If you would like to use AWS client id / secret, you can use `aws configure` to set them up.

By default, UniverSQL uses your default AWS profile, you can pass `--aws_profile` option to `universql` to use a different profile than the default profile.

#### Google Cloud

[Install](https://cloud.google.com/sdk/docs/initializing) and [configure](https://cloud.google.com/sdk/docs/authorizing) Google Cloud SDK.

By default, UniverSQL uses your default GCP account attached to `gcloud`, you can pass `--gcp_account` option to `universql` to use a different profile than the default account.

#### Azure

Install [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)) and [configure it](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli-interactively).

By default, UniverSQL uses [your default Azure tenant](https://learn.microsoft.com/en-us/cli/azure/manage-azure-subscriptions-azure-cli?tabs=bash#change-the-active-tenant) attached to `az`, you can pass `--azure_tenant` option to `universql` to use a different profile than the default account.

## Compute Strategies

`hybrid`: Runs the queries locally if they're `SELECT` queries and can be transpiled into DuckDB query. Otherwise runs queries on Snowflake. This is the default strategy.

`local`: If the query requires a running warehouse on Snowflake, fails the query. Otherwise runs the query locally.

`cloud`: Runs the queries directly on Snowflake, use it as a passthrough.

By default, UniverSQL uses `hybrid` compute strategy, which runs the queries locally if they're `SELECT` queries and runs them on Snowflake if they're not.

# Limitations

## Self signed certificates are not supported in Snowflake SQL V1 API

Snowflake V1 API requires valid CA certificate, which is [not possible with self-signed certificates](https://letsencrypt.org/docs/certificates-for-localhost/). 

If you don't need to expose UniverSQL to public internet with a public tunnel service, UniverSQL ships SSL certificate of [http://localhost.universql.com]() domain in the binary, which has [DNS record to 127.0.0.1](https://mxtoolbox.com/SuperTool.aspx?action=a%3alocalhost.universql.com&run=toolpage). 
It gives you free https connection to your local server and it's the default host. 

> Your data doesn't go through an external server with this approach as the DNS resolves to your localhost.

## Can't query all Snowflake types

Here is a Markdown table of some Snowflake data types with a "Supported" column. The checkbox indicates whether the type is supported or not. Please replace the checkboxes with the correct values according to your project's support for each data type.

| Snowflake Data Type | Supported                      |
| --- |--------------------------------|
| NUMBER | &check;                        |
| DECIMAL | &check;                        |
| INT | &check;                        |
| BIGINT | &check;                        |
| SMALLINT | &check;                        |
| TINYINT | &check;                        |
| FLOAT | &check;                        |
| DOUBLE | &check;                        |
| VARCHAR | &check;                        |
| CHAR | &check;                        |
| STRING | &check;                        |
| TEXT | &check;                        |
| BOOLEAN | &check;                        |
| DATE | &check;                        |
| DATETIME | &check;                        |
| TIME | &check;                        |
| TIMESTAMP | &check;                        |
| TIMESTAMP_LTZ | &cross; ¹                       |
| TIMESTAMP_NTZ | &cross; ¹ |
| TIMESTAMP_TZ | &cross;¹                       |
| VARIANT | &check;                     |
| OBJECT | &check;                          |
| ARRAY | &check;                           |
| GEOGRAPHY | &cross; ¹                         |
| VECTOR | &cross; ¹                          |

¹: No Support in DuckDB yet.

## Can't query native Snowflake tables

UniverSQL doesn't support querying native Snowflake tables as they're not accessible from outside of Snowflake. If you try to query a Snowflake table directly, it will return an error.

 ```sql
    SELECT * FROM my_snowflake_table;
```

You have two alternatives:

1. Create a dynamic iceberg table replicating from your native Snowflake table. This approach requires warehouse but the usage will be minimum as dynamic tables are serverless, with the caveat to have some lag provided in `TARGET_LAG` option. 

 ```sql
 CREATE DYNAMIC ICEBERG TABLE my_iceberg_snowflake_table 
   TARGET_LAG = '1 hour'  WAREHOUSE = 'compute_xs'
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'your_data_lake_volume'
    BASE_LOCATION = 'my_transformed_table'
    REFRESH_MODE = auto
    INITIALIZE = on_create
 AS SELECT * FROM my_snowflake_table;
```
Dynamic tables is the recommended approach **if your natives tables have more than 2B+ of rows** so that you can filter / aggregate them before pulling them into your local environment. If your native tables are small enough, consider switching them to use Iceberg from Native.

2. You can use `universql.execute` function to run queries directly in Snowflake and return the result as a table. You can join native Snowflake table with your local files as follows: 

```sql
SELECT * FROM table(universql.execute('select col1 from my_snowflake_table', {'target_lag': '1h'})) t1 join 'local://./my_local_table.csv' as t2 on t1.col1 = t2.col1;
```

UniverSQL doesn't actually require you to create the `universql.execute` function in your Snowflake database. When you use the proxy server, it will create a query plan to execute your Snowflake query first and map the result as an Arrow table in DuckDB. 
This approach is recommended for hybrid execution where you need to query your native Snowflake tables on the fly. UniverSQL has query caching based on the setting `target_lag`, which is saved locally.

## Only read-only `SELECT` queries can use local warehouse.

Since UniverSQL uses SQLGlot for parsing Snowflake queries and it supports most of the Snowflake syntax. 
* In the cases where we can't parse the query, we can't run the query locally. If you run into such case, please open an issue with the query. You can use `--passthrough` option when starting the proxy server to run the query in Snowflake if it can't be parsed. That way you can make sure your client applications don't break.
* Anything except `SELECT` query is directly executed in your target Snowflake account. That way, your changes (including `CREATE TABLE`) are visible to all other Snowflake users.

## You need a tunnel service to connect from external tools
If your local computer is not accessible from public network, (i.e. no external IP) external tools such as notebooks (Hex, Google Colab etc.) and BI tools (Tableau Online, Looker, Mode etc.) can't connect UniverSQL. 
The workaround is to use a public tunnel service to expose your local server to the internet. Here are some options:

* [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/do-more-with-tunnels/trycloudflare/) (recommended)
* [ngrok](https://ngrok.com/)

## No support for Snowflake SQL V2 API yet

While SQL V1 API is internal, most Snowflake clients are using SQL V1 API, including JDBC, Python, ODBC etc. Feel free to help supporting [SQL V2 API](https://docs.snowflake.com/en/developer-guide/sql-api/intro) by contributing to the project. It should be easy enough as we already use Arrow interface for the V1 API, which is the interface for V2.

