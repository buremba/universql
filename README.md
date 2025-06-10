# Safely connect AI Agents to your data

Spile is similar to Supabase and Neon but for your existing datasets and databases. It gives you branching, time-travel and approval 

[![Demo](./resources/cli_demo.png)](https://www.youtube.com/watch?v=s1fpSEE-pAc)

**[Watch on YouTube](https://www.youtube.com/watch?v=s1fpSEE-pAc)**

[![Launch GitHub Codespace](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?hide_repo_select=true&ref=main&repo=buremba/universql)

When you query an Iceberg table on Snowflake for the first time, UniverSQL looks up Iceberg metadata from Snowflake, (metadata operation, no compute cost) re-writes the query for DuckDB dialect, 
sets up [filesystem](https://duckdb.org/docs/guides/python/filesystems.html) that connects to your data lake with [your cloud credentials](#install-data-lake-sdks) and caches the Parquet files and executes the query on DuckDB.

# Getting Started

## Access to Storage

### Snowflake

Since Snowflake doesn't provide direct access to data lake, UniverSQL uses your local credentials for cloud storage so [make sure you configure the cloud SDKs](#install-data-lake-sdks).
You should install the your cloud's SDK and configure it with your credentials.

### Polaris

Polaris Catalog is a managed Iceberg table catalog that is available in Snowflake.
It manages access credentials to data lake and the metadata of the Iceberg tables.
If your Snowflake account (`snowflake --account`) is a Polaris Catalog, UniverSQL will use PyIceberg to fetch data from your data lake and map them as Arrow tables in DuckDB.

###### AWS

[Install](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and [configure](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html#sso-configure-profile-token-auto-sso) AWS CLI.
If you would like to use AWS client id / secret, you can use `aws configure` to set them up.
By default, UniverSQL uses your default AWS profile, you can pass `--aws-profile` option to `universql` to use a different profile than the default profile.

###### Google Cloud

[Install](https://cloud.google.com/sdk/docs/initializing) and [configure](https://cloud.google.com/sdk/docs/authorizing) Google Cloud SDK. You can use `gcloud auth application-default login` to login with your Google Cloud account.
By default, UniverSQL uses your default GCP account attached to `gcloud`, you can pass `--gcp-account` option to `universql` to use a different profile than the default account.

###### Azure

[Install](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) and [configure](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli-interactively) Azure CLI.
By default, UniverSQL uses [your default Azure tenant](https://learn.microsoft.com/en-us/cli/azure/manage-azure-subscriptions-azure-cli?tabs=bash#change-the-active-tenant) attached to `az`, you can pass `--azure-tenant` option to `universql` to use a different profile than the default account.


### Python

```bash
python3 -m pip install universql
```

#### Using virtual environments

We recommend using virtual environments (venv) to namespace pip modules. Create a new venv as follows:

```bash
python -m venv universql-env
```

Activate that same virtual environment each time you create a shell window or session:

```
source universql-env/bin/activate         # activate the environment for Mac and Linux OR
universql-env\Scripts\activate            # activate the environment for Windows
```

```bash
universql 
    snowflake --account lt51601.europe-west2.gcp
```

### Docker

Alternatively, pull the Docker image: (recommended for running in background)

```bash
docker run -p 8084:8084 -v ~/.universql:/root/.universql buremba/universql snowflake --account lt51601.europe-west2.gcp
```

### Docker Compose

Make a copy of the `.env.example` file and update the values.

**Note:** The SSL certificates are not included in the repository, and are optional. If you want to use SSL, you need to generate your own and place them in the `ssl` directory then update the `.env` file with the correct names.

```bash
cp .env.example .env
```

```bash
docker compose up
```