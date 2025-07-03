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

## PostgreSQL Protocol Support

UniverSQL provides a PostgreSQL wire protocol proxy that allows you to connect with any PostgreSQL client.

### Basic Usage

```bash
# Start PostgreSQL proxy (non-SSL)
universql postgresql --url postgresql://user:password@host:5432/database --port 5436

# Start with SSL using localhostcomputing.com certificates
universql postgresql --url postgresql://user:password@host:5432/database --host localhostcomputing.com --port 5436

# Connect with any PostgreSQL client
psql "host=localhostcomputing.com port=5436 dbname=database user=user password=password sslmode=require"
```

### SSL Support

UniverSQL PostgreSQL proxy supports SSL encryption using built-in `localhostcomputing.com` certificates:

- **Automatic SSL**: When using `--host localhostcomputing.com`, SSL is automatically enabled
- **Custom Certificates**: Use `--ssl_certfile` and `--ssl_keyfile` for custom certificates  
- **Client Compatibility**: Works with any PostgreSQL client that supports SSL (psql, DBeaver, etc.)
- **Protocol Compliance**: Proper SSL upgrade negotiation following PostgreSQL wire protocol

#### SSL Examples

```bash
# Automatic SSL with localhostcomputing.com
universql postgresql --host localhostcomputing.com --port 5436

# Custom SSL certificates
universql postgresql --ssl_certfile /path/to/cert.pem --ssl_keyfile /path/to/key.pem --port 5436

# Connect with SSL required
psql "host=localhostcomputing.com port=5436 dbname=database sslmode=require"
```

#### DBeaver SSL Configuration

When connecting with DBeaver:

1. **Main Tab**: Set host to `localhostcomputing.com` (or `127.0.0.1` if using custom certs)
2. **SSL Tab**: Check "Use SSL" 
3. **SSL Tab**: All SSL parameters are optional - the built-in certificates are used automatically
4. **SSL Mode**: Can be set to "require" for secure connections

The server will automatically handle SSL upgrade when clients request it, responding with 'S' for SSL-capable servers or 'N' for non-SSL servers.

### Features

- **Full PostgreSQL Protocol**: Support for prepared statements, transactions, and authentication
- **WebSocket Integration**: Queries can be routed through WebSocket connections  
- **FastAPI Integration**: Built-in REST API and MCP (Model Context Protocol) support
- **SSL/TLS Encryption**: Secure connections using localhostcomputing.com certificates
- **JDBC Support**: Full compatibility with JDBC clients like DBeaver, IntelliJ IDEA, and enterprise applications

#### JDBC Integration

UniverSQL supports all JDBC-based PostgreSQL clients through the [jaydebeapi](https://github.com/baztian/jaydebeapi) integration:

**Prerequisites:** Java 8+ is **required** for JDBC testing and development. Tests will **fail** if Java is not available.

```bash
# Install Java (macOS)
brew install openjdk@17

# Install Java (Ubuntu/Debian)
sudo apt-get install openjdk-17-jdk

# Install Java (CentOS/RHEL)
sudo yum install java-17-openjdk-devel
```

```python
import jaydebeapi

# Non-SSL JDBC connection
conn = jaydebeapi.connect(
    "org.postgresql.Driver",
    "jdbc:postgresql://127.0.0.1:5436/postgres",
    {"user": "username", "password": "password"},
    "/path/to/postgresql-42.7.7.jar"
)

# SSL JDBC connection
conn = jaydebeapi.connect(
    "org.postgresql.Driver", 
    "jdbc:postgresql://localhostcomputing.com:5436/postgres?ssl=true&sslmode=require",
    {"user": "username", "password": "password"},
    "/path/to/postgresql-42.7.7.jar"
)
```

**JDBC Driver Management:**
- **Latest Version**: PostgreSQL JDBC driver v42.7.7 (automatically downloaded)
- **Source**: Maven Central (not PostgreSQL.org)
- **Caching**: Drivers cached in `.cache/jdbc_drivers/` for reuse
- **Auto-Download**: First test run downloads and caches the driver

**Supported JDBC Clients:**
- DBeaver Community & Enterprise
- IntelliJ IDEA Database Tools
- Eclipse Data Tools Platform
- SQuirreL SQL Client
- Any application using PostgreSQL JDBC driver

#### Development & Testing

```bash
# Setup development environment with JDBC testing
make setup

# Check if Java is available
make check-java

# Show JDBC driver cache information
make cache-info

# Run all PostgreSQL tests (auto-detects Java)
make test-all-postgresql

# Test JDBC integration specifically (requires Java - will FAIL if missing)
make test-jdbc
```

**Cache Management:**
```bash
# Clean JDBC cache only
make clean-jdbc

# Clean all test artifacts and cache
make clean
```

**Note:** JDBC tests now **fail** (not skip) if Java is missing, ensuring proper CI/CD pipeline behavior.