import base64
import functools
import logging
import os
import shutil
import socket
import sys
import tempfile
import threading
import typing
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import click
import duckdb
from fastapi import FastAPI

from universql.agent.cloudflared import start_cloudflared
from universql.protocol.pg_ruff import PostgresqlServer
from universql.protocol.session import UniverSQLSession
from universql.util import LOCALHOST_UNIVERSQL_COM_BYTES, Catalog, LOCALHOSTCOMPUTING_COM, \
    DEFAULTS, initialize_context, detect_catalog_type
from universql.plugin import register

logger = logging.getLogger("🏠")


def resolve_host_to_ipv4(hostname):
    """Resolves a hostname to its IPv4 address, if available."""
    try:
        # Use AF_INET to specifically request IPv4 addresses
        addrinfo = socket.getaddrinfo(hostname, None, socket.AF_INET)
        # Return the first IPv4 address found
        if addrinfo:
            return addrinfo[0][4][0]  # The IP address is at index 4, item 0
    except socket.gaierror:
        logger.warning(f"Could not resolve {hostname} to an IPv4 address.", exc_info=True)
    except Exception: # Catch potential other errors during resolution
        logger.warning(f"An unexpected error occurred while resolving {hostname} to IPv4.", exc_info=True)
    return None


def common_options(func):
    """Common click options decorator for both postgresql and snowflake commands"""
    @click.option('--host', help='Host for the proxy server',
                  default=LOCALHOSTCOMPUTING_COM,
                  envvar='SERVER_HOST',
                  type=str)
    @click.option('--motherduck-token', type=str,
                  help='Motherduck token to enable', envvar='MOTHERDUCK_TOKEN')
    @click.option('--tunnel', type=click.Choice(["cloudflared", "ngrok"]),
                  help='Use tunnel for accessing server from public internet', envvar='TUNNEL')
    @click.option('--database-path', type=click.Path(exists=False, writable=True),
                  help='Optional DuckDB Path. (default: :memory:) For persistent storage, provide a path similar to `~/.universql/$session_id.duckdb`',
                  envvar='DATABASE_PATH')
    @click.option('--max-cache-size', type=str, default=DEFAULTS["max_cache_size"],
                  help='DuckDB maximum cache used in local disk (default: 80% of total available disk)',
                  envvar='CACHE_PERCENTAGE')
    @click.option('--home-directory',
                  help=f'Home directory for local operations (default: {Path.home()})',
                  default=Path.home(),
                  envvar='HOME', type=str)
    @click.option('--cache-directory',
                  help=f'Data lake cache directory (default: {Path.home() / ".universql" / "cache"})',
                  default=Path.home() / ".universql" / "cache",
                  envvar='CACHE_DIRECTORY', type=str)
    @click.option('--max-memory', type=str, default=DEFAULTS["max_memory"],
                  help='DuckDB Max memory to use for the server (default: 80% of total memory)',
                  envvar='MAX_MEMORY')
    @click.option('--ssl_certfile', help='SSL certfile for the proxy server, optional. ', type=str)
    @click.option('--ssl_keyfile',
                  help='SSL keyfile for the proxy server, optional. Use it if you don\'t want to use localhostcomputing.com',
                  type=str)
    @click.option('--gcp-project',
                  help='GCP project to access GCS and apply quota. (to see how to setup auth for GCP and use different accounts, visit https://cloud.google.com/docs/authentication/application-default-credentials)',
                  type=str)
    @click.option('--aws-profile', help='AWS profile to access S3 (default: `default`)', type=str)
    @click.option('--universql-catalog', type=str,
                  help='The external catalog that will be used for Iceberg tables. (default: duckdb:///:memory:)',
                  envvar='UNIVERSQL_CATALOG')
    @click.option('--metrics-port', help='Grafana metrics port, only available for cloudflared host (default: 5675)',
                  default=5675, envvar='METRICS_PORT', type=str)
    @click.option('--file', type=click.Path(exists=True),
                  help='Path to the SQL file to execute and exit.')
    @click.option('--auth-file', type=click.Path(exists=True),
                  help='Path to auth file with user:password pairs')
    @click.option('--frontend-dir', type=click.Path(exists=True),
                  help='Path to the Vite frontend build directory (e.g., /path/to/dist)',
                  envvar='FRONTEND_DIR')
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


@click.group(context_settings={'max_content_width': shutil.get_terminal_size().columns - 10})
@click.version_option(version="0.1")
def cli():
    pass

@cli.command(
    epilog='[BETA] Check out docs at https://github.com/buremba/universql and let me know if you have any cool use-case on Github!')
@click.option('--url',
              help='The Postgresql server to proxy to',
               envvar='DATABASE_URL', required=False)
@click.option('--port', help='Port for PostgreSQL proxy server (default: 5432)', default=5432, envvar='POSTGRESQL_PORT')
@click.option('--api-port', help='Port for FastAPI and WebSocket servers (default: 8084)', default=8084, envvar='API_PORT')
@common_options
def postgresql(url, port, api_port, host, ssl_keyfile, ssl_certfile, tunnel, frontend_dir, **kwargs):
    logger.info(f"UniverSQL is starting reverse proxy for Postgresql server on {url}")

    # Initialize context first
    initialize_context()

    if url is not None:
        parsed_url = urlparse(url)
    
        url_credentials = {
            'scheme': parsed_url.scheme,
            'host': parsed_url.hostname,
            'port': parsed_url.port or 5432,
            'database': parsed_url.path.lstrip('/') if parsed_url.path else None,
            'user': parsed_url.username,
            'password': parsed_url.password,
        }
        
        # Parse query parameters (like sslmode)
        if parsed_url.query:
            query_params = parse_qs(parsed_url.query)
            for key, value in query_params.items():
                url_credentials[key] = value[0] if len(value) == 1 else value
        
        # Store the parsed URL in context for the protocol handler
        from universql.util import current_context
        current_context['url'] = url_credentials
    
    # Store frontend directory in context for UI plugin
    from universql.util import current_context
    if frontend_dir:
        current_context['frontend_dir'] = str(frontend_dir)
        logger.info(f"📁 Frontend directory specified: {frontend_dir}")
    
    bind_host = host

    if host == LOCALHOSTCOMPUTING_COM:
        # DNS resolution check similar to Snowflake implementation
        try:
            addr_list = socket.getaddrinfo(LOCALHOSTCOMPUTING_COM, None)
            resolved_ips = [info[4][0] for info in addr_list]
            logger.info(f"DNS for {LOCALHOSTCOMPUTING_COM} resolves to: {resolved_ips}")
            if "127.0.0.1" not in resolved_ips:
                 logger.error(
                     f"The DNS setting for {LOCALHOSTCOMPUTING_COM} doesn't include 127.0.0.1, refusing to start. Please update UniverSQL or DNS settings.")
            else:
                 # Attempt to force IPv4 binding
                 ipv4_address = resolve_host_to_ipv4(LOCALHOSTCOMPUTING_COM)
                 if ipv4_address:
                     logger.info(f"Attempting to bind specifically to IPv4 address: {ipv4_address}")
                     bind_host = ipv4_address
                 else:
                     logger.warning(f"Could not explicitly resolve {LOCALHOSTCOMPUTING_COM} to IPv4. Falling back to hostname binding.")

        except socket.gaierror as e:
            logger.warning(f"Unable to resolve DNS for {LOCALHOSTCOMPUTING_COM}. Check internet connection or DNS settings. Error: {e}")

    if tunnel == 'cloudflared':
        start_cloudflared(port, kwargs.get('metrics_port', 5675))
        if os.getenv('USE_LOCALCOMPUTING_COM') == '1':
            bind_host = "0.0.0.0"
        else:
            bind_host = '127.0.0.1'
            logger.info("Cloudflared tunnel enabled, binding server to 127.0.0.1")
    elif tunnel == 'ngrok':
        logger.error("Ngrok is not supported yet. Please use cloudflared.")
        sys.exit(1)

    # Check for cert-manager provided certificates
    cert_manager_cert = os.getenv('SSL_CERT_PATH')
    cert_manager_key = os.getenv('SSL_KEY_PATH')
    
    # Use cert-manager certs if available and files exist
    if cert_manager_cert and cert_manager_key and os.path.exists(cert_manager_cert) and os.path.exists(cert_manager_key):
        ssl_certfile = cert_manager_cert
        ssl_keyfile = cert_manager_key
        logger.info(f"🔒 Using cert-manager provided certificates: {ssl_certfile}")

    # Determine if SSL should be used based on host, explicit files, or env var
    use_ssl = (host == LOCALHOSTCOMPUTING_COM or ssl_certfile is not None or os.getenv('USE_LOCALCOMPUTING_COM') == '1') and tunnel != 'cloudflared'

    if use_ssl:
        logger.info(f"Attempting to start PostgreSQL server with SSL on {bind_host}:{port}")
        with tempfile.NamedTemporaryFile(suffix='cert.pem', delete=False) as cert_file:
            cert_file.write(base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['cert']))
            cert_file.flush()
            with tempfile.NamedTemporaryFile(suffix='key.pem', delete=False) as key_file:
                key_file.write(base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['key']))
                key_file.flush()

                try:
                    server = PostgresqlServer(
                        bind_host, port, api_port, 
                        ssl_certfile=ssl_certfile or cert_file.name,
                        ssl_keyfile=ssl_keyfile or key_file.name
                    )
                    # Store connection info in server app state for API endpoints
                    server.app.state.host = bind_host
                    server.app.state.port = port
                    server.app.state.api_port = api_port
                    server.app.state.ssl_enabled = True
                    
                    logger.info(f"PostgreSQL proxy listening on {bind_host}:{port} with SSL")
                    logger.info(f"FastAPI server (MCP) will be available on {bind_host}:{api_port}")
                    logger.info("Starting UniverSQL PostgreSQL server with SSL and integrated MCP support")
                    
                    import asyncio
                    asyncio.run(server.start())
                except Exception as e:
                    logger.critical(f"Unable to start the SSL server on {bind_host}:{port}", exc_info=e)
                    if isinstance(e, socket.error) and "address already in use" in str(e).lower():
                        logger.error(f"Port {port} is already in use on {bind_host}. Check for other running instances or services.")
                    elif isinstance(e, socket.error) and "cannot assign requested address" in str(e).lower():
                         logger.error(f"Cannot assign requested address {bind_host}. Check network configuration and if the address is valid for this host.")
                    raise e
                finally:
                    # Clean up temp files
                    try:
                        os.unlink(cert_file.name)
                        os.unlink(key_file.name)
                    except:
                        pass
    else:
        logger.info(f"Attempting to start PostgreSQL server without SSL on {bind_host}:{port}")
        try:
            server = PostgresqlServer(bind_host, port, api_port)
            # Store connection info in server app state for API endpoints
            server.app.state.host = bind_host
            server.app.state.port = port
            server.app.state.api_port = api_port
            server.app.state.ssl_enabled = False
            
            logger.info(f"PostgreSQL proxy listening on {bind_host}:{port}")
            logger.info(f"FastAPI server (MCP) will be available on {bind_host}:{api_port}")
            logger.info("Starting UniverSQL PostgreSQL server with integrated MCP support")
            
            import asyncio
            asyncio.run(server.start())
        except Exception as e:
            logger.critical(f"Unable to start the non-SSL server on {bind_host}:{port}", exc_info=e)
            if isinstance(e, socket.error) and "address already in use" in str(e).lower():
                logger.error(f"Port {port} is already in use on {bind_host}. Check for other running instances or services.")
            elif isinstance(e, socket.error) and "cannot assign requested address" in str(e).lower():
                 logger.error(f"Cannot assign requested address {bind_host}. Check network configuration and if the address is valid for this host.")
            raise e


@cli.command(
    epilog='[BETA] Check out docs at https://github.com/buremba/universql and let me know if you have any cool use-case on Github!')
@click.option('--account',
              help='The account to use. Supports both Snowflake and Polaris (ex: rt21601.europe-west2.gcp)',
              prompt='Account ID (example: rt21601.europe-west2.gcp)', envvar='SNOWFLAKE_ACCOUNT')
@click.option('--port', help='Port for Snowflake proxy server (default: 8084)', default=8084, envvar='SERVER_PORT')
@click.option('--catalog', type=click.Choice([e.value for e in Catalog]),
              help='Type of the Snowflake account. Automatically detected from the account if not provided.')
@common_options
def snowflake(file, host, port, ssl_keyfile, ssl_certfile, account, catalog, metrics_port, tunnel, frontend_dir, **kwargs):
    context__params = click.get_current_context().params
    auto_catalog_mode = catalog is None
    context__params["catalog"] = detect_catalog_type(account, catalog)

    adjective = "apparently" if auto_catalog_mode else ""
    logger.info(f"UniverSQL is starting reverse proxy for {account}.snowflakecomputing.com, "
                f"it's {adjective} a {context__params['catalog']} server.")

    bind_host = host # Start with the provided host

    initialize_context()
    
    # Store frontend directory in context for UI plugin
    from universql.util import current_context
    if frontend_dir:
        current_context['frontend_dir'] = str(frontend_dir)
        logger.info(f"📁 Frontend directory specified: {frontend_dir}")

    if host == LOCALHOSTCOMPUTING_COM:
        # Existing DNS resolution check (informational)
        try:
            # Use getaddrinfo for a more comprehensive check, including IPv4/IPv6
            addr_list = socket.getaddrinfo(LOCALHOSTCOMPUTING_COM, None)
            resolved_ips = [info[4][0] for info in addr_list]
            logger.info(f"DNS for {LOCALHOSTCOMPUTING_COM} resolves to: {resolved_ips}")
            if "127.0.0.1" not in resolved_ips:
                 logger.error(
                     f"The DNS setting for {LOCALHOSTCOMPUTING_COM} doesn't include 127.0.0.1, refusing to start. Please update UniverSQL or DNS settings.")
                 # Decide if you want to exit or just warn
                 # sys.exit(1) # Uncomment if this should be fatal
            else:
                 # Attempt to force IPv4 binding
                 ipv4_address = resolve_host_to_ipv4(LOCALHOSTCOMPUTING_COM)
                 if ipv4_address:
                     logger.info(f"Attempting to bind specifically to IPv4 address: {ipv4_address}")
                     bind_host = ipv4_address
                 else:
                     logger.warning(f"Could not explicitly resolve {LOCALHOSTCOMPUTING_COM} to IPv4. Falling back to hostname binding.")
                     # Keep bind_host as LOCALHOSTCOMPUTING_COM

        except socket.gaierror as e:
            logger.warning(f"Unable to resolve DNS for {LOCALHOSTCOMPUTING_COM}. Check internet connection or DNS settings. Error: {e}")
            # Decide how to proceed - maybe fall back to 127.0.0.1 or exit?
            # bind_host = '127.0.0.1' # Option: Fallback safely
            # sys.exit(1)           # Option: Exit if resolution fails

    if tunnel == 'cloudflared':
        start_cloudflared(port, metrics_port)
        if os.getenv('USE_LOCALCOMPUTING_COM') == '1':
            # If using cloudflared tunnel AND specifically forcing localcomputing.com via env var (unusual case),
            # bind to all interfaces for the tunnel to connect.
             bind_host = "0.0.0.0"
        else:
            # Default cloudflared behavior usually involves binding to localhost
             bind_host = '127.0.0.1'
             logger.info("Cloudflared tunnel enabled, binding server to 127.0.0.1")

    elif tunnel == 'ngrok':
        logger.error("Ngrok is not supported yet. Please use cloudflared.")
        sys.exit(1)

    # Determine if SSL should be used based on host, explicit files, or env var
    use_ssl = (host == LOCALHOSTCOMPUTING_COM or ssl_certfile is not None or os.getenv('USE_LOCALCOMPUTING_COM') == '1') and tunnel != 'cloudflared'

    if file is not None:

        # Read the SQL file
        with open(file, 'r') as f:
            sql = f.read()

        # Create a session and execute the query
        session = UniverSQLSession(
            context=context__params,
            base_engine="snowflake",
            session_id="file-run",
            credentials={"account": account},
            session_parameters={}
        )

        try:
            result = session.do_query(sql)
            # Print the results
            print(result.to_pandas())
        finally:
            session.close()
            sys.exit(0)

    if use_ssl:
        logger.info(f"Attempting to start server with SSL on {bind_host}:{port}")
        with tempfile.NamedTemporaryFile(suffix='cert.pem', delete=True) as cert_file:
            cert_file.write(base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['cert']))
            cert_file.flush()
            with tempfile.NamedTemporaryFile(suffix='key.pem', delete=True) as key_file:
                key_file.write(base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['key']))
                key_file.flush()

                try:
                    import uvicorn
                    uvicorn.run("universql.protocol.snowflake:app",
                                host=bind_host, # Use the potentially resolved IPv4 address
                                port=port,
                                ssl_keyfile=ssl_keyfile or key_file.name,
                                ssl_certfile=ssl_certfile or cert_file.name,
                                reload=False,
                                use_colors=True)
                except Exception as e: # Catch specific exceptions if possible (e.g., socket.error)
                    logger.critical(f"Unable to start the SSL server on {bind_host}:{port}", exc_info=e)
                    # Provide more specific advice if possible based on the error (e.g., address in use)
                    if isinstance(e, socket.error) and "address already in use" in str(e).lower():
                        logger.error(f"Port {port} is already in use on {bind_host}. Check for other running instances or services.")
                    elif isinstance(e, socket.error) and "cannot assign requested address" in str(e).lower():
                         logger.error(f"Cannot assign requested address {bind_host}. Check network configuration and if the address is valid for this host.")
                    raise e # Re-raise the exception after logging
    else:
        logger.info(f"Attempting to start server without SSL on {bind_host}:{port}")
        try:
            import uvicorn
            uvicorn.run("universql.protocol.snowflake:app",
                        host=bind_host, # Use the potentially resolved IPv4 address
                        port=port,
                        reload=False,
                        use_colors=True)
        except Exception as e: # Catch specific exceptions if possible
             logger.critical(f"Unable to start the non-SSL server on {bind_host}:{port}", exc_info=e)
             if isinstance(e, socket.error) and "address already in use" in str(e).lower():
                 logger.error(f"Port {port} is already in use on {bind_host}. Check for other running instances or services.")
             elif isinstance(e, socket.error) and "cannot assign requested address" in str(e).lower():
                  logger.error(f"Cannot assign requested address {bind_host}. Check network configuration and if the address is valid for this host.")
             raise e # Re-raise the exception after logging


class EndpointFilter(logging.Filter):
    def __init__(
            self,
            path: str,
            *args: typing.Any,
            **kwargs: typing.Any,
    ):
        super().__init__(*args, **kwargs)
        self._path = path

    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage().find(self._path) == -1



uvicorn_logger = logging.getLogger("uvicorn.access")
uvicorn_logger.addFilter(EndpointFilter(path="/telemetry/send"))
uvicorn_logger.addFilter(EndpointFilter(path="/queries/v1/query-request"))
uvicorn_logger.addFilter(EndpointFilter(path="/session"))

# @register()
def install_mcp_server(app: FastAPI):
    """Install MCP server endpoints on the FastAPI app"""
    from universql.util import current_context
    from fastmcp import FastMCP

    # Create MCP server instance
    mcp = FastMCP(
        name="UniverSQL-PostgreSQL",
        instructions="""
            This server provides PostgreSQL data analysis and SQL execution tools through UniverSQL.
            You can execute SQL queries, analyze data, and perform database operations on the connected PostgreSQL database.
        """,
        tags=["postgresql", "sql", "data_analysis"],
        version="1.0.0",
        auth=None
    )

    @mcp.tool
    def execute_sql(query: str) -> dict:
        """Executes a SQL query on the connected PostgreSQL database."""
        try:
            # Get URL credentials from context
            url_credentials = current_context.get('url', {})
            session = UniverSQLSession(
                context=current_context,
                base_engine="postgresql",
                session_id="mcp-session",
                credentials=url_credentials,
                session_parameters={}
            )
            result = session.do_query(query)
            return {
                "success": True,
                "data": result.to_pandas().to_dict('records') if hasattr(result, 'to_pandas') else str(result),
                "query": query
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "query": query
            }
    
    @mcp.tool
    def get_table_schema(table_name: str) -> dict:
        """Gets the schema information for a specific table."""
        schema_query = f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        return execute_sql(schema_query)
    
    @mcp.tool
    def list_tables() -> dict:
        """Lists all tables in the current database."""
        tables_query = """
        SELECT table_name, table_type 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name;
        """
        return execute_sql(tables_query)
    
    @mcp.resource("postgresql://connection")
    def get_connection_info() -> dict:
        """Provides the PostgreSQL connection configuration."""
        url_credentials = current_context.get('url', {})
        return {
            "host": url_credentials.get('host'),
            "port": url_credentials.get('port'),
            "database": url_credentials.get('database'),
            "user": url_credentials.get('user'),
        }
    
    @mcp.prompt
    def analyze_table_data(table_name: str, analysis_type: str = "summary") -> str:
        """Creates a prompt for analyzing data in a specific table."""
        return f"""Please analyze the '{table_name}' table in the PostgreSQL database.
        Analysis type: {analysis_type}
        
        Steps to follow:
        1. First, get the table schema using get_table_schema('{table_name}')
        2. Then, execute appropriate SQL queries to analyze the data
        3. Provide insights based on the analysis type: {analysis_type}
        
        Available analysis types: summary, distribution, trends, anomalies"""
    
    # Add health check endpoint
    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "service": "universql-postgresql"}
    
    # Add database info endpoint  
    @app.get("/db/info")
    async def database_info():
        url_credentials = current_context.get('url', {})
        return {
            "database_type": "postgresql",
            "proxy_host": getattr(app.state, 'host', 'unknown'),
            "proxy_port": getattr(app.state, 'port', 'unknown'),
            "fastapi_port": getattr(app.state, 'api_port', 'unknown'),
            "target": url_credentials.get('host', 'unknown')
        }
    
    # Mount MCP HTTP app as sub-application
    app.mount("/mcp", mcp.http_app())
    
    return app

if __name__ == '__main__':
    cli(prog_name="universql")
