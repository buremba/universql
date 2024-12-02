import base64
import logging
import os
import shutil
import socket
import sys
import tempfile
import typing
from pathlib import Path

import click
import requests
import uvicorn
from requests import RequestException

from universql.agent.cloudflared import start_cloudflared
from universql.util import LOCALHOST_UNIVERSQL_COM_BYTES, Catalog, LOCALHOSTCOMPUTING_COM, \
    DEFAULTS

logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)s %(name)s %(levelname)-6s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("🏠")


@click.group(context_settings={'max_content_width': shutil.get_terminal_size().columns - 10})
@click.version_option(version="0.1")
def cli():
    pass


@cli.command(
    epilog='[BETA] Check out docs at https://github.com/buremba/universql and let me know if you have any cool use-case on Github!')
@click.option('--account',
              help='The account to use. Supports both Snowflake and Polaris (ex: rt21601.europe-west2.gcp)',
              prompt='Account ID', envvar='SNOWFLAKE_ACCOUNT')
@click.option('--port', help='Port for Snowflake proxy server (default: 8084)', default=8084, envvar='SERVER_PORT', type=int)
@click.option('--host', help='Host for Snowflake proxy server (default: localhostcomputing.com)',
              default=LOCALHOSTCOMPUTING_COM,
              envvar='SERVER_HOST',
              type=str)
@click.option('--metrics-port', help='Grafana metrics port, only available for cloudflared host (default: 5675)',
              default=5675,
              envvar='METRICS_PORT',
              type=str)
@click.option('--catalog', type=click.Choice([e.value for e in Catalog]),
              help='Type of the Snowflake account. Automatically detected from the account if not provided.')
@click.option('--universql-catalog', type=str,
              help='The external catalog that will be used for Iceberg tables. (default: duckdb:///:memory:)',
              envvar='UNIVERSQL_CATALOG')
# @click.option('--snowflake-catalog-integration', type=str,
#               help='Snowflake catalog integration for CREATE TABLE queries',
#               envvar='SNOWFLAKE_CATALOG_INTEGRATION')
# @click.option('--snowflake-external-volume', type=str,
#               help='Snowflake external volume for CREATE TABLE queries',
#               envvar='SNOWFLAKE_EXTERNAL_VOLUME')
@click.option('--aws-profile', help='AWS profile to access S3 (default: `default`)', type=str)
@click.option('--gcp-project',
              help='GCP project to access GCS and apply quota. (to see how to setup auth for GCP and use different accounts, visit https://cloud.google.com/docs/authentication/application-default-credentials)',
              type=str)
# @click.option('--azure-tenant', help='Azure account to access Blob Storage (ex: default az credentials)', type=str)
@click.option('--ssl_keyfile',
              help='SSL keyfile for the proxy server, optional. Use it if you don\'t want to use localhostcomputing.com',
              type=str)
@click.option('--ssl_certfile', help='SSL certfile for the proxy server, optional. ', type=str)
@click.option('--max-memory', type=str, default=DEFAULTS["max_memory"],
              help='DuckDB Max memory to use for the server (default: 80% of total memory)',
              envvar='MAX_MEMORY', )
@click.option('--cache-directory',
              help=f'Data lake cache directory (default: {Path.home() / ".universql" / "cache"})',
              default=Path.home() / ".universql" / "cache",
              envvar='CACHE_DIRECTORY',
              type=str)
@click.option('--home-directory',
              help=f'Home directory for local operations (default: {Path.home()})',
              default=Path.home(),
              envvar='HOME',
              type=str)
@click.option('--max-cache-size', type=str, default=DEFAULTS["max_cache_size"],
              help='DuckDB maximum cache used in local disk (default: 80% of total available disk)',
              envvar='CACHE_PERCENTAGE', )
@click.option('--database-path', type=click.Path(exists=False, writable=True),
              help='Optional DuckDB Path. (default: :memory:) For persistent storage, provide a path similar to `~/.universql/$session_id.duckdb`',
              envvar='DATABASE_PATH')
@click.option('--tunnel', type=click.Choice(["cloudflared", "ngrok"]),
              help='Use tunnel for accessing server from public internet', envvar='TUNNEL')
@click.option('--motherduck-token', type=str,
              help='Motherduck token to enable', envvar='MOTHERDUCK_TOKEN')
def snowflake(host, port, ssl_keyfile, ssl_certfile, account, catalog, metrics_port, tunnel, **kwargs):
    context__params = click.get_current_context().params
    auto_catalog_mode = catalog is None
    if auto_catalog_mode:
        try:
            polaris_server_check = requests.get(
                f"https://{account}.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens")
            is_polaris = polaris_server_check.status_code == 405
        except RequestException as e:
            error_message = (
                f"Unable to find Snowflake account (https://{account}.snowflakecomputing.com), make sure if you have access to the Snowflake account. (maybe need VPN access?) \n"
                f"You can set `--catalog` property to avoid this error. \n {str(e.args)}")
            logger.error(error_message)
            sys.exit(1)

        context__params["catalog"] = Catalog.POLARIS.value if is_polaris else Catalog.SNOWFLAKE.value

    adjective = "apparently" if auto_catalog_mode else ""
    logger.info(f"UniverSQL is starting reverse proxy for {account}.snowflakecomputing.com, "
                f"it's {adjective} a {context__params['catalog']} server.")

    if host == LOCALHOSTCOMPUTING_COM:
        data = socket.gethostbyname_ex(LOCALHOSTCOMPUTING_COM)
        logger.info(f"Using the SSL keyfile and certfile for localhostcomputing.com. DNS resolves to {data}")
        if "127.0.0.1" not in data[2]:
            logger.error(
                "The DNS setting for localhostcomputing.com doesn't point to localhost, refusing to start. Please update UniverSQL.")
            sys.exit(1)

    if tunnel == 'cloudflared':
        start_cloudflared(port, metrics_port)
        if os.getenv('USE_LOCALCOMPUTING_COM') == '1':
            host = "0.0.0.0"
        else:
            host = '127.0.0.1'
    elif tunnel == 'ngrok':
        logger.error("Ngrok is not supported yet. Please use cloudflared.")
        sys.exit(1)

    if host == LOCALHOSTCOMPUTING_COM or ssl_certfile is not None or os.getenv('USE_LOCALCOMPUTING_COM') == '1':
        with tempfile.NamedTemporaryFile(suffix='cert.pem', delete=True) as cert_file:
            cert_file.write(base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['cert']))
            cert_file.flush()
            with tempfile.NamedTemporaryFile(suffix='key.pem', delete=True) as key_file:
                key_file.write(base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['key']))
                key_file.flush()

                uvicorn.run("universql.protocol.snowflake:app",
                            host=host, port=port,
                            ssl_keyfile=ssl_keyfile or key_file.name,
                            ssl_certfile=ssl_certfile or cert_file.name,
                            reload=False,
                            use_colors=True)
    else:
        uvicorn.run("universql.protocol.snowflake:app",
                    host=host, port=port,
                    reload=False,
                    use_colors=True)


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


def get_context_params(endpoint):
    env_vars = {}
    for param in endpoint.params:
        if param.default is not None:
            env_vars[param.name] = str(param.default)
        if param.envvar is not None:
            env_value = os.getenv(param.envvar, None)
            if env_value is not None:
                env_vars[param.name] = env_value
    return env_vars


uvicorn_logger = logging.getLogger("uvicorn.access")
uvicorn_logger.addFilter(EndpointFilter(path="/telemetry/send"))
uvicorn_logger.addFilter(EndpointFilter(path="/queries/v1/query-request"))
uvicorn_logger.addFilter(EndpointFilter(path="/session"))

if __name__ == '__main__':
    cli(prog_name="universql")
