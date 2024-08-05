import base64
import logging
import socket
import sys
import tempfile
import typing
from pathlib import Path

import click
import pkg_resources
import psutil
import requests
import uvicorn
import yaml
from requests.exceptions import SSLError

from universql.util import LOCALHOST_UNIVERSQL_COM_BYTES, Compute, Catalog, sizeof_fmt, prepend_to_lines

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("🏠")

DEFAULTS = {
    "max_memory": sizeof_fmt(psutil.virtual_memory().total * 0.8),
    "max_cache_size": sizeof_fmt(psutil.disk_usage("./").free * 0.8)
}


@click.group()
@click.version_option(version=pkg_resources.get_distribution('universql').version)
def cli():
    pass


@cli.command(epilog='[BETA] Check out docs at https://github.com/buremba/universql and let me know if you have any cool use-case on Github!')
@click.option('--account',
              help='The account to use. Supports both Snowflake and Polaris (ex: rt21601.europe-west2.gcp)',
              prompt='Account ID')
@click.option('--port', help='Port for Snowflake proxy server (default: 8084)', default=8084, type=int)
@click.option('--host', help='Host for Snowflake proxy server (default: localhostcomputing.com)',
              default="localhostcomputing.com",
              type=str)
@click.option('--compute', type=click.Choice([e.value for e in Compute], case_sensitive=False),
              default=Compute.AUTO.value, help=f'Enforce the query execution layer (default: {Compute.AUTO.value}, try with DuckDB and use Snowflake if it fails)')
@click.option('--catalog', type=click.Choice([e.value for e in Catalog]),
              help='Type of the Snowflake account. Automatically detected if not provided.')
@click.option('--aws-profile', help='AWS profile to access S3 (default: `default`)', type=str)
@click.option('--gcp-project',
              help='GCP project to access GCS and apply quota. (to see how to setup auth for GCP and use different accounts, visit https://cloud.google.com/docs/authentication/application-default-credentials)',
              type=str)
# @click.option('--azure-tenant', help='Azure account to access Blob Storage (ex: default az credentials)', type=str)
@click.option('--ssl_keyfile', help='SSL keyfile for the proxy server, optional. Use it if you don\'t want to use localhostcomputing.com', type=str)
@click.option('--ssl_certfile', help='SSL certfile for the proxy server, optional. ', type=str)
@click.option('--max-memory', type=str, default=DEFAULTS["max_memory"],
              help='DuckDB Max memory to use for the server (default: 80% of total memory)')
@click.option('--cache-directory', help=f'Data lake cache directory (default: {Path.home() / ".universql" / "cache"})',
              default=Path.home() / ".universql" / "cache",
              type=str)
@click.option('--max-cache-size', type=str, default=DEFAULTS["max_cache_size"],
              help='DuckDB maximum cache used in local disk (default: 80% of total available disk)')
def snowflake(host, port, ssl_keyfile, ssl_certfile, account, catalog, compute, **kwargs):
    context__params = click.get_current_context().params
    auto_catalog_mode = catalog is None
    if auto_catalog_mode:
        try:
            polaris_server_check = requests.get(
                f"https://{account}.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens")
            is_polaris = polaris_server_check.status_code == 405
        except SSLError as e:
            error_message = (f"Unable to find Snowflake account (https://{account}.snowflakecomputing.com), make sure if you have access to the Snowflake account. (maybe need VPN access?) \n"
                             f"You can set `--catalog` property to avoid this error. \n {str(e.args[0])}")
            logger.error(error_message)
            sys.exit(1)

        context__params["catalog"] = Catalog.POLARIS.value if is_polaris else Catalog.SNOWFLAKE.value

    if context__params["catalog"] == Catalog.POLARIS.value:
        if compute == Compute.SNOWFLAKE.value:
            logger.error("Polaris catalog doesn't support Snowflake compute. Refusing to start.")
            sys.exit(1)

    adjective = "apparently" if auto_catalog_mode else ""
    logger.info(f"UniverSQL is starting reverse proxy for {account}.snowflakecomputing.com, "
                f"it's {adjective} a {context__params['catalog']} server.")

    if compute == Compute.AUTO.value:
        logger.info("The queries will run on DuckDB and fallback to Snowflake if they fail.")
    elif compute == Compute.LOCAL.value:
        logger.info("The queries will run locally")
    elif compute == Compute.SNOWFLAKE.value:
        logger.info("The queries will run directly on Snowflake")

    if not ssl_keyfile or not ssl_certfile:
        data = socket.gethostbyname_ex("localhostcomputing.com")
        logger.info(f"Using the SSL keyfile and certfile for localhostcomputing.com. DNS resolves to {data}")
        if "127.0.0.1" not in data[2]:
            logger.error(
                "The DNS setting for localhostcomputing.com doesn't point to localhost, refusing to start. Please update UniverSQL.")
            # sys.exit(1)

    with tempfile.NamedTemporaryFile(suffix='cert.pem', delete=True) as cert_file:
        cert_file.write(base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['cert']))
        cert_file.flush()
        with tempfile.NamedTemporaryFile(suffix='key.pem', delete=True) as key_file:
            key_file.write(base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['key']))
            key_file.flush()

            uvicorn.run("universql.server:app",
                        host=host, port=port,
                        ssl_keyfile=ssl_keyfile or key_file.name,
                        ssl_certfile=ssl_certfile or cert_file.name,
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


uvicorn_logger = logging.getLogger("uvicorn.access")
uvicorn_logger.addFilter(EndpointFilter(path="/telemetry/send"))
uvicorn_logger.addFilter(EndpointFilter(path="/queries/v1/query-request"))
uvicorn_logger.addFilter(EndpointFilter(path="/session"))

if __name__ == '__main__':
    cli(prog_name="universql")
