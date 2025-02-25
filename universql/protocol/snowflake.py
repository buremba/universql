import asyncio
import glob
import json
import logging
import base64
import os
import signal
import threading
from traceback import print_exc

from typing import Any
from uuid import uuid4

import click
import pyarrow as pa
import sentry_sdk
import yaml

from fastapi import FastAPI
from pyarrow import Schema
from snowflake.connector import DatabaseError
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, HTMLResponse

from universql.agent.cloudflared import get_cloudflare_url
from universql.plugin import APPS
from universql.util import unpack_request_body, session_from_request, parameters, \
    print_dict_as_markdown_table, QueryError, LOCALHOSTCOMPUTING_COM, current_context
from fastapi.encoders import jsonable_encoder
from starlette.concurrency import run_in_threadpool
from universql.protocol.session import UniverSQLSession, sessions, harakiri, kill_event

logger = logging.getLogger("🧵")

app = FastAPI()
query_results = {}

# register all warehouses and plugins
for module in [
    "universql.warehouse.duckdb",
    "universql.warehouse.bigquery",
    "universql.warehouse.snowflake",
    "universql.warehouse.redshift",
    "universql.plugins.snow",
    "universql.plugins.ui",
]:
    __import__(module)


@app.post("/session/v1/login-request")
async def login_request(request: Request) -> JSONResponse:
    body = await unpack_request_body(request)

    login_data = body.get('data')
    client_environment = login_data.get('CLIENT_ENVIRONMENT')
    credentials = {key: client_environment[key] for key in ["schema", "warehouse", "role", "user", "database"] if
                   key in client_environment}
    if login_data.get('PASSWORD') is not None:
        credentials['password'] = login_data.get('PASSWORD')
    if "user" not in credentials and login_data.get('LOGIN_NAME') is not None:
        credentials["user"] = login_data.get("LOGIN_NAME")

    params = request.query_params
    if "database" not in credentials:
        credentials["database"] = params.get('databaseName')
    if "warehouse" not in credentials:
        credentials["warehouse"] = params.get('warehouse')
    if "role" not in credentials:
        credentials["role"] = params.get('roleName')
    if "schema" not in credentials:
        # TODO: support different default schemas stored in `SHOW PARAMETERS LIKE 'search_path'`
        credentials["schema"] = params.get('schemaName') or "PUBLIC"

    token = str(uuid4())
    message = None
    try:
        session = UniverSQLSession(current_context, token, credentials, login_data.get("SESSION_PARAMETERS"))
        sessions[session.session_id] = session
    except QueryError as e:
        message = e.message

    client = f"{request.client.host}:{request.client.port}"

    if message is None:
        logger.info(
            f"[{token}] Created local session for user {credentials.get('user')} from {client}")
    else:
        logger.error(
            f"Rejected login request from {client} for user {credentials.get('user')}. Reason: {message}")

    return JSONResponse(
        {
            "data":
                {
                    "token": token,
                    # TODO: figure out how to generate safer token
                    "masterToken": token,
                    "parameters": parameters,
                    "sessionInfo": {f'{k}Name': v for k, v in credentials.items()},
                    "idToken": None,
                    "idTokenValidityInSeconds": 0,
                    "responseData": None,
                    "mfaToken": None,
                    "mfaTokenValidityInSeconds": 0
                },
            "message": message,
            "success": message is None,
            "code": None,
            "validityInSeconds": 3600,
            "masterValidityInSeconds": 14400,
            "displayUserName": "",
            "serverVersion": "duck",
            "firstLogin": False,
            "remMeToken": None,
            "remMeValidityInSeconds": 0,
            "healthCheckInterval": 45,
        })


@app.post("/session")
async def delete_session(request: Request):
    if request.query_params.get("delete") == "true":
        try:
            session = session_from_request(sessions, request)
        except HTTPException as e:
            if e.status_code == 401:
                # most likely the server has started
                return JSONResponse({"success": True})

        del sessions[session.session_id]
        logger.info(f"[{session.session_id}] Session closed, cleaning up resources.")
        session.close()
        return JSONResponse({"success": True})
    return Response(status_code=404)


@app.post("/telemetry/send")
async def telemetry_request(request: Request) -> JSONResponse:
    request = await unpack_request_body(request)
    logs = request.get('logs')
    return JSONResponse(content={"success": True}, status_code=200)


@app.post("/session/heartbeat")
async def session_heartbeat(request: Request) -> JSONResponse:
    try:
        session_from_request(sessions, request)
    except HTTPException as e:
        if e.status_code == 401:
            # most likely the server has started
            return JSONResponse({"success": False})

    return JSONResponse(
        {"data": {}, "success": True})


def get_columns_for_sf_compat(schema: Schema) -> list[dict[str, Any]]:
    columns = []
    for idx, col in enumerate(schema):
        # args = sqlglot.expressions.DataType.build(col[1], dialect=dialect).args
        # precision = args.get('expressions')[0].this
        # scale = args.get('expressions')[1].this
        columns.append({
            "name": col.name,
            "database": "",
            "schema": "",
            "table": "",
            "nullable": True,
            "type": col.metadata.get(b'logicalType').decode(),
            "length": None,
            "scale": None,
            "precision": None,
            # "scale": int(scale.name),
            # "precision": int(precision.name),
            "byteLength": None,
            "collation": None
        })
    return columns


@app.post("/queries/v1/abort-request")
async def abort_request(request: Request) -> JSONResponse:
    return JSONResponse(
        {"data": {}, "success": True})


@app.post("/queries/v1/query-request")
async def query_request(request: Request) -> JSONResponse:
    query_id = str(uuid4())
    query = None
    try:
        session = session_from_request(sessions, request)
        body = await unpack_request_body(request)
        query = body["sqlText"]
        queryResultFormat = "arrow"
        transaction = sentry_sdk.get_current_scope().transaction
        transaction.set_tag("query", query)
        result = await run_in_threadpool(session.do_query, query)
        columns = get_columns_for_sf_compat(result.schema)
        if result is None:
            return JSONResponse({"success": False, "message": "no query provided"})
        data: dict = {
            "finalDatabaseName": session.credentials.get("database"),
            "finalSchemaName": session.credentials.get("schema"),
            "finalWarehouseName": session.credentials.get("warehouse"),
            "finalRoleName": session.credentials.get("role"),
            "rowtype": columns,
            "queryResultFormat": queryResultFormat,
            "databaseProvider": None,
            "numberOfBinds": 0,
            "arrayBindSupported": False,
            "queryId": query_id,
            "parameters": parameters
        }
        if body.get("asyncExec", False):
            # we should store the result in the server for when it gets retrieved
            query_results[query_id] = result

        format = data.get('queryResultFormat')
        if format == "json":
            data["rowset"] = jsonable_encoder(result)
        elif format == "arrow":
            number_of_rows = len(result)
            data["returned"] = number_of_rows
            if number_of_rows > 0:
                sink = pa.BufferOutputStream()
                with pa.ipc.new_stream(sink, result.schema) as writer:
                    batches = result.to_batches()
                    if len(batches) == 0:
                        empty_batch = pa.RecordBatch.from_pylist([], schema=result.schema)
                        writer.write_batch(empty_batch)
                    else:
                        for batch in batches:
                            writer.write_batch(batch)
                buf = sink.getvalue()
                # one-copy
                pybytes = buf.to_pybytes()
                b_encode = base64.b64encode(pybytes)
                encode = b_encode.decode('utf-8')
            else:
                encode = ""
            data["rowsetBase64"] = encode
        else:
            raise Exception(f"Format {format} is not supported")
        return JSONResponse({"data": data, "success": True})
    except QueryError as e:
        # print_exc(limit=1)
        return JSONResponse({"id": query_id, "success": False, "message": e.message, "data": {"sqlState": e.sql_state}})
    except DatabaseError as e:
        print_exc(limit=10)
        return JSONResponse({"id": query_id, "success": False,
                             "message": f"Error running query on Snowflake: {e.raw_msg}",
                             "data": {"sqlState": e.sqlstate}})
    except Exception as e:
        if not isinstance(e, HTTPException):
            print_exc(limit=10)
            if query is not None:
                logger.exception(f"Error processing query: {query}")
            else:
                logger.exception(f"Error processing query request", e)
        return JSONResponse({"id": query_id, "success": False,
                             "message": "Unable to run the query due to a system error. Please create issue on https://github.com/buremba/universql/issues",
                             "data": {"sqlState": "0000"}})


@app.get("/jupyterlite/new")
async def jupyter(request: Request) -> JSONResponse:
    return HTMLResponse(" <style>body {margin: 0}</style>" + f"""
    <iframe
    frameborder="0"
  src="https://jupyterlite.github.io/{'demo/repl' if request.query_params.get('repl') is not None else 'demo/lab'}/index.html?code=import numpy as np&theme=JupyterLab Dark"
  width="100%"
  height="100%"
></iframe>
    """)


@app.get("/")
async def home(request: Request) -> JSONResponse:
    return JSONResponse(
        {"status": "X-Duck is ducking 🐥", "new": {"jupyter": "/jupyterlite/new", "streamlit": "/streamlit/new"}})

def get_files(path):
    files_dict = {}
    prefix = f"app/{path}/"
    for filepath in glob.glob(f"{prefix}**/*", recursive=True):
        if os.path.isfile(filepath):
            with open(filepath, 'r', encoding='utf-8') as file:
                # Create relative path as key
                relative_path = os.path.relpath(filepath, prefix)
                files_dict[relative_path] = file.read()
    return files_dict

def get_requirements(path):
    requirements_file = os.path.abspath(f"app/{path}/requirements.txt")
    if os.path.exists(requirements_file):
        with open(requirements_file, 'r', encoding='utf-8') as file:
            return file.read().splitlines()
    else:
        return []


@app.get("/{rest_of_path:path}")
async def streamlit_renderer(request: Request, rest_of_path: str) -> JSONResponse:
    settings = {"requirements": get_requirements(rest_of_path), "entrypoint": "page.py", "streamlitConfig": {},
                "files": get_files(rest_of_path)}
    return HTMLResponse(f"""
    <!doctype html>
<html>
  <head>
    <meta charset="UTF-8" />
    <meta http-equiv="X-UA-Compatible" content="IE=edge" />
    <meta
      name="viewport"
      content="width=device-width, initial-scale=1, shrink-to-fit=no"
    />
    <title>Stlite App</title>
    <link
      rel="stylesheet"
      href="https://cdn.jsdelivr.net/npm/@stlite/mountable@0.63.1/build/stlite.css"
    />
  </head>
  <body>
    <div id="root"></div>
    <script src="https://cdn.jsdelivr.net/npm/@stlite/mountable@0.63.1/build/stlite.js"></script>
    <script>
      stlite.mount({json.dumps(settings)},
        document.getElementById("root"),
      );
    </script>
  </body>
</html>
    """)


@app.get("/monitoring/queries/{query_id:str}")
async def query_monitoring_query(request: Request) -> JSONResponse:
    query_id = request.path_params["query_id"]
    if query_id not in query_results:
        return JSONResponse({"success": False, "message": "query not found"})
    # note that we always execute synchronously, so we can just return a static success
    return JSONResponse({"data": {"queries": [{"status": "SUCCESS"}]}, "success": True})


@app.on_event("shutdown")
async def shutdown_event():
    kill_event.set()
    for token, session in sessions.items():
        session.close()


@app.on_event("startup")
async def startup_event():
    params = {k: v for k, v in current_context.items() if
              v is not None and k not in ["host", "port"]}
    click.secho(yaml.dump(params).strip())
    if threading.current_thread() is threading.main_thread():
        try:
            signal.signal(signal.SIGINT, harakiri)
        except Exception as e:
            logger.warning("Failed to set signal handler for SIGINT: %s", str(e))
    host = current_context.get('host')
    tunnel = current_context.get('tunnel')
    port = current_context.get('port')
    if tunnel == "cloudflared":
        metrics_port = current_context.get('metrics_port')
        click.secho(f" * Traffic stats available on http://127.0.0.1:{metrics_port}/metrics")
        host_port = host = get_cloudflare_url(metrics_port)
        port = 443
        click.secho(f" * Tunnel available at https://{host_port}")
    elif os.getenv('USE_LOCALCOMPUTING_COM') == '1':
        host_port = f"{LOCALHOSTCOMPUTING_COM}:{port}"
    else:
        host_port = f"{host}:{port}"
    connections = {
        "Node.js": f"snowflake.createConnection({{accessUrl: '{host_port}'}})",
        "JDBC": f"jdbc:snowflake://{host_port}/dbname",
        "Python": f"snowflake.connector.connect(host='{host}', port='{port}')",
        "PHP": f"new PDO('snowflake:host={host_port}', '<user>', '<password>')",
        "Go": f"sql.Open('snowflake', 'user:pass@{host_port}/dbname')",
        ".NET": f"host={host_port};db=testdb",
        "ODBC": f"Server={host}; Database=dbname; Port={port}",
    }
    click.secho(print_dict_as_markdown_table(connections, footer_message=(
        "You can connect to UniverSQL with any Snowflake client using your Snowflake credentials.",
        "For application support, see https://github.com/buremba/universql",)))


loop = asyncio.get_event_loop()

for app_to_be_installed in APPS:
    app_to_be_installed(app)
    # loop.run_until_complete(app_to_be_installed(app))
# loop.close()