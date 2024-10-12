from __future__ import annotations

import base64
import logging
import os
import signal
import threading
import time
from threading import Thread
from typing import Any
from uuid import uuid4

import click
import pyarrow as pa
import yaml

from fastapi import FastAPI
from pyarrow import Schema
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, HTMLResponse

from universql.lake.fsspec_util import get_friendly_disk_usage
from universql.util import unpack_request_body, session_from_request, parameters, \
    print_dict_as_markdown_table, QueryError
from fastapi.encoders import jsonable_encoder
from starlette.concurrency import run_in_threadpool
from universql.protocol.session import UniverSQLSession

app = FastAPI()

sessions = {}
query_results = {}

context = click.get_current_context(silent=True)
if context is None:
    from universql.main import snowflake, get_context_params
    current_context = get_context_params(snowflake)
else:
    current_context = context.params

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("🧵")


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    # if request.url.path in ["/queries/v1/query-request"]:
    # print(f"Time took to process {request.url.path} is {time.perf_counter() - start_time} sec")
    return response


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
        credentials["schema"] = params.get('schemaName')

    token = str(uuid4())
    message = None
    try:
        session = UniverSQLSession(current_context, token, credentials, login_data.get("SESSION_PARAMETERS"))
        sessions[session.token] = session
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

        del sessions[session.token]
        logger.info(f"[{session.token}] Session closed, cleaning up resources.")
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


@app.post("/queries/v1/query-request")
async def query_request(request: Request) -> JSONResponse:
    query_id = str(uuid4())
    try:
        session = session_from_request(sessions, request)
        body = await unpack_request_body(request)
        query = body["sqlText"]
        queryResultFormat = "arrow"
        result = await run_in_threadpool(session.do_query, query)
        columns = get_columns_for_sf_compat(result.schema)
        if result is None:
            return JSONResponse({"success": False, "message": "no query provided"})
        data: dict = {
            "finalDatabaseName": session.credentials.get("database"),
            "finalSchemaName": session.credentials.get("schema"),
            "rowtype": columns,
            "queryResultFormat": queryResultFormat,
            "queryId": query_id,
            "parameters": parameters
        }
        if body.get("asyncExec", False):
            # we should store the result in the server for when it gets retrieved
            query_results[query_id] = result
        if not result:
            return JSONResponse({"data": data, "success": True})

        format = data.get('queryResultFormat')
        if format == "json":
            data["rowset"] = jsonable_encoder(result)
        elif format == "arrow":
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, result.schema) as writer:
                for batch in result.to_batches():
                    writer.write_batch(batch)
            buf = sink.getvalue()
            # one-copy
            pybytes = buf.to_pybytes()
            b_encode = base64.b64encode(pybytes)
            encode = b_encode.decode('utf-8')
            data["rowsetBase64"] = encode
        return JSONResponse({"data": data, "success": True})
    except QueryError as e:
        # print_exc( limit=1)  # we print exec here because the connector + webservice combo doesn't always do a good job of
        return JSONResponse({"id": query_id, "success": False, "message": e.message, "data": {"sqlState": e.sql_state}})


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


@app.get("/streamlit/new")
async def streamlit_new(request: Request) -> JSONResponse:
    return HTMLResponse("""
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
      stlite.mount(
        `
import streamlit as st

name = st.text_input('Your name')
st.write("Hello,", name or "world")
`,
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


ENABLE_DEBUG_WATCH_TOWER = False
WATCH_TOWER_SCHEDULE_SECONDS = 2
kill_event = threading.Event()


def watch_tower(cache_directory, **kwargs):
    while True:
        kill_event.wait(timeout=WATCH_TOWER_SCHEDULE_SECONDS)
        if kill_event.is_set():
            break
        processing_sessions = sum(session.processing for token, session in sessions.items())
        if ENABLE_DEBUG_WATCH_TOWER or processing_sessions > 0:
            try:
                import psutil
                process = psutil.Process()
                cpu_percent = f"[CPU: {'%.1f' % psutil.cpu_percent()}%]"
                memory_percent = f"[Memory: {'%.1f' % process.memory_percent()}%]"
            except:
                memory_percent = ""
                cpu_percent = ""

            disk_info = get_friendly_disk_usage(cache_directory, debug=ENABLE_DEBUG_WATCH_TOWER)
            logger.info(f"Currently {len(sessions)} sessions running {processing_sessions} queries "
                        f"| System: {cpu_percent} {memory_percent} [Disk: {disk_info}] ")


thread = Thread(target=watch_tower, kwargs=(current_context))
thread.daemon = False
thread.start()


# If the user intends to kill the server, not wait for DuckDB to gracefully shutdown.
# It's nice to treat the Duck better giving it time but user's time is more valuable than the duck's.
def harakiri(sig, frame):
    print("Killing the server, bye!")
    kill_event.set()
    os.kill(os.getpid(), signal.SIGKILL)


# last_intent_to_kill = time.time()
# def graceful_shutdown(_, frame):
#     global last_intent_to_kill
#     processing_sessions = sum(session.processing for token, session in sessions.items())
#     if processing_sessions == 0 or (time.time() - last_intent_to_kill) < 5000:
#         harakiri(signal, frame)
#     else:
#         print(f'Repeat sigint to confirm killing {processing_sessions} running queries.')
#         last_intent_to_kill = time.time()

@app.on_event("shutdown")
async def shutdown_event():
    kill_event.set()
    for token, session in sessions.items():
        session.close()


@app.on_event("startup")
async def startup_event():
    signal.signal(signal.SIGINT, harakiri)
    host_port = f"{current_context.get('host')}:{current_context.get('port')}"
    connections = {
        "Node.js": f"snowflake.createConnection({{accessUrl: 'https://{host_port}'}})",
        "JDBC": f"jdbc:snowflake://{host_port}/dbname",
        "Python": f"snowflake.connector.connect(host='{current_context.get('host')}', port='{current_context.get('port')}')",
        "PHP": f"new PDO('snowflake:host={host_port}', '<user>', '<password>')",
        "Go": f"sql.Open('snowflake', 'user:pass@{host_port}/dbname')",
        ".NET": f"host={host_port};db=testdb",
        "ODBC": f"Server={current_context.get('host')}; Database=dbname; Port={current_context.get('port')}",
    }
    params = {k: v for k, v in current_context.items() if
              v is not None and k not in ["host", "port"]}
    click.secho(yaml.dump(params).strip())
    click.secho(print_dict_as_markdown_table(connections,
                                             footer_message=(
                                                 "You can connect to UniverSQL with any Snowflake client using your Snowflake credentials.",
                                                 "For application support, see https://github.com/buremba/universql",)))
