from __future__ import annotations

import base64
import logging
import os
import signal
import threading
from threading import Thread
from uuid import uuid4

import click
import psutil
import pyarrow as pa
import yaml

from fastapi import FastAPI
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from universql.lake.fsspec_util import get_friendly_disk_usage
from universql.util import unpack_request_body, session_from_request, SnowflakeError, parameters, \
    print_dict_as_markdown_table
from fastapi.encoders import jsonable_encoder

from universql.warehouse.duckdb.duckdb import UniverSQLSession

app = FastAPI()

sessions = {}
query_results = {}
current_context = click.get_current_context().params

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("🧵")


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
    except SnowflakeError as e:
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
    req = await unpack_request_body(request)
    return Response(status_code=200)


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


@app.post("/queries/v1/query-request")
async def query_request(request: Request) -> JSONResponse:
    try:
        session = session_from_request(sessions, request)
        body = await unpack_request_body(request)
        query = body["sqlText"]
        queryResultFormat, columns, result = session.do_query(query)
        if result is None:
            return JSONResponse({"success": False, "message": "no query provided"})
        query_id = str(uuid4())
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
    except SnowflakeError as e:
        # print_exc( limit=1)  # we print exec here because the connector + webservice combo doesn't always do a good job of
        return JSONResponse(e.to_dict())


@app.get("/")
async def home(request: Request) -> JSONResponse:
    return JSONResponse({"success": True, "status": "X-Duck is ducking 🐥"})


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
            process = psutil.Process()
            percent = psutil.cpu_percent()
            cpu_percent = "%.1f" % percent
            memory_percent = "%.1f" % process.memory_percent()
            disk_info = get_friendly_disk_usage(cache_directory, debug=ENABLE_DEBUG_WATCH_TOWER)
            logger.info(f"Currently {len(sessions)} sessions running {processing_sessions} queries "
                        f"| System: [CPU: {cpu_percent}%] [Memory: {memory_percent}%] [Disk: {disk_info}] ")


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
