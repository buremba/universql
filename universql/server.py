from __future__ import annotations

import base64
import gzip
import json
import logging
import os
import signal
import time
from threading import Thread
from uuid import uuid4

import click
import psutil
import pyarrow as pa

from fastapi import FastAPI
from pyiceberg.exceptions import OAuthError
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from universql.lake.fsspec_util import pprint_disk_usage
from universql.util import unpack_request_body, session_from_request, SnowflakeError, parameters, \
    print_dict_as_markdown_table
from fastapi.encoders import jsonable_encoder

from universql.warehouse.duckdb import UniverSQLSession

app = FastAPI()

sessions = {}
query_results = {}
current_context = click.get_current_context().params

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("🧵")


@app.post("/session/v1/login-request")
async def login_request(request: Request) -> JSONResponse:
    request_body = await request.body()
    if request.headers.get('content-encoding') == 'gzip':
        request_body = gzip.decompress(request_body)

    body = json.loads(request_body)
    login_data = body.get('data')
    client_environment = login_data.get('CLIENT_ENVIRONMENT')
    credentials = {key: client_environment[key] for key in ["schema", "warehouse", "role", "user", "database"] if
                   key in client_environment}
    credentials['password'] = login_data.get('PASSWORD')

    if "user" not in credentials:
        credentials["user"] = login_data.get("LOGIN_NAME")
    if "database" not in credentials:
        credentials["database"] = request.query_params.get('databaseName')
    if "warehouse" not in credentials:
        credentials["warehouse"] = request.query_params.get('warehouse')

    token = str(uuid4())
    message = None
    try:
        session = UniverSQLSession(current_context, token, credentials, login_data.get("SESSION_PARAMETERS"))
        sessions[session.token] = session
    except OAuthError as e:
        message = e.args[0]

    logger.info(
        f"[{token}] Created local session for user {credentials.get('user')} from {request.client.host}:{request.client.port}")
    return JSONResponse(
        {
            "data":
                {
                    "token": token,
                    "masterToken": token,
                    "parameters": parameters,
                    "sessionInfo": {
                        "databaseName": credentials.get('database'),
                        "schemaName": credentials.get('schema'),
                        "warehouseName": credentials.get('warehouse'),
                        "roleName": credentials.get('role')
                    },
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
            "firstLogin": True,
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
        session.close()
        return JSONResponse({"success": True})
    return Response(status_code=404)


@app.post("/telemetry/send")
async def telemetry_request(request: Request) -> JSONResponse:
    json = await request.json()
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
    return JSONResponse({"success": True})


@app.get("/monitoring/queries/{query_id:str}")  # type: ignore[arg-type]
async def query_monitoring_query(self, request: Request) -> JSONResponse:
    query_id = request.path_params["query_id"]
    if query_id not in query_results:
        return JSONResponse({"success": False, "message": "query not found"})
    # note that we always execute synchronously, so we can just return a static success
    return JSONResponse({"data": {"queries": [{"status": "SUCCESS"}]}, "success": True})


def watch_tower(cache_directory, **kwargs):
    while True:
        time.sleep(3)
        processing_sessions = sum(session.processing for token, session in sessions.items())
        if processing_sessions > 0:
            process = psutil.Process()
            percent = psutil.cpu_percent()
            cpu_percent = "%.1f" % percent
            memory_percent = "%.1f" % process.memory_percent()
            disk_info = pprint_disk_usage(cache_directory)
            logger.info(f"[CPU: {cpu_percent}%] [Memory: {memory_percent}%] [Disk: {disk_info}] "
                        f"Currently {len(sessions)} sessions running {processing_sessions} queries. ")


thread = Thread(target=watch_tower, kwargs=(current_context))
thread.start()


def harakiri(_, frame):
    os.kill(os.getpid(), signal.SIGTERM)


last_intent_to_kill = time.time()


def graceful_shutdown(_, frame):
    global last_intent_to_kill
    processing_sessions = sum(session.processing for token, session in sessions.items())
    if processing_sessions == 0 or (time.time() - last_intent_to_kill) < 5000:
        harakiri(signal, frame)
    else:
        print(f'Repeat sigint to confirm killing {processing_sessions} running queries.')
        last_intent_to_kill = time.time()


@app.on_event("startup")
async def startup_event():
    import signal
    signal.signal(signal.SIGINT, graceful_shutdown)
    host_port = f"{current_context.get('host')}:{current_context.get('port')}"
    connections = {
        "Node.js": f"snowflake.createConnection({{accessUrl: 'https://{host_port}'}})",
        "JDBC": f"jdbc:snowflake://{host_port}/dbname",
        "Python": f"snowflake.connector.connect(host='{current_context.get('host')}', port='{current_context.get('port')}')",
        "PHP": f"new PDO('snowflake:host={host_port}', '<user>', '<password>')",
        "Go": f"sql.Open('snowflake', 'user:pass@{host_port}/dbname')",
        ".NET": f"host=;{host_port};db=testdb",
        "ODBC": f"Server={current_context.get('host')}; Database=dbname; Port={current_context.get('port')}",
    }
    click.secho(print_dict_as_markdown_table(connections, footer_message=f"For other clients and applications, see https://github.com/buremba/universql",))
