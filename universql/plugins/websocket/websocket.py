import asyncio
import json
import logging
import os
import secrets
import string
import uuid
import time
from typing import Dict, Set, Optional, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from concurrent.futures import Future

import pyarrow
import sqlglot
from universql.plugin import Executor, register, Tables, Locations, ICatalog
from universql.protocol.session import Sessions, UniverSQLSession, sessions as global_sessions
from .websocket_models import SqlResponse

# Ensure warehouse modules are imported to register in COMPUTES
for module in [
    "universql.warehouse.duckdb",
    "universql.warehouse.postgresql",
]:
    __import__(module)

logger = logging.getLogger("websocket")

# Global response queues for coordinating responses from websocket
# Use thread-safe futures instead of asyncio futures
response_queues: Dict[str, Future] = {}
response_queue_timestamps: Dict[str, float] = {}
_main_loop: Optional[asyncio.AbstractEventLoop] = None

# Response queue cleanup configuration
RESPONSE_QUEUE_TIMEOUT = 30  # seconds

def set_main_loop(loop: asyncio.AbstractEventLoop):
    """Store reference to the main asyncio loop"""
    global _main_loop
    _main_loop = loop

def cleanup_response_queue(query_id: str):
    """Clean up response queue for a specific query"""
    if query_id in response_queues:
        fut = response_queues[query_id]
        if not fut.done():
            fut.cancel()
        del response_queues[query_id]
    
    # Also clean up timestamp tracking
    response_queue_timestamps.pop(query_id, None)

async def cleanup_stale_response_queues():
    """Periodic cleanup of stale response queues"""
    while True:
        await asyncio.sleep(10)  # Check every 10 seconds
        current_time = time.time()
        stale_queries = []
        
        for query_id, timestamp in response_queue_timestamps.items():
            if current_time - timestamp > RESPONSE_QUEUE_TIMEOUT:
                stale_queries.append(query_id)
        
        for query_id in stale_queries:
            cleanup_response_queue(query_id)
            logger.debug(f"Cleaned up stale response queue: {query_id}")
        
        if stale_queries:
            logger.info(f"Cleaned up {len(stale_queries)} stale response queues")

@register(name="websocket")
class WebsocketCatalog (ICatalog):
    def __init__(self, session: UniverSQLSession):
        super().__init__(session)
        # Store the session object since the base ICatalog doesn't store it
        self.session = session

    def get_table_paths(self, tables: List[sqlglot.exp.Table]) -> Tables:
        # For websocket catalog, return empty table paths as we delegate to remote execution
        return {table: None for table in tables}

    def register_locations(self, tables: Locations):
        # For websocket catalog, this is a no-op as we don't manage physical locations
        pass

    def executor(self) -> "Executor":
        return WebsocketExecutor(self)

class WebsocketExecutor (Executor):
    def __init__(self, catalog: WebsocketCatalog):
        self.catalog = catalog
        self.last_result = None
        self.last_query_log = ""

    def execute(self, ast: sqlglot.exp.Expression, catalog_executor: "Executor", locations: Tables) -> Optional[Locations]:
        # Execute the query and store the result
        self.execute_raw(ast.sql(), catalog_executor)
        # For websocket executor, we don't create new table locations, so return None
        return None

    def execute_raw(self, raw_query: str, catalog_executor: Optional["Executor"]):
        session = self.catalog.session
        websocket = global_sessions.find_websocket_by_credentials(session.credentials)

        if websocket is None:
            raise RuntimeError("No active websocket connection found")

        timeout = 10
        server_id = session.credentials.get("server_id") or session.credentials.get("user")
        if not server_id:
            raise RuntimeError("No server_id or user found in session credentials")

        # Run the async operation using thread-safe mechanism
        if _main_loop is None:
            raise RuntimeError("Main event loop not set")
            
        future = asyncio.run_coroutine_threadsafe(
            self._send_query_and_wait_for_response(websocket, server_id, raw_query, session.session_id, timeout),
            _main_loop
        )
        
        try:
            result_table = future.result(timeout=timeout + 1)  # Add 1 second buffer
            self.last_result = result_table
        except Exception as e:
            raise RuntimeError(f"Error executing query via websocket: {str(e)}")

    async def _send_query_and_wait_for_response(self, websocket, server_id: str, sql: str, session_id: str, timeout: int) -> pyarrow.Table:
        query_id = str(uuid.uuid4())
        
        logger.info(f"Sending query to websocket: {sql}")
        await websocket.send_json({
            "type": "query",
            "sql": sql,
            "session_id": session_id,
            "query_id": query_id
        })

        fut = Future()
        response_queues[query_id] = fut
        response_queue_timestamps[query_id] = time.time()  # Track creation time
        logger.info(f"Created future for query_id: {query_id}")

        try:
            # Wait for the thread-safe future
            loop = asyncio.get_running_loop()
            response_data = await asyncio.wait_for(
                loop.run_in_executor(None, fut.result),
                timeout=timeout
            )
            
            logger.info(f"Received response from websocket: {response_data}")
            response = SqlResponse.model_validate(response_data)
            
            columns = response.results[0].columns
            rows = response.results[0].rows

            pa_arrays = []
            names = []
            for idx, col_name in enumerate(columns):
                names.append(col_name)
                col_values = [row[idx] for row in rows]
                pa_arrays.append(pyarrow.array(col_values))

            return pyarrow.Table.from_arrays(pa_arrays, names)
        except asyncio.TimeoutError:
            cleanup_response_queue(query_id)
            raise RuntimeError(f"Query timeout after {timeout} seconds")
        except Exception as e:
            cleanup_response_queue(query_id)
            raise RuntimeError(f"Error executing query via websocket: {str(e)}")
        finally:
            cleanup_response_queue(query_id)

    async def _convert_query_result(self, query_result, command_tag: str) -> dict:
        if not query_result:
            return {
                "success": True,
                "columns": [],
                "rows": [],
                "command_tag": command_tag
            }
        
        # Handle PyArrow Table
        if hasattr(query_result, 'schema') and hasattr(query_result, 'num_columns'):
            columns = []
            for i in range(query_result.num_columns):
                field = query_result.schema.field(i)
                columns.append({
                    "name": field.name,
                    "type_oid": 25,  
                    "type_name": "text"
                })
            
            rows = []
            for row_idx in range(query_result.num_rows):
                row = []
                for col_idx in range(query_result.num_columns):
                    column_array = query_result.column(col_idx)
                    value = column_array[row_idx].as_py()
                    row.append(str(value) if value is not None else None)
                rows.append(row)
            
            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "command_tag": f"{command_tag} {len(rows)}"
            }
        
        return {
            "success": True,
            "columns": [],
            "rows": [],
            "command_tag": command_tag
        }

    def get_as_table(self) -> pyarrow.Table:
        """Get the result as a PyArrow table"""
        if self.last_result is None:
            # Return empty table
            return pyarrow.Table.from_pylist([])
        
        # Handle PyArrow Table directly
        if hasattr(self.last_result, 'schema') and hasattr(self.last_result, 'num_columns'):
            return self.last_result
        
        # Handle other result types - try to convert to PyArrow Table
        try:
            if hasattr(self.last_result, 'rows') and hasattr(self.last_result, 'column_count'):
                # Custom query result object
                columns = []
                column_names = []
                
                # Get column information
                for i in range(self.last_result.column_count()):
                    name, bvtype = self.last_result.column(i)
                    column_names.append(name)
                
                # Convert rows to list of dictionaries
                rows_data = []
                for row in self.last_result.rows():
                    row_dict = {}
                    for j, value in enumerate(row):
                        if j < len(column_names):
                            row_dict[column_names[j]] = value
                    rows_data.append(row_dict)
                
                return pyarrow.Table.from_pylist(rows_data)
            else:
                # Fallback to empty table
                return pyarrow.Table.from_pylist([])
        except Exception as e:
            self.last_query_log += f" - Error converting to PyArrow table: {str(e)}"
            return pyarrow.Table.from_pylist([])

    def get_query_log(self, total_duration) -> str:
        """Get the query execution log"""
        base_log = self.last_query_log if self.last_query_log else "WebSocket query execution"
        
        return f"{base_log} | Duration: {total_duration:.3f}s"

    def close(self):    
        self.last_result = None
        self.last_query_log = ""

@register()
def websocket_server(app: FastAPI):
    
    # Store the main loop reference when FastAPI starts
    @app.on_event("startup")
    async def startup_event():
        set_main_loop(asyncio.get_running_loop())
        # Start the response queue cleanup task
        asyncio.create_task(cleanup_stale_response_queues())

    origins = ["*"]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    global_sessions.set_link("user-1750810808175", {
            "credentials": {
                "user": "user-1750810808175",
                "password": "pass_9bxi9s8j3"
            }, 
            "websocket": None
    })


    @app.post("/api/create-session")
    async def create_session_endpoint(request: dict = {}):
        from fastapi import HTTPException
        
        try:
            server_id = request.get("server_id") if request else None
            
            # Check for server_id conflict
            if global_sessions.get_link(server_id) is not None:
                raise HTTPException(status_code=401, detail=f"Server ID {server_id} is already in use")
            
            user = f"user_{secrets.token_hex(4)}"
            password = secrets.token_urlsafe(16)
            
            credentials = {
                "server_id": server_id,
                "user": user,
                "password": password
            }
            
            global_sessions.set_link(server_id, {
                "credentials": credentials,
                "websocket": None
            })
            
            return {
                "success": True,
                "credentials": credentials,
                "message": "Session created successfully"
            }
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to create session: {str(e)}")

    @app.get("/api/health")
    async def health_check():
        """Health check endpoint with connection and queue statistics"""
        current_time = time.time()
        
        # Count active response queues by age
        active_queues = len(response_queues)
        stale_queues = sum(
            1 for timestamp in response_queue_timestamps.values()
            if current_time - timestamp > RESPONSE_QUEUE_TIMEOUT
        )
        
        # Count active websocket connections
        active_websockets = sum(
            1 for link in global_sessions._links.values()
            if link.get("websocket") is not None
        )
        
        return {
            "status": "healthy",
            "timestamp": current_time,
            "response_queues": {
                "active": active_queues,
                "stale": stale_queues,
                "total": len(response_queue_timestamps)
            },
            "sessions": {
                "total_sessions": len(global_sessions._sessions),
                "total_links": len(global_sessions._links),
                "active_websockets": active_websockets,
                "user_index_size": len(global_sessions._user_index),
                "credential_hash_index_size": len(global_sessions._credential_hash_index)
            },
            "config": {
                "response_queue_timeout": RESPONSE_QUEUE_TIMEOUT
            }
        }

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        authenticated = False
        server_id = None
        
        try:
            while True:
                data = await websocket.receive_text()
                
                try:
                    message = json.loads(data)
                    command = message.get("type")
                    
                    if command == "connect":
                        credentials = message.get("credentials", {})
                        
                        server_id = message.get("server_id")
                        server = global_sessions.get_link(server_id)
                        if not server or server.get("credentials").get("user") != credentials.get("user") or    server.get("credentials").get("password") != credentials.get("password"):
                            await websocket.send_json({
                                "type": "error",
                                "message": "authentication_failed"
                            })
                            continue
                        
                        global_sessions.get_link(server_id)["websocket"] = websocket                            
                        authenticated = True
                        global_sessions.set_link(server_id, {
                            "credentials": server.get("credentials"),
                            "websocket": websocket
                        })
                        
                        await websocket.send_json({
                            "type": "connection_established",
                            "data": {
                                "user": server.get("credentials").get("user"),
                                "server_id": server.get("credentials").get("server_id"),
                            },
                            "server_version": "universql-websocket",
                            "client_encoding": "UTF8",
                            "DateStyle": "ISO"
                        })
                        
                    elif command == "query_executed" and authenticated:
                        query_id = message.get("query_id")
                        logger.info(f"Processing query_executed for query_id: {query_id}")
                        if query_id in response_queues and not response_queues[query_id].done():
                            response_queues[query_id].set_result(message)
                        else:
                            logger.warning(f"No pending future found for query_id: {query_id}")
                        
                except json.JSONDecodeError:
                    pass
                
                if not authenticated:
                    await websocket.send_json({
                        "type": "error",
                        "message": "authentication_required"
                    })
                    continue
                    
        except WebSocketDisconnect:
            if authenticated and server_id:
                if server_id in global_sessions._sessions:
                    global_sessions.get_link(server_id)["websocket"] = None
    

    logger.info("WebSocket server plugin registered successfully") 