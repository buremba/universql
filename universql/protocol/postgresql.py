import asyncio
import base64
import datetime
import uuid
import ssl
import tempfile
import os

import dateutil.parser
import hashlib
import io
import json
import logging
import random
import socket
import struct
from typing import Dict, List, Optional

from fastapi import FastAPI
import pyarrow
import sqlglot

from universql.plugin import APPS
from universql.protocol.session import Sessions, UniverSQLSession, sessions as global_sessions
from universql.util import LOCALHOST_UNIVERSQL_COM_BYTES
from universql.warehouse.postgresql import PyPgType

# register all warehouses and plugins
for module in [
    "universql.warehouse.postgresql",
    "universql.plugins.ui",
    "universql.plugins.websocket.websocket",
]:
    __import__(module)

logger = logging.getLogger("🐘")

logging.getLogger('postgresql.connector.proxy').setLevel(logging.DEBUG)
# SQLGlot doesn't recognize postgresql key for dialect
sqlglot.dialects.Dialect.classes['postgresql'] = sqlglot.dialects.Dialect.classes['postgres']


NULL_BYTE = b"\x00"


class ServerResponse:
    """Byte codes for server responses in the PG wire protocol."""

    AUTHENTICATION_REQUEST = b"R"
    BACKEND_KEY_DATA = b"K"
    BIND_COMPLETE = b"2"
    CLOSE_COMPLETE = b"3"
    COMMAND_COMPLETE = b"C"
    DATA_ROW = b"D"
    EMPTY_QUERY_RESPONSE = b"I"
    ERROR_RESPONSE = b"E"
    NO_DATA = b"n"
    NOTICE_RESPONSE = b"N"
    PARAMETER_DESCRIPTION = b"t"
    PARAMETER_STATUS = b"S"
    PARSE_COMPLETE = b"1"
    PORTAL_SUSPENDED = b"s"
    READY_FOR_QUERY = b"Z"
    ROW_DESCRIPTION = b"T"


TYPE_OIDS = {
    # see Postgres pg_type_d.h
    0: ("date/time", None, datetime.datetime(1900, 1, 1)),
    20: ("INT8OID", "!q", 42),
    21: ("INT2OID", "!h", 42),
    23: ("INT4OID", "!i", 42),
    700: ("FLOAT4OID", "!f", 42.0),
    701: ("FLOAT8OID", "!d", 42.0)
    # ...

    # BOOLOID 16
    # BYTEAOID 17
    # CHAROID 18
    # NAMEOID 19
    # TEXTOID 25
    # OIDOID 26
    # UNKNOWNOID 705
    # MONEYOID 790
    # INETOID 869
    # VARCHAROID 1043
    # DATEOID 1082
    # TIMEOID 1083
    # TIMESTAMPOID 1114
    # TIMESTAMPTZOID 1184
    # INTERVALOID 1186
    # TIMETZOID 1266
    # BITOID 1560
    # VARBITOID 1562
    # NUMERICOID 1700
}


class TransactionStatus:
    IDLE = b"I"
    IN_TRANSACTION = b"T"
    IN_FAILED_TRANSACTION = b"E"


# Client commands
class ClientCommand:
    """Byte codes for client commands in the PG wire protocol."""

    BIND = b"B"
    CLOSE = b"C"
    DESCRIBE = b"D"
    EXECUTE = b"E"
    FLUSH = b"H"
    QUERY = b"Q"
    PARSE = b"P"
    PASSWORD_MESSAGE = b"p"
    SYNC = b"S"
    TERMINATE = b"X"


def _time_to_microseconds(t):
    # Convert hours, minutes, seconds, and microseconds to microseconds
    hours_to_microseconds = t.hour * 60 * 60 * 1e6
    minutes_to_microseconds = t.minute * 60 * 1e6
    seconds_to_microseconds = t.second * 1e6
    microseconds = t.microsecond
    total_microseconds = (
            hours_to_microseconds
            + minutes_to_microseconds
            + seconds_to_microseconds
            + microseconds
    )
    return int(total_microseconds)


def _micros_since_2000(dt):
    micros = (
                     dt - datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
             ).total_seconds() * 1000000
    return int(micros)


def _arrow_type_to_bvtype(arrow_type: pyarrow.DataType) -> PyPgType:
    """Convert PyArrow type to BVType for PostgreSQL compatibility."""
    if pyarrow.types.is_null(arrow_type):
        return PyPgType.NULL
    elif pyarrow.types.is_boolean(arrow_type):
        return PyPgType.BOOL
    elif pyarrow.types.is_integer(arrow_type):
        if arrow_type.bit_width <= 32:
            return PyPgType.INTEGER
        else:
            return PyPgType.BIGINT
    elif pyarrow.types.is_floating(arrow_type):
        return PyPgType.FLOAT
    elif pyarrow.types.is_decimal(arrow_type):
        return PyPgType.DECIMAL
    elif pyarrow.types.is_string(arrow_type) or pyarrow.types.is_large_string(arrow_type):
        return PyPgType.TEXT
    elif pyarrow.types.is_binary(arrow_type) or pyarrow.types.is_large_binary(arrow_type):
        return PyPgType.BYTES
    elif pyarrow.types.is_date(arrow_type):
        return PyPgType.DATE
    elif pyarrow.types.is_time(arrow_type):
        return PyPgType.TIME
    elif pyarrow.types.is_timestamp(arrow_type):
        return PyPgType.TIMESTAMP
    elif pyarrow.types.is_duration(arrow_type):
        return PyPgType.INTERVAL
    elif pyarrow.types.is_list(arrow_type) or pyarrow.types.is_large_list(arrow_type):
        # Check if it's a list of integers or strings
        value_type = arrow_type.value_type
        if pyarrow.types.is_integer(value_type):
            return PyPgType.INTEGERARRAY
        elif pyarrow.types.is_string(value_type):
            return PyPgType.STRINGARRAY
        else:
            return PyPgType.ARRAY
    elif pyarrow.types.is_struct(arrow_type) or pyarrow.types.is_map(arrow_type):
        return PyPgType.JSON
    else:
        # Default to TEXT for unknown types
        return PyPgType.TEXT


def _get_status(query_result) -> str:
    """Get status from query result, handling both PyArrow Tables and DuckDBQueryResult objects."""
    if hasattr(query_result, 'schema') and hasattr(query_result, 'num_columns'):
        # PyArrow Table - return a default SELECT status
        return "SELECT"
    elif hasattr(query_result, 'status'):
        # DuckDBQueryResult or similar custom objects
        return query_result.status()
    else:
        # Fallback
        return "UNKNOWN"


PG_UNKNOWN = (705, str)
BVTYPE_TO_PGTYPE = {
    PyPgType.NULL: (-1, lambda v: None),
    PyPgType.ARRAY: (2277, lambda v: "{" + ",".join(v) + "}", None),
    PyPgType.BIGINT: (20, str, lambda r: int.to_bytes(r, 8, "big")),
    PyPgType.BOOL: (
        16,
        lambda v: "true" if v else "false",
        lambda r: b"\x01" if r else b"\x00",
    ),
    PyPgType.BYTES: (17, lambda v: "\\x" + v.hex(), lambda r: r),
    PyPgType.DATE: (
        1082,
        lambda v: v.isoformat(),
        lambda r: int.to_bytes((r.toordinal() - 730120), 4, "big"),
    ),
    PyPgType.DECIMAL: (1700, str),
    PyPgType.FLOAT: (701, str, lambda r: struct.pack("!d", r)),
    PyPgType.INTEGER: (23, str, lambda r: int.to_bytes(r, 4, "big")),
    PyPgType.INTEGERARRAY: (1007, lambda v: "{" + ",".join(str(x) for x in v) + "}"),
    PyPgType.INTERVAL: (
        1186,
        lambda v: f"{v.days} days {v.seconds} seconds {v.microseconds} microseconds",
    ),
    PyPgType.JSON: (114, lambda v: json.dumps(v)),
    PyPgType.STRINGARRAY: (1009, lambda v: "{" + ",".join(v) + "}"),
    PyPgType.TEXT: (25, str, lambda r: r.encode("utf-8")),
    PyPgType.TIME: (
        1083,
        lambda v: v.isoformat(),
        lambda r: int.to_bytes(_time_to_microseconds(r), 8, "big"),
    ),
    PyPgType.TIMESTAMP: (
        1114,
        lambda v: v.isoformat().replace("T", " "),
        lambda r: int.to_bytes(_micros_since_2000(r), 8, "big"),
    ),
}


class BVBuffer(object):
    """A helper for reading and writing bytes in the format the PG wire protocol expects."""

    def __init__(self, stream=None):
        if not stream:
            stream = io.BytesIO()
        self.stream = stream

    def read_bytes(self, n) -> bytes:
        return self.stream.read(n)

    def read_byte(self) -> bytes:
        return self.read_bytes(1)

    def read_int16(self) -> int:
        data = self.read_bytes(2)
        return struct.unpack("!h", data)[0]

    def read_uint32(self) -> int:
        data = self.read_bytes(4)
        return struct.unpack("!I", data)[0]

    def read_int32(self) -> int:
        data = self.read_bytes(4)
        return struct.unpack("!i", data)[0]

    def write_bytes(self, value: bytes):
        self.stream.write(value)

    def write_byte(self, value):
        self.stream.write(struct.pack("!c", value))

    def write_int16(self, value: int):
        self.stream.write(struct.pack("!h", value))

    def write_int32(self, value: int):
        self.stream.write(struct.pack("!i", value))

    def write_string(self, value):
        self.stream.write(value.encode() if isinstance(value, str) else value)
        self.stream.write(b"\x00")

    def get_value(self) -> bytes:
        return self.stream.getvalue()


class AsyncBVBuffer(object):
    """Async version of BVBuffer for reading from asyncio streams."""

    def __init__(self, reader: asyncio.StreamReader):
        self.reader = reader

    async def read_bytes(self, n) -> bytes:
        data = await self.reader.readexactly(n)
        return data

    async def read_byte(self) -> bytes:
        return await self.read_bytes(1)

    async def read_int16(self) -> int:
        data = await self.read_bytes(2)
        return struct.unpack("!h", data)[0]

    async def read_uint32(self) -> int:
        data = await self.read_bytes(4)
        return struct.unpack("!I", data)[0]

    async def read_int32(self) -> int:
        data = await self.read_bytes(4)
        return struct.unpack("!i", data)[0]


class PGContext:
    def __init__(
            self, session: UniverSQLSession, params: Dict[str, str]
    ):
        self.session = session
        self.params = params
        self.process_id = random.randint(0, 2 ** 32 - 1)
        self.secret_key = random.randint(0, 2 ** 32 - 1)
        self.stmts = {}
        self.portals = {}
        self.result_cache = {}
        self.has_error = False
        self.authenticated = False
        self.salt = None
        self.auth_type = None  # Track what type of authentication we requested

    def get_hashed_password(self, user: str, password: str) -> str:
        first = hashlib.md5(password.encode("utf-8") + user.encode("utf-8")).hexdigest()
        return "md5" + hashlib.md5(first.encode("utf-8") + self.salt).hexdigest()

    def mark_error(self):
        self.has_error = True

    def transaction_status(self):
        if self.session.in_transaction():
            if self.has_error:
                return TransactionStatus.IN_FAILED_TRANSACTION
            else:
                return TransactionStatus.IN_TRANSACTION
        return TransactionStatus.IDLE

    def describe_portal(self, name: str):
        stmt, params, result_fmt = self.portals[name]
        sql, param_oids = self.stmts[stmt]
        final_sql = self._substitute_parameters(sql, params)
        query_result = self._route_query(final_sql)
        logger.debug(f"Describe portal: {final_sql}, result: {query_result}")
        self.result_cache[name] = query_result
        return query_result

    def describe_statement(self, name: str):
        sql, param_oids = self.stmts[name]
        params = []
        for typeoid in param_oids:
            type = TYPE_OIDS.get(typeoid)
            if type:
                params.append(type[2])
            else:
                raise Exception(f"Unsupported parameter type: {typeoid}")
        return self._route_query(sql)

    def execute_portal(self, name: str):
        if name in self.result_cache:
            query_result = self.result_cache[name]
            del self.result_cache[name]
            return query_result
        else:
            stmt, params, result_fmt = self.portals[name]
            sql, param_oids = self.stmts[stmt]
            # Substitute parameters in SQL
            final_sql = self._substitute_parameters(sql, params)
            qr = self._route_query(final_sql)
            return qr

    def add_statement(self, name: str, sql: str, param_oids: List[int]):
        self.stmts[name] = (sql, param_oids)

    def close_statement(self, name: str):
        del self.stmts[name]

    def add_portal(
            self, name: str, stmt: str, params: Dict[str, str], result_formats: List[int]
    ):
        self.portals[name] = (stmt, params, result_formats)

    def close_portal(self, name: str):
        del self.portals[name]

    def flush(self):
        pass

    def sync(self):
        if self.has_error:
            self.has_error = False
            # If we're in a transaction and had an error, rollback the transaction
            if self.session.in_transaction():
                logger.debug(f"[{self.session.session_id}] Auto-rolling back failed transaction on SYNC")
                self.session.set_transaction_state(False)

    def _substitute_parameters(self, sql: str, params: List) -> str:
        """Substitute $1, $2, etc. parameters with actual values."""
        if not params:
            return sql
        
        final_sql = sql
        for i, param in enumerate(params):
            placeholder = f"${i + 1}"
            # Simple parameter substitution - escape single quotes and wrap in quotes
            if param is None:
                param_str = "NULL"
            elif isinstance(param, str):
                # Escape single quotes and wrap in quotes
                escaped_param = param.replace("'", "''")
                param_str = f"'{escaped_param}'"
            elif isinstance(param, (int, float)):
                param_str = str(param)
            elif isinstance(param, bool):
                param_str = "TRUE" if param else "FALSE"
            else:
                # Default: convert to string and quote
                escaped_param = str(param).replace("'", "''")
                param_str = f"'{escaped_param}'"
            
            final_sql = final_sql.replace(placeholder, param_str)
        
        return final_sql

    async def _route_query(self, sql: str) -> pyarrow.Table:
        """Route query through websocket if available, otherwise execute directly."""
        from universql.protocol.session import sessions as global_sessions
        
        # Debug: Log credentials being used for websocket lookup
        logger.debug(f"[{self.session.session_id}] Looking for websocket with credentials: {self.session.credentials}")
        
        # Check if there's a linked websocket for this session
        websocket = global_sessions.find_websocket_by_credentials(self.session.credentials)
        
        logger.debug(f"[{self.session.session_id}] Found websocket: {websocket is not None}")
        
        if websocket:
            logger.info(f"[{self.session.session_id}] 🚀 Routing query through WebSocket")
            try:
                await websocket.send_json({
                    "type": "query_routed",
                    "sql": sql,
                    "session_id": self.session.session_id
                })
                logger.info(f"[{self.session.session_id}] ✅ Notified WebSocket of query: {sql[:50]}...") 
            except Exception as e:
                logger.warning(f"[{self.session.session_id}] WebSocket notification failed: {e}")
        else:
            logger.debug(f"[{self.session.session_id}] No websocket found, executing directly")
        
        return self.session.do_query(sql)


async def send_row_description(query_result, writer: asyncio.StreamWriter):
    buf = BVBuffer()

    # Handle PyArrow Table
    if hasattr(query_result, 'schema') and hasattr(query_result, 'num_columns'):
        num_columns = query_result.num_columns
        for i in range(num_columns):
            field = query_result.schema.field(i)
            name = field.name
            bvtype = _arrow_type_to_bvtype(field.type)
            oid = BVTYPE_TO_PGTYPE.get(bvtype, PG_UNKNOWN)[0]
            buf.write_string(name)
            fmt = 0  # Default to text format for PyArrow tables
            buf.write_bytes(struct.pack("!ihihih", 0, 0, oid, 0, -1, fmt))
    else:
        # Handle DuckDBQueryResult or similar objects
        num_columns = query_result.column_count()
        for i in range(num_columns):
            name, bvtype = query_result.column(i)
            oid = BVTYPE_TO_PGTYPE.get(bvtype, PG_UNKNOWN)[0]
            buf.write_string(name)
            fmt = query_result.result_format[i] if query_result.result_format else 0
            buf.write_bytes(struct.pack("!ihihih", 0, 0, oid, 0, -1, fmt))

    out = buf.get_value()
    sig = struct.pack(
        "!cih",
        ServerResponse.ROW_DESCRIPTION,
        len(out) + 6,
        num_columns,
    )
    writer.write(sig + out)
    await writer.drain()


async def send_data_rows(query_result: pyarrow.Table, writer: asyncio.StreamWriter, limit: int = 0) -> int:
    cnt = 0
    converters = []

    num_columns = query_result.num_columns
    num_rows = query_result.num_rows

    # Setup converters for PyArrow table
    for i in range(num_columns):
        field = query_result.schema.field(i)
        bvtype = _arrow_type_to_bvtype(field.type)
        pgtype = BVTYPE_TO_PGTYPE.get(bvtype, PG_UNKNOWN)
        # Default to text format for PyArrow tables
        converters.append((pgtype[1], True))

    # Convert to row-wise format
    for row_idx in range(num_rows):
        buf = BVBuffer()
        for col_idx in range(num_columns):
            column_array = query_result.column(col_idx)
            r = column_array[row_idx].as_py()  # Convert PyArrow scalar to Python value

            if r is None:
                buf.write_int32(-1)
            else:
                converter, do_encode = converters[col_idx]
                v = converter(r)
                if do_encode:
                    v = v.encode("utf-8")
                buf.write_int32(len(v))
                buf.write_bytes(v)

        # Write the complete row after processing all columns
        out = buf.get_value()
        row_sig = struct.pack(
            "!cih",
            ServerResponse.DATA_ROW,
            len(out) + 6,
            num_columns,
        )
        writer.write(row_sig + out)
        cnt += 1
        if limit > 0 and cnt >= limit:
            break
    await writer.drain()
    return cnt


async def send_parameter_description(param_oids: List[int], writer: asyncio.StreamWriter):
    buf = BVBuffer()
    for oid in param_oids:
        buf.write_int32(oid)
    out = buf.get_value()
    sig = struct.pack(
        "!cih",
        ServerResponse.PARAMETER_DESCRIPTION,
        len(out) + 6,
        len(param_oids),
    )
    writer.write(sig + out)
    await writer.drain()


async def send_error(exception, writer: asyncio.StreamWriter, ctx: Optional[PGContext] = None):
    estr = str(exception)
    logger.error(estr)
    buf = BVBuffer()
    buf.write_byte(b"M")
    buf.write_string(estr)
    buf.write_byte(NULL_BYTE)
    out = buf.get_value()
    err_sig = struct.pack("!ci", ServerResponse.ERROR_RESPONSE, len(out) + 4)
    writer.write(err_sig + out)
    await writer.drain()
    if ctx:
        ctx.mark_error()

class PostgresqlHandler:
    """Async PostgreSQL protocol handler."""

    def __init__(self, server: 'PostgresqlServer'):
        self.server = server

    def _create_sni_callback(self):
        """Create SNI callback that stores server name in SSL context."""
        def sni_callback(ssl_sock, server_name, ssl_ctx):
            if server_name:
                logger.info("Client requested host via SNI: %s", server_name)
                server_name_str = server_name.decode('utf-8') if isinstance(server_name, bytes) else server_name
                ssl_ctx.sni_server_name = server_name_str
            else:
                ssl_ctx.sni_server_name = None
        return sni_callback

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a single PostgreSQL client connection."""
        client_socket = writer.get_extra_info('socket')
        client_host, client_port = client_socket.getpeername()[:2]
        logger.info(f"Client connected from: {client_host}:{client_port}")
        
        client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        
        r = AsyncBVBuffer(reader)
        ctx = None
        try:
            ctx, r, writer = await self.handle_startup(r, writer)
            if ctx:
                self.server.ctxts[ctx.process_id] = ctx
                logger.debug(f"Connection established for user '{ctx.session.credentials.get('user')}', waiting for messages...")
            
            while ctx:
                try:
                    # Add timeout for reading messages to prevent hanging
                    logger.debug("Waiting for next message from client...")
                    if not ctx.authenticated:
                        # Shorter timeout during authentication phase (30 seconds)
                        timeout = 30.0
                        logger.debug("Authentication phase - using 30s timeout")
                    else:
                        # Longer timeout for normal operation (60 seconds)
                        timeout = 60.0
                    
                    type_code = await asyncio.wait_for(r.read_byte(), timeout=timeout)
                    logger.debug(f"Received message type: {type_code}")
                except asyncio.TimeoutError:
                    if not ctx.authenticated:
                        logger.error(f"Authentication timeout for user '{ctx.session.credentials.get('user')}' - client did not send expected response")
                        # Send authentication timeout error
                        await self.send_error("28000", "Authentication timeout - client did not respond", writer)
                    else:
                        logger.warning(f"Message timeout for user '{ctx.session.credentials.get('user')}'")
                    break
                except asyncio.IncompleteReadError:
                    # Client disconnected
                    logger.debug("Client disconnected (IncompleteReadError)")
                    break
                except Exception as e:
                    logger.error(f"Error reading type code: {e}")
                    break
                    
                if not type_code or type_code == ClientCommand.TERMINATE:
                    # we're done
                    logger.debug("Client terminated connection")
                    break

                try:
                    logger.debug(f"Reading message length for type: {type_code}")
                    msglen = await asyncio.wait_for(r.read_uint32(), timeout=30.0)
                    logger.debug(f"Message length: {msglen}")
                    
                    if msglen > 4:
                        logger.debug(f"Reading payload of {msglen - 4} bytes")
                        payload = await asyncio.wait_for(r.read_bytes(msglen - 4), timeout=30.0)
                        logger.debug(f"Payload read successfully, length: {len(payload) if payload else 0}")
                    else:
                        payload = None
                        logger.debug("No payload for this message")
                except asyncio.TimeoutError:
                    logger.error(f"Timeout reading message length/payload for type: {type_code}")
                    break
                except Exception as e:
                    logger.error(f"Error reading message length/payload: {e}")
                    break

                logger.debug(f"Processing message type: {type_code}, length: {msglen}, authenticated: {ctx.authenticated}")

                try:
                    if not ctx.authenticated:
                        if type_code == ClientCommand.PASSWORD_MESSAGE:
                            # Check what type of authentication we requested
                            logger.debug(f"Received password message, auth_type: {ctx.auth_type}")
                            if ctx.auth_type == 'plain':
                                # We requested plain-text password
                                logger.debug("Processing plain-text password")
                                await self.handle_plain_password(ctx, payload, writer)
                            else:
                                # We requested MD5 password (or default)
                                logger.debug("Processing MD5 password")
                                await self.handle_md5_password(ctx, payload, writer)
                        else:
                            logger.error(f"Not authenticated, received unexpected message type: {type_code} (expected {ClientCommand.PASSWORD_MESSAGE})")
                            # Send error response for unexpected message
                            await self.send_error("28000", f"Authentication required, but received unexpected message type: {type_code}", writer)
                            break
                    elif type_code == ClientCommand.QUERY:
                        await self.handle_query(ctx, payload, writer)
                    elif type_code == ClientCommand.PARSE:
                        await self.handle_parse(ctx, payload, writer)
                    elif type_code == ClientCommand.BIND:
                        await self.handle_bind(ctx, payload, writer)
                    elif type_code == ClientCommand.DESCRIBE:
                        await self.handle_describe(ctx, payload, writer)
                    elif type_code == ClientCommand.EXECUTE:
                        await self.handle_execute(ctx, payload, writer)
                    elif type_code == ClientCommand.CLOSE:
                        await self.handle_close(ctx, payload, writer)
                    elif type_code == ClientCommand.SYNC:
                        logger.debug("Handling SYNC command")
                        ctx.sync()
                        await self.send_ready_for_query(ctx, writer)
                    elif type_code == ClientCommand.FLUSH:
                        logger.debug("Handling FLUSH command")
                        ctx.flush()
                    else:
                        logger.error(f"Unknown message type_code: {type_code}")
                        await self.send_error("08P01", f"Unknown message type: {type_code}", writer)
                        break
                except Exception as e:
                    logger.exception(f"Error processing message type {type_code}: {e}")
                    await send_error(e, writer, ctx)
                    # Don't break here unless it's a critical error
        except Exception as e:
            logger.exception(f"Critical error in connection handling: {e}")
            await send_error(e, writer)
        finally:
            logger.debug(f"Cleaning up connection for user: {ctx.session.credentials.get('user') if ctx else 'unknown'}")
            if ctx:
                self.server.sessions.close_session(ctx.session.session_id)
                if ctx.process_id in self.server.ctxts:
                    del self.server.ctxts[ctx.process_id]
            writer.close()
            await writer.wait_closed()
            logger.debug("Connection cleanup completed")

    async def handle_startup(self, r: AsyncBVBuffer, writer: asyncio.StreamWriter):
        msglen = await r.read_uint32() - 4
        code = await r.read_uint32()
        if code == 80877103:  ## SSL request
            if self.server.ssl_certfile and self.server.ssl_keyfile:
                writer.write(b'S')
                await writer.drain()
                logger.debug("SSL request accepted - upgrading connection to SSL")
                
                # Create SSL context for the upgrade
                ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                ssl_context.set_servername_callback(self._create_sni_callback())
                
                # Check if we're using localhost computing and have embedded certificates
                is_localhost = (
                    self.server.host in ['localhostcomputing.com', 'localhost', '127.0.0.1', '::1', '0.0.0.0'] or
                    self.server.host.endswith('.localhostcomputing.com')
                )
                
                if is_localhost and LOCALHOST_UNIVERSQL_COM_BYTES:
                    logger.debug("Using embedded SSL certificates for localhost computing")
                    # Decode the embedded certificates
                    cert_data = base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['cert'])
                    key_data = base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['key'])
                    
                    # Create temporary files for the certificates
                    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.pem') as cert_file:
                        cert_file.write(cert_data)
                        cert_file_path = cert_file.name
                    
                    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.key') as key_file:
                        key_file.write(key_data)
                        key_file_path = key_file.name
                    
                    try:
                        ssl_context.load_cert_chain(cert_file_path, key_file_path)
                    finally:
                        # Clean up temporary files
                        try:
                            os.unlink(cert_file_path)
                            os.unlink(key_file_path)
                        except:
                            pass
                else:
                    # Use provided certificate files
                    ssl_context.load_cert_chain(self.server.ssl_certfile, self.server.ssl_keyfile)
                
                # Get the current transport and protocol
                transport = writer.transport
                current_protocol = transport.get_protocol()
                loop = asyncio.get_event_loop()
                
                try:
                    # Create a new reader for the SSL connection
                    new_reader = asyncio.StreamReader()
                    new_protocol = asyncio.StreamReaderProtocol(new_reader)
                    
                    # Upgrade the connection to SSL
                    ssl_transport = await loop.start_tls(
                        transport, 
                        new_protocol, 
                        ssl_context, 
                        server_side=True
                    )
                    
                    # Create new writer for the SSL connection  
                    new_writer = asyncio.StreamWriter(ssl_transport, new_protocol, new_reader, loop)
                    
                    # Update our local references
                    writer = new_writer
                    r = AsyncBVBuffer(new_reader)
                    
                    logger.debug("SSL upgrade completed successfully")
                    
                except Exception as e:
                    logger.error(f"SSL upgrade failed: {e}")
                    # Close the original connection on SSL upgrade failure
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except:
                        pass
                    return None, r, writer
            else:
                # Reject SSL
                writer.write(b'N')
                await writer.drain()
                logger.debug("SSL request rejected - server has no SSL configuration")
            
            # Recursively handle the next startup message (should be the actual startup)
            return await self.handle_startup(r, writer)
            
        elif code == 196608:  # Protocol 3.0
            msg = [
                x.decode("utf-8")
                for x in (await r.read_bytes(msglen - 4)).split(NULL_BYTE)
            ]
            params = dict(zip(msg[::2], msg[1::2]))
            logger.info("Client connection params: %s", params)

            from universql.util import current_context

            # Extract SNI server name from SSL transport if available
            sni_server_name = None
            if hasattr(writer, 'transport') and hasattr(writer.transport, 'get_extra_info'):
                ssl_object = writer.transport.get_extra_info('ssl_object')
                if ssl_object and hasattr(ssl_object, 'context') and hasattr(ssl_object.context, 'sni_server_name'):
                    sni_server_name = ssl_object.context.sni_server_name

            # Extract PostgreSQL connection parameters from the parsed URL
            credentials = {"server_id": sni_server_name}
            
            # Get URL credentials if available
            url_credentials = current_context.get('url')
            if url_credentials:
                # If url_credentials is a string, parse it; otherwise assume it's already a dict-like object
                from urllib.parse import urlparse, parse_qs

                if isinstance(url_credentials, str):
                    parsed = urlparse(url_credentials)
                    if parsed.scheme and parsed.scheme.startswith('postgres'):
                        if parsed.hostname:
                            credentials['host'] = parsed.hostname
                        if parsed.port:
                            credentials['port'] = parsed.port
                        if parsed.path and len(parsed.path) > 1:
                            credentials['database'] = parsed.path.lstrip('/')
                        if parsed.username:
                            credentials['user'] = parsed.username
                        if parsed.password:
                            credentials['password'] = parsed.password

                        # Extract query params such as sslmode
                        qparams = parse_qs(parsed.query)
                        for k, v in qparams.items():
                            if v:
                                credentials[k] = v[0]
                else:
                    # Assume dict-like
                    pg_params = ['host', 'port', 'database', 'user', 'password', 'sslmode', 'sslcert', 'sslkey', 'sslrootcert']
                    for key in pg_params:
                        if key in url_credentials and url_credentials[key] is not None:
                            credentials[key] = url_credentials[key]


            if "application_name" in params:
                credentials["application_name"] = params["application_name"]

            if "user" in params:
                credentials["user"] = params["user"]
            if "database" in params:
                credentials["database"] = params["database"]

            logger.debug(f"🔍 PostgreSQL startup looking for session with credentials: {credentials}")
            session = self.server.sessions.find_session_by_credentials(sni_server_name, credentials)
            logger.debug(f"🔍 find_session_by_credentials returned: {session.session_id if session else None}")

            if not session:
                import uuid as _uuid
                session = UniverSQLSession(
                    context=current_context,
                    session_id=str(_uuid.uuid4()),
                    base_engine="postgresql",
                    target_engine="websocket",  
                    credentials=credentials,
                    session_parameters={},
                )
                self.server.sessions.register_session(session)
                logger.debug(f"🆕 Created new PostgreSQL session: {session.session_id}")
            else:
                logger.debug(f"✅ Found session via credentials: {session.session_id}, base_engine: {session.base_engine}, context keys: {list(session.context.keys())}")

            ctx = PGContext(session, params)
            await self.send_authentication_md5(sni_server_name, ctx, writer)
            return ctx, r, writer
            
        elif code == 80877102:  ## Cancel request
            process_id, secret_key = await r.read_uint32(), await r.read_uint32()
            ctx = self.server.ctxts.get(process_id)
            if ctx and ctx.secret_key == secret_key:
                self.server.sessions.close_session(ctx.session.session_id)
                del self.server.ctxts[ctx.process_id]
            return None, r, writer
        elif code == 796357408:  ## HTTP:
            raise Exception(f"HTTP request received")
        else:
            raise Exception(f"Unsupported startup message: {code}")

    async def send_authentication_ok(self, writer: asyncio.StreamWriter):
        writer.write(
            struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 0)
        )
        await writer.drain()

    async def send_authentication_md5(self, server_id: str, ctx: PGContext, writer: asyncio.StreamWriter):
        """Decide between requesting MD5 or plain-text password based on the global session registry."""
        # Request plain-text authentication if we don't already have a password for this session/user.
        request_plain = ctx.session.credentials.get('password') is None
        
        # Debug logging - use proper Sessions lookup with server_id and credentials
        user = ctx.session.credentials.get('user')
        
        existing_session = self.server.sessions.find_session_by_credentials(server_id, ctx.session.credentials)
        logger.debug(f"🔐 Auth decision for user '{user}': request_plain={request_plain}, existing_session={existing_session is not None}")
        if existing_session:
            logger.debug(f"🔐 Existing session has password: {existing_session.credentials.get('password') is not None}")

        if request_plain:
            # Request plain-text password (authentication type 3)
            ctx.auth_type = 'plain'
            writer.write(struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 3))
            logger.info(
                "🔐 Requesting plain-text auth for user '%s' (existing session needs password)",
                ctx.session.credentials.get('user'),
            )
            logger.debug("Plain-text auth request sent, waiting for password message...")
        else:
            # Default to MD5 authentication (more compatible with standard clients)
            ctx.auth_type = 'md5'
            ctx.salt = os.urandom(4)
            writer.write(struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 12, 5))
            writer.write(ctx.salt)
            logger.info(
                "🔐 Using MD5 auth for user '%s' (standard PostgreSQL authentication)",
                ctx.session.credentials.get('user'),
            )
            logger.debug("MD5 auth request sent, waiting for password message...")

        await writer.drain()
        logger.debug("Authentication request flushed to client")

    async def handle_md5_password(self, ctx: PGContext, payload: bytes, writer: asyncio.StreamWriter):
        logger.debug(f"Handling MD5 password, payload length: {len(payload) if payload else 0}")
        
        # Extract the password from the client request
        client_password_hash = payload.decode("utf-8").rstrip("\x00")
        
        logger.debug(f"Password hash captured from client authentication: '{client_password_hash}'")
        
        # This is a limitation - MD5 auth doesn't give us the plain password
        logger.warning("🔐 MD5 authentication used - Password not stored in session")
        
        # Accept the authentication at proxy level
        logger.debug("Sending authentication OK for MD5")
        await self.send_authentication_ok(writer)
        logger.debug("Authentication OK sent, starting post-auth for MD5")
        await self.handle_post_auth(ctx, writer)
        logger.debug("Post-auth completed successfully for MD5")

    async def handle_plain_password(self, ctx: PGContext, payload: bytes, writer: asyncio.StreamWriter):
        logger.debug(f"Handling plain password, payload length: {len(payload) if payload else 0}")
        
        try:
            # Extract plain-text password from client request
            plain_password = payload.decode("utf-8").rstrip("\x00")
            logger.debug(f"Extracted password from client (length: {len(plain_password)})")
            
            # Store the plain password for backend authentication
            # This should override any URL-provided password
            ctx.session.credentials['password'] = plain_password
            logger.debug(f"Plain-text password captured from client authentication")
            logger.info(f"🔐 Updated credentials with client password for user='{ctx.session.credentials.get('user')}'")
            
            # Accept the authentication at proxy level
            logger.debug("Sending authentication OK for plain-text")
            await self.send_authentication_ok(writer)
            logger.debug("Authentication OK sent, starting post-auth for plain-text")
            await self.handle_post_auth(ctx, writer)
            logger.debug("Post-auth completed successfully for plain-text")
            
        except Exception as e:
            logger.error(f"Error in handle_plain_password: {e}")
            await self.send_error("28000", f"Authentication failed: {e}", writer)
            raise

    async def handle_post_auth(self, ctx: PGContext, writer: asyncio.StreamWriter):
        logger.debug("Starting post-auth: sending parameter status messages")
        # Send parameter status messages that JDBC clients expect to prevent client info errors
        await self.send_parameter_status({
            "server_version": "16.0",  # Use a realistic PostgreSQL version
            "server_encoding": "UTF8",
            "client_encoding": "UTF8",
            "DateStyle": "ISO, MDY",
            "TimeZone": "UTC",
            "integer_datetimes": "on",
            "IntervalStyle": "postgres",
            "application_name": ctx.session.credentials.get('application_name', 'universql'),
            "is_superuser": "off",
            "session_authorization": ctx.session.credentials.get('user', 'universql'),
            "standard_conforming_strings": "on",
            # Additional parameters that DBeaver expects
            "search_path": '"$user", public',
            "default_tablespace": "",
            "default_transaction_isolation": "read committed",
            "default_transaction_read_only": "off",
            "default_transaction_deferrable": "off",
            "statement_timeout": "0",
            "lock_timeout": "0",
            "idle_in_transaction_session_timeout": "0",
            "row_security": "off",
            "lc_collate": "en_US.UTF-8",
            "lc_ctype": "en_US.UTF-8",
            "lc_messages": "en_US.UTF-8",
            "lc_monetary": "en_US.UTF-8",
            "lc_numeric": "en_US.UTF-8",
            "lc_time": "en_US.UTF-8",
            "shared_preload_libraries": "",
            "dynamic_shared_memory_type": "posix",
            "wal_level": "replica",
            "max_wal_senders": "10",
            "max_replication_slots": "10",
            "wal_keep_segments": "0",
            "wal_sender_timeout": "60s",
            "max_connections": "100",
        }, writer)
        logger.debug("Parameter status messages sent, sending backend key data")
        await self.send_backend_key_data(ctx, writer)
        logger.debug("Backend key data sent, sending ready for query")
        await self.send_ready_for_query(ctx, writer)
        logger.debug("Ready for query sent, setting authenticated=True")
        ctx.authenticated = True
        logger.info(f"🎉 Authentication completed successfully for user '{ctx.session.credentials.get('user')}'")
        return

    async def handle_query(self, ctx: PGContext, payload: bytes, writer: asyncio.StreamWriter):
        logger.debug("Handle query")
        decoded = payload.decode("utf-8").rstrip("\x00")
        logger.debug(f"Executing SQL: {decoded!r}")

        # Check for SET commands that might contain serverid (DBeaver user properties)
        decoded_upper = decoded.strip().upper()
        # Check if transaction is in failed state
        if ctx.has_error and ctx.session.in_transaction():
            # Allow ROLLBACK and COMMIT commands even in failed transaction state
            if not (decoded_upper.startswith('ROLLBACK') or decoded_upper.startswith('COMMIT') or decoded_upper.startswith('END')):
                error_msg = "current transaction is aborted, commands ignored until end of transaction block"
                await self.send_error("25P02", error_msg, writer)
                await self.send_ready_for_query(ctx, writer)
                return

        # Auto-start transaction for queries that need it (but not for transaction control commands)
        should_auto_start_transaction = (
            not ctx.session.in_transaction() and 
            not any(decoded_upper.startswith(cmd) for cmd in ['BEGIN', 'START', 'COMMIT', 'ROLLBACK', 'END', 'SET '])
        )
        
        if should_auto_start_transaction:
            logger.debug(f"[{ctx.session.session_id}] Auto-starting transaction for query")
            ctx.session.set_transaction_state(True)

        try:
            # Execute query in a thread pool to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            
            async def execute_query():
                try:
                    result: pyarrow.Table = await ctx._route_query(decoded)
                    return result
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    raise
            
            # Add timeout to prevent DBeaver from hanging on slow queries
            query_result : pyarrow.Table = await asyncio.wait_for(
                loop.run_in_executor(None, execute_query),
                timeout=300.0  # 5 minute timeout
            )
            logger.debug(f"Query result type: {type(query_result)}")
        except asyncio.TimeoutError:
            logger.error(f"Query timed out after 300 seconds: {decoded!r}")
            await send_error(Exception("Query timed out after 300 seconds"), writer, ctx)
            await self.send_ready_for_query(ctx, writer)
            return
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            await send_error(e, writer, ctx)
            await self.send_ready_for_query(ctx, writer)
            return

        if query_result.num_rows > 0:
            logger.debug("Query has results, sending row description")
            await send_row_description(query_result, writer)
            logger.debug("Sending data rows")
            row_count = await send_data_rows(query_result, writer)
            logger.debug(f"Sent {row_count} rows, sending command complete")
            await self.send_command_complete("SELECT %d\x00" % row_count, writer)
        else:
            status = _get_status(query_result)
            logger.debug(f"Query has no results, status: {status}")
            await self.send_command_complete(f"{status}\x00", writer)
        await self.send_ready_for_query(ctx, writer)

    async def handle_parse(self, ctx: PGContext, payload: bytes, writer: asyncio.StreamWriter):
        logger.debug("Handling parse")
        ba = bytearray(payload)
        stmt_idx = ba.index(NULL_BYTE)
        stmt = ba[:stmt_idx].decode("utf-8")
        query_idx = ba.index(NULL_BYTE, stmt_idx + 1)
        sql = ba[stmt_idx + 1: query_idx].decode("utf-8")
        logger.debug("Parsed statement: %s", sql)
        buf = BVBuffer(io.BytesIO(ba[query_idx + 1:]))
        num_params = buf.read_int16()
        param_oids = []
        for i in range(num_params):
            param_oids.append(buf.read_int32())
        ctx.add_statement(stmt, sql, param_oids)
        await self.send_parse_complete(writer)

    async def handle_bind(self, ctx: PGContext, payload: bytes, writer: asyncio.StreamWriter):
        logger.debug("Handling bind")
        ba = bytearray(payload)
        portal_idx = ba.index(NULL_BYTE)
        portal = ba[:portal_idx].decode("utf-8")
        stmt_idx = ba.index(NULL_BYTE, portal_idx + 1)
        stmt = ba[portal_idx + 1: stmt_idx].decode("utf-8")
        buf = BVBuffer(io.BytesIO(ba[stmt_idx + 1:]))
        # First param format stuff...
        num_formats = buf.read_int16()
        formats = []
        for i in range(num_formats):
            formats.append(buf.read_int16())
        # ... then the actual param values
        num_params = buf.read_int16()
        if num_formats < num_params:
            if formats:
                formats = [formats[0]] * num_params
            else:
                formats = [0] * num_params
        params = []
        for i in range(num_params):
            nb = buf.read_int32()
            v = buf.read_bytes(nb)
            typeoid = ctx.stmts[stmt][1][i]
            logger.debug("Format: %d, Type: %d", formats[i], typeoid)
            if formats[i] == 0:
                decoded = v.decode("utf-8")
                if typeoid == 0:
                    # strangely this is set to 0 for dates and times
                    params.append(dateutil.parser.parse(decoded))
                else:
                    if decoded.startswith("{") and decoded.endswith("}"):
                        params.append(decoded[1:-1].split(","))
                    else:
                        params.append(decoded)
            else:
                # see Postgres pg_type_d.h for type OIDs
                type = TYPE_OIDS.get(typeoid)
                if type:
                    params.append(struct.unpack(type[1], v)[0])
                else:
                    raise Exception(f"Unsupported binary parameter type: {typeoid}")

        logger.debug("Bind params: %s", params)
        # now expected result formats
        num_result_formats = buf.read_int16()
        result_formats = []
        for i in range(num_result_formats):
            result_formats.append(buf.read_int16())
        ctx.add_portal(portal, stmt, params, result_formats)
        await self.send_bind_complete(writer)

    async def handle_describe(self, ctx: PGContext, payload: bytes, writer: asyncio.StreamWriter):
        logger.debug("Handling describe")
        ba = bytearray(payload)
        describe_type = ba[0]
        if describe_type == ord("P"):
            portal = ba[1: len(ba) - 1].decode("utf-8")
            try:
                loop = asyncio.get_event_loop()
                query_result = await asyncio.wait_for(
                    loop.run_in_executor(None, ctx.describe_portal, portal),
                    timeout=30.0  # 30 second timeout for describe operations
                )
            except asyncio.TimeoutError:
                await send_error(Exception("Describe operation timed out"), writer, ctx)
                return
            except Exception as e:
                await send_error(e, writer, ctx)
                return
        elif describe_type == ord("S"):
            stmt = ba[1: len(ba) - 1].decode("utf-8")
            try:
                loop = asyncio.get_event_loop()
                query_result = await asyncio.wait_for(
                    loop.run_in_executor(None, ctx.describe_statement, stmt),
                    timeout=30.0  # 30 second timeout for describe operations
                )
            except asyncio.TimeoutError:
                await send_error(Exception("Describe operation timed out"), writer, ctx)
                return
            except Exception as e:
                await send_error(e, writer, ctx)
                return
            param_oids = ctx.stmts[stmt][1]
            await send_parameter_description(param_oids, writer)
        else:
            raise Exception(f"Unknown describe type: {describe_type}")
        if query_result.num_rows > 0:
            await send_row_description(query_result, writer)
        else:
            # Send NoData response when there are no results to describe
            writer.write(struct.pack("!ci", ServerResponse.NO_DATA, 4))
            await writer.drain()

    async def handle_execute(self, ctx: PGContext, payload: bytes, writer: asyncio.StreamWriter):
        logger.debug("Handling execute")
        if ctx.has_error:
            logger.info("Skipping execute due to previous error")
            error_msg = "current transaction is aborted, commands ignored until end of transaction block"
            await self.send_error("25P02",error_msg, writer)
            return
        ba = bytearray(payload)
        portal_idx = ba.index(NULL_BYTE)
        portal = ba[:portal_idx].decode("utf-8")
        limit = struct.unpack("!i", ba[portal_idx + 1: portal_idx + 5])[0]
        query_result = None
        try:
            loop = asyncio.get_event_loop()
            query_result = await asyncio.wait_for(
                loop.run_in_executor(None, ctx.execute_portal, portal),
                timeout=300.0  # 5 minute timeout for execute operations
            )
        except asyncio.TimeoutError:
            await send_error(Exception("Execute operation timed out"), writer, ctx)
            return
        except Exception as e:
            await send_error(e, writer, ctx)
            return
        if query_result.num_rows > 0:
            row_count = await send_data_rows(query_result, writer, limit)
            if limit == 0 or row_count < limit:
                await self.send_command_complete("SELECT %d\x00" % row_count, writer)
            else:
                # add result back to the cache, so they'll
                # be picked up on text call
                ctx.result_cache[portal] = query_result
                await self.send_portal_suspend(writer)

        else:
            status = _get_status(query_result)
            await self.send_command_complete(f"{status}\x00", writer)

    async def handle_close(self, ctx: PGContext, payload: bytes, writer: asyncio.StreamWriter):
        logger.debug("Handling close")
        close_type = payload[0]
        if close_type == ord("S"):
            ctx.close_statement(payload[1:-1].decode("utf-8"))
        elif close_type == ord("P"):
            ctx.close_portal(payload[1:-1].decode("utf-8"))
        else:
            raise Exception(f"Unknown close type: {close_type}")
        await self.send_close_complete(writer)

    async def send_notice(self, writer: asyncio.StreamWriter):
        writer.write(ServerResponse.NOTICE_RESPONSE)
        await writer.drain()

    async def send_backend_key_data(self, ctx, writer: asyncio.StreamWriter):
        writer.write(
            struct.pack(
                "!ciII",
                ServerResponse.BACKEND_KEY_DATA,
                12,
                ctx.process_id,
                ctx.secret_key,
            )
        )
        await writer.drain()

    async def send_ready_for_query(self, ctx: Optional[PGContext], writer: asyncio.StreamWriter):
        logger.debug("Sending ready for query")
        status = ctx.transaction_status() if ctx else TransactionStatus.IDLE
        writer.write(struct.pack("!cic", ServerResponse.READY_FOR_QUERY, 5, status))
        await writer.drain()

    async def send_parameter_status(self, params: Dict[str, str], writer: asyncio.StreamWriter):
        for name, value in params.items():
            buf = BVBuffer()
            buf.write_string(name)
            buf.write_string(value)
            out = buf.get_value()
            psig = struct.pack("!ci", ServerResponse.PARAMETER_STATUS, len(out) + 4)
            writer.write(psig + out)
        await writer.drain()

    async def send_parse_complete(self, writer: asyncio.StreamWriter):
        writer.write(struct.pack("!ci", ServerResponse.PARSE_COMPLETE, 4))
        await writer.drain()

    async def send_bind_complete(self, writer: asyncio.StreamWriter):
        writer.write(struct.pack("!ci", ServerResponse.BIND_COMPLETE, 4))
        await writer.drain()

    async def send_close_complete(self, writer: asyncio.StreamWriter):
        writer.write(struct.pack("!ci", ServerResponse.CLOSE_COMPLETE, 4))
        await writer.drain()

    async def send_portal_suspend(self, writer: asyncio.StreamWriter):
        writer.write(struct.pack("!ci", ServerResponse.PORTAL_SUSPENDED, 4))
        await writer.drain()

    async def send_command_complete(self, tag: str, writer: asyncio.StreamWriter):
        buf = BVBuffer()
        buf.write_bytes(
            struct.pack("!ci", ServerResponse.COMMAND_COMPLETE, len(tag) + 4)
        )
        buf.write_bytes(tag.encode())
        out = buf.get_value()
        writer.write(out)
        await writer.drain()

    async def send_error(self, errorcode: str, message: str, writer: asyncio.StreamWriter):
        buf = BVBuffer()
        # Severity
        buf.write_byte(b"S")
        buf.write_string("ERROR")
        # SQL State (25P02 = in_failed_transaction_state)
        buf.write_byte(b"C")
        buf.write_string(errorcode)
        # Message
        buf.write_byte(b"M")
        buf.write_string(message)
        buf.write_byte(NULL_BYTE)
        
        out = buf.get_value()
        err_sig = struct.pack("!ci", ServerResponse.ERROR_RESPONSE, len(out) + 4)
        writer.write(err_sig + out)
        await writer.drain()


class PostgresqlServer:
    """Async PostgreSQL server using asyncio."""

    def __init__(self, host='0.0.0.0', postgresql_port=5436, api_port=55432, ssl_certfile=None, ssl_keyfile=None):
        self.host = host
        self.port = postgresql_port  # PostgreSQL proxy port
        self.api_port = api_port  # FastAPI port
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.ctxts = {}
        self.handler = PostgresqlHandler(self)
        self.server = None
        self.app = FastAPI()
        self.sessions = global_sessions

    async def start(self):
        """Start the async PostgreSQL server with integrated FastAPI."""
        import uvicorn
        
        # Install all apps (including MCP server)
        for app_to_be_installed in APPS:
            app_to_be_installed(self.app)

        # Start PostgreSQL protocol server (without SSL - we handle SSL upgrade manually)
        self.server = await asyncio.start_server(
            self.handler.handle_connection,
            self.host,
            self.port,
            reuse_address=True,
            reuse_port=True
        )
        
        addr = self.server.sockets[0].getsockname()
        ssl_status = "SSL-capable" if (self.ssl_certfile and self.ssl_keyfile) else "non-SSL"
        logger.info(f"PostgreSQL server started on {ssl_status} {addr[0]}:{addr[1]}")
        
        # Configure uvicorn server for FastAPI
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.api_port,
            loop="asyncio",
            log_level="info"
        )
        fastapi_server = uvicorn.Server(config)
        
        logger.info(f"FastAPI server (MCP endpoints) starting on {self.host}:{self.api_port}")
        
        fastapi_task = asyncio.create_task(fastapi_server.serve())
        
        try:
            async with self.server:
                await self.server.serve_forever()
        finally:
            # Clean up FastAPI server
            fastapi_task.cancel()
            try:
                await fastapi_task
            except asyncio.CancelledError:
                pass
            
    async def stop(self):
        """Stop the async PostgreSQL server."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("PostgreSQL server stopped")

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()