import asyncio
import datetime
import uuid

import dateutil.parser
import hashlib
import io
import json
import logging
import os
import random
import socket
import struct
from typing import Dict, List, Optional

import pyarrow
import sqlglot

from universql.protocol.session import Sessions, UniverSQLSession
from universql.warehouse.postgresql import PyPgType, PostgresqlCatalog

# register all warehouses and plugins
for module in [
    "universql.warehouse.duckdb",
    "universql.warehouse.postgresql",
    "universql.plugins.ui",
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


def _has_results(query_result) -> bool:
    """Check if a query result has results, handling both PyArrow Tables and DuckDBQueryResult objects."""
    if hasattr(query_result, 'schema') and hasattr(query_result, 'num_columns'):
        # PyArrow Table - check if it has columns and meaningful content
        # Empty tables or tables with no columns should not be considered as having results
        return query_result.num_columns > 0 and query_result.num_rows > 0
    elif hasattr(query_result, 'has_results'):
        # DuckDBQueryResult or similar custom objects
        return query_result.has_results()
    else:
        # Fallback - assume it has results
        return True


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


class BVContext:
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
        # todo: parse params? LIMIT 0?
        query_result = self.execute_sql(sql=sql, params=params, result_fmt=result_fmt)
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
        return self.execute_sql(sql, params)

    def execute_portal(self, name: str):
        if name in self.result_cache:
            query_result = self.result_cache[name]
            del self.result_cache[name]
            return query_result
        else:
            stmt, params, result_fmt = self.portals[name]
            sql, param_oids = self.stmts[stmt]
            # parse the params?
            qr = self.execute_sql(sql=sql, params=params, result_fmt=result_fmt)
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

    def execute_sql(self, sql: str, params=None, result_fmt=None):
        """Execute SQL using the UniversSQL session"""
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


async def send_data_rows(query_result, writer: asyncio.StreamWriter, limit: int = 0) -> int:
    cnt = 0
    converters = []

    # Handle PyArrow Table
    if hasattr(query_result, 'schema') and hasattr(query_result, 'num_columns'):
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
    else:
        # Handle DuckDBQueryResult or similar objects
        num_columns = query_result.column_count()
        for i in range(num_columns):
            bvtype = query_result.column(i)[1]
            pgtype = BVTYPE_TO_PGTYPE.get(bvtype, PG_UNKNOWN)
            if not query_result.result_format or query_result.result_format[i] == 0:
                converters.append((pgtype[1], True))
            else:
                converters.append((pgtype[2], False))

        for row in query_result.rows():
            buf = BVBuffer()
            for j in range(num_columns):
                r = row[j]
                if r is None:
                    buf.write_int32(-1)
                else:
                    converter, do_encode = converters[j]
                    v = converter(r)
                    if do_encode:
                        v = v.encode("utf-8")
                    buf.write_int32(len(v))
                    buf.write_bytes(v)
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


async def send_error(exception, writer: asyncio.StreamWriter, ctx: Optional[BVContext] = None):
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

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a single PostgreSQL client connection."""
        # Set TCP_NODELAY for better latency
        writer.get_extra_info('socket').setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        
        r = AsyncBVBuffer(reader)
        ctx = None
        try:
            ctx = await self.handle_startup(r, writer)
            if ctx:
                self.server.ctxts[ctx.process_id] = ctx
            
            while ctx:
                try:
                    type_code = await r.read_byte()
                except asyncio.IncompleteReadError:
                    # Client disconnected
                    break
                    
                if not type_code or type_code == ClientCommand.TERMINATE:
                    # we're done
                    break

                msglen = await r.read_uint32()
                if msglen > 4:
                    payload = await r.read_bytes(msglen - 4)
                else:
                    payload = None

                if not ctx.authenticated:
                    if type_code == ClientCommand.PASSWORD_MESSAGE:
                        # Check what type of authentication we requested
                        if ctx.auth_type == 'plain':
                            # We requested plain-text password
                            await self.handle_plain_password(ctx, payload, writer)
                        else:
                            # We requested MD5 password (or default)
                            await self.handle_md5_password(ctx, payload, writer)
                    else:
                        raise Exception("Not authenticated")
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
                    ctx.sync()
                    await self.send_ready_for_query(ctx, writer)
                elif type_code == ClientCommand.FLUSH:
                    ctx.flush()
                else:
                    raise Exception("Unknown type_code: %s" % type_code)
        except Exception as e:
            logger.exception(e)
            await send_error(e, writer)
        finally:
            if ctx:
                self.server.sessions.close_session(ctx.session.session_id)
                if ctx.process_id in self.server.ctxts:
                    del self.server.ctxts[ctx.process_id]
            writer.close()
            await writer.wait_closed()

    async def handle_startup(self, r: AsyncBVBuffer, writer: asyncio.StreamWriter) -> BVContext:
        msglen = await r.read_uint32() - 4
        code = await r.read_uint32()
        if code == 80877103:  ## SSL request
            await self.send_notice(writer)
            return await self.handle_startup(r, writer)
        elif code == 196608:  # Protocol 3.0
            msg = [
                x.decode("utf-8")
                for x in (await r.read_bytes(msglen - 4)).split(NULL_BYTE)
            ]
            params = dict(zip(msg[::2], msg[1::2]))
            logger.info("Client connection params: %s", params)

            from universql.util import current_context

            # Extract PostgreSQL connection parameters from the parsed URL
            credentials = {}
            
            # Get URL credentials if available
            url_credentials = current_context.get('url')
            if url_credentials:
                # Only include PostgreSQL connection parameters, exclude scheme and other non-DB params
                pg_params = ['host', 'port', 'database', 'user', 'password', 'sslmode', 'sslcert', 'sslkey', 'sslrootcert']
                for key in pg_params:
                    if key in url_credentials and url_credentials[key] is not None:
                        credentials[key] = url_credentials[key]
            
            # Override with client-provided parameters if present
            if "user" in params:
                credentials["user"] = params["user"]
            if "database" in params:
                credentials["database"] = params["database"]
            if "application_name" in params:
                credentials["application_name"] = params["application_name"]

            # Create session with proper context and credentials
            session = UniverSQLSession(current_context, str(uuid.uuid4()), "postgresql", "postgresql", credentials, {})
            self.server.sessions.register_session(session)
            ctx = BVContext(session, params)
            await self.send_authentication_md5(ctx, writer)
            return ctx
        elif code == 80877102:  ## Cancel request
            process_id, secret_key = await r.read_uint32(), await r.read_uint32()
            ctx = self.server.ctxts.get(process_id)
            if ctx and ctx.secret_key == secret_key:
                self.server.sessions.close_session(ctx.session.session_id)
                del self.server.ctxts[ctx.process_id]
            return None
        else:
            raise Exception(f"Unsupported startup message: {code}")

    async def send_authentication_ok(self, writer: asyncio.StreamWriter):
        writer.write(
            struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 0)
        )
        await writer.drain()

    async def send_authentication_md5(self, ctx: BVContext, writer: asyncio.StreamWriter):
        # If we don't have a password from the URL, request plain-text password instead of MD5
        # This allows us to capture the actual password for backend authentication
        if not ctx.session.credentials.get('password'):
            # Request plain-text password (authentication type 3)
            ctx.auth_type = 'plain'
            writer.write(
                struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 3)
            )
        else:
            # We have password from URL, proceed with MD5 authentication
            ctx.auth_type = 'md5'
            ctx.salt = os.urandom(4)
            writer.write(
                struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 12, 5)
            )
            writer.write(ctx.salt)
        await writer.drain()

    async def handle_md5_password(self, ctx: BVContext, payload: bytes, writer: asyncio.StreamWriter):
        # Extract the password from the client request
        client_password_hash = payload.decode("utf-8").rstrip("\x00")
        
        # Store the password hash for potential backend use
        ctx.session.credentials['password_hash'] = client_password_hash
        logger.debug("Password hash captured from client authentication")
        
        # Accept the authentication at proxy level
        await self.send_authentication_ok(writer)
        await self.handle_post_auth(ctx, writer)

    async def handle_plain_password(self, ctx: BVContext, payload: bytes, writer: asyncio.StreamWriter):
        # Extract plain-text password from client request
        plain_password = payload.decode("utf-8").rstrip("\x00")
        
        # Store the plain password for backend authentication
        ctx.session.credentials['password'] = plain_password
        logger.debug("Plain-text password captured from client authentication")
        
        # Accept the authentication at proxy level
        await self.send_authentication_ok(writer)
        await self.handle_post_auth(ctx, writer)

    async def handle_post_auth(self, ctx: BVContext, writer: asyncio.StreamWriter):
        await self.send_parameter_status({
            "server_version": "universql",
            "client_encoding": "UTF8",
            "DateStyle": "ISO",
        }, writer)
        await self.send_backend_key_data(ctx, writer)
        await self.send_ready_for_query(ctx, writer)
        ctx.authenticated = True
        return

    async def handle_query(self, ctx: BVContext, payload: bytes, writer: asyncio.StreamWriter):
        logger.debug("Handle query")
        decoded = payload.decode("utf-8").rstrip("\x00")
        logger.debug(f"Executing SQL: {decoded!r}")
        try:
            # Execute query in a thread pool to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            query_result = await loop.run_in_executor(None, ctx.session.do_query, decoded)
            logger.debug(f"Query result type: {type(query_result)}")
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            await send_error(e, writer)
            await self.send_ready_for_query(ctx, writer)
            return

        if _has_results(query_result):
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

    async def handle_parse(self, ctx: BVContext, payload: bytes, writer: asyncio.StreamWriter):
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

    async def handle_bind(self, ctx: BVContext, payload: bytes, writer: asyncio.StreamWriter):
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

    async def handle_describe(self, ctx: BVContext, payload: bytes, writer: asyncio.StreamWriter):
        logger.debug("Handling describe")
        ba = bytearray(payload)
        describe_type = ba[0]
        if describe_type == ord("P"):
            portal = ba[1: len(ba) - 1].decode("utf-8")
            try:
                loop = asyncio.get_event_loop()
                query_result = await loop.run_in_executor(None, ctx.describe_portal, portal)
            except Exception as e:
                await send_error(e, writer, ctx)
                return
        elif describe_type == ord("S"):
            stmt = ba[1: len(ba) - 1].decode("utf-8")
            try:
                loop = asyncio.get_event_loop()
                query_result = await loop.run_in_executor(None, ctx.describe_statement, stmt)
            except Exception as e:
                await send_error(e, writer, ctx)
                return
            param_oids = ctx.stmts[stmt][1]
            await send_parameter_description(param_oids, writer)
        else:
            raise Exception(f"Unknown describe type: {describe_type}")
        if _has_results(query_result):
            await send_row_description(query_result, writer)

    async def handle_execute(self, ctx: BVContext, payload: bytes, writer: asyncio.StreamWriter):
        logger.debug("Handling execute")
        if ctx.has_error:
            logger.info("Skipping execute due to previous error")
            return
        ba = bytearray(payload)
        portal_idx = ba.index(NULL_BYTE)
        portal = ba[:portal_idx].decode("utf-8")
        limit = struct.unpack("!i", ba[portal_idx + 1: portal_idx + 5])[0]
        query_result = None
        try:
            loop = asyncio.get_event_loop()
            query_result = await loop.run_in_executor(None, ctx.execute_portal, portal)
        except Exception as e:
            await send_error(e, writer, ctx)
            return
        if _has_results(query_result):
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

    async def handle_close(self, ctx: BVContext, payload: bytes, writer: asyncio.StreamWriter):
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

    async def send_ready_for_query(self, ctx: Optional[BVContext], writer: asyncio.StreamWriter):
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


class PostgresqlServer:
    """Async PostgreSQL server using asyncio."""

    def __init__(self, host='0.0.0.0', port=5432):
        self.host = host
        self.port = port
        self.sessions = Sessions()
        self.ctxts = {}
        self.handler = PostgresqlHandler(self)
        self.server = None

    async def start(self):
        """Start the async PostgreSQL server."""
        self.server = await asyncio.start_server(
            self.handler.handle_connection,
            self.host,
            self.port,
            reuse_address=True,
            reuse_port=True
        )
        
        addr = self.server.sockets[0].getsockname()
        logger.info(f"PostgreSQL server started on {addr[0]}:{addr[1]}")
        
        async with self.server:
            await self.server.serve_forever()

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