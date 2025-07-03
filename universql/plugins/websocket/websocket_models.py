#!/usr/bin/env python3
"""
WebSocket Protocol Message Models for PostgreSQL Bridge

This module defines the Pydantic models for high-level messages exchanged between
WebSocket clients and the PostgreSQL bridge server.

Supported high-level operations:
- create: Create new compute instance
- connect: Connect to existing instance  
- query: Execute SQL queries
- close: Close database connection

query_result) without exposing raw PostgreSQL protocol details.
"""

from typing import List, Optional, Any, Dict, Union
from pydantic import BaseModel, Field


# Client -> Server Messages (Incoming)
class ClientAuthCreateMessage(BaseModel):
    """Client request to create a new user session"""
    type: str = Field("create", description="Message type identifier")


class ClientAuthConnectMessage(BaseModel):
    """Client request to connect with existing credentials"""
    type: str = Field("connect", description="Message type identifier")
    credentials: Dict[str, str] = Field(..., description="Connection credentials (id, user, password)")


class ClientQueryMessage(BaseModel):
    """Client request to execute a simple SQL query"""
    type: str = Field("query", description="Message type identifier")
    sql: str = Field(..., description="SQL query to execute")


class ClientCloseConnectionMessage(BaseModel):
    """Client request to close the database connection"""
    type: str = Field("close", description="Message type identifier")


# Server -> Client Messages (Outgoing)
class ServerConnectionEstablishedMessage(BaseModel):
    """Server response confirming successful authentication"""
    type: str = Field("connection_established", description="Message type identifier")
    data: Dict[str, str] = Field(..., description="User credentials (user, password, id)")


class ServerConnectionErrorMessage(BaseModel):
    """Server response for authentication errors"""
    type: str = Field("connection_error", description="Message type identifier")
    error: str = Field(..., description="Error message")


class ServerColumnInfo(BaseModel):
    """Column metadata information"""
    name: str = Field(..., description="Column name")
    type_oid: int = Field(..., description="PostgreSQL type OID")
    type_name: Optional[str] = Field(None, description="Human-readable type name")
    type_size: Optional[int] = Field(None, description="Type size (-1 for variable)")


class ServerQueryResultMessage(BaseModel):
    """Server response with query results"""
    type: str = Field("query_result", description="Message type identifier")
    success: bool = Field(..., description="Whether query succeeded")
    columns: List[ServerColumnInfo] = Field(..., description="Column metadata")
    rows: List[List[Optional[str]]] = Field(..., description="Result rows (text format)")
    command_tag: str = Field(..., description="PostgreSQL command tag")


class ServerConnectionClosedMessage(BaseModel):
    """Server response confirming connection closed"""
    type: str = Field("connection_closed", description="Message type identifier")
    message: str = Field(..., description="Connection close confirmation message")


class ServerReadyForQueryMessage(BaseModel):
    """Server response indicating ready for next query"""
    type: str = Field("ready_for_query", description="Message type identifier")
    transaction_status: str = Field(..., description="Transaction status: 'I'=idle, 'T'=in transaction, 'E'=error")


class ServerErrorMessage(BaseModel):
    """Server response for errors"""
    type: str = Field("error", description="Message type identifier")
    severity: Optional[str] = Field("ERROR", description="Error severity")
    code: Optional[str] = Field(None, description="PostgreSQL error code")
    message: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Error detail")
    hint: Optional[str] = Field(None, description="Error hint")


class ServerAuthenticationRequiredMessage(BaseModel):
    """Server response when authentication is required"""
    type: str = Field("authentication_required", description="Message type identifier")
    error: str = Field(..., description="Authentication requirement message")


# API Request/Response Models
class ApiQueryRequest(BaseModel):
    """API request to execute query on WebSocket client"""
    server_id: str = Field(..., description="Target server/client ID")
    sql: str = Field(..., description="SQL query to execute")


class ApiQueryResponse(BaseModel):
    """API response for query execution"""
    success: bool = Field(..., description="Whether request succeeded")
    message: str = Field(..., description="Status message")
    compute_available: bool = Field(..., description="Whether compute instance is available")
    result: Optional[Dict[str, Any]] = Field(None, description="Query result if successful")


# Union types for message parsing
ClientMessage = Union[
    ClientAuthCreateMessage,
    ClientAuthConnectMessage,
    ClientQueryMessage,
    ClientCloseConnectionMessage
]

ServerMessage = Union[
    ServerConnectionEstablishedMessage,
    ServerConnectionErrorMessage,
    ServerQueryResultMessage,
    ServerConnectionClosedMessage,
    ServerReadyForQueryMessage,
    ServerErrorMessage,
    ServerAuthenticationRequiredMessage
]

# HTMX Dashboard Models
class HtmxCreateInstanceRequest(BaseModel):
    """Request to create a new compute instance via HTMX"""
    pass  # No parameters needed for creation


class HtmxConnectInstanceRequest(BaseModel):
    """Request to connect to existing compute instance via HTMX"""
    server_id: str = Field(..., description="Server/compute instance ID")
    username: str = Field(..., description="Username for authentication")
    password: str = Field(..., description="Password for authentication")


class HtmxExecuteQueryRequest(BaseModel):
    """Request to execute query via HTMX"""
    server_id: Optional[str] = Field(None, description="Target server ID (empty for current)")
    sql: str = Field(..., description="SQL query to execute")


class HtmxStatsResponse(BaseModel):
    """Response for statistics endpoint"""
    active_connections: int = Field(..., description="Number of active WebSocket connections")
    postgresql_sessions: int = Field(..., description="Number of PostgreSQL sessions")
    total_sessions: int = Field(..., description="Total user sessions created")


class HtmxStatusResponse(BaseModel):
    """Response for status updates"""
    status: str = Field(..., description="Status indicator (connected/disconnected/pending)")
    message: str = Field(..., description="Human-readable status message")
    credentials: Optional[Dict[str, str]] = Field(None, description="Server credentials if available")


class HtmxLogEntry(BaseModel):
    """Activity log entry"""
    timestamp: str = Field(..., description="Timestamp of the log entry")
    message: str = Field(..., description="Log message")
    type: str = Field(..., description="Log type (info/success/error)")


class HtmxActivityLogResponse(BaseModel):
    """Response for activity log"""
    entries: List[HtmxLogEntry] = Field(..., description="List of log entries")


class SqlColumn(BaseModel):
    name: str
    type_oid: int = 25  # Default to text
    type_name: str = "text"

class QueryResult(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    affected_rows: Optional[int] = Field(None, alias="affectedRows", description="Number of rows affected by the query")
    error: Optional[str] = None

class SqlResponse(BaseModel):
    query_id: str
    type: str = "query_executed"
    results: List[QueryResult]
