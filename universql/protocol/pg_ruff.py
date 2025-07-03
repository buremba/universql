import asyncio
import base64
import logging
import os
import tempfile
import threading
import uuid
from typing import Dict, Optional

import pyarrow as pa
import riffq
from fastapi import FastAPI

from universql.plugin import APPS
from universql.protocol.session import Sessions, UniverSQLSession, sessions as global_sessions
from universql.util import LOCALHOST_UNIVERSQL_COM_BYTES

# Register all warehouses and plugins
for module in [
    "universql.warehouse.postgresql",
    "universql.plugins.ui", 
    "universql.plugins.websocket.websocket",
]:
    __import__(module)

logger = logging.getLogger("🦀")

class UniversQLConnection(riffq.BaseConnection):
    """PostgreSQL connection handler with WebSocket integration using riffq."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session: Optional[UniverSQLSession] = None
        self.authenticated = False
        self.server_id = None
        
    def _extract_server_id(self, host: str, port: int) -> Optional[str]:
        """Extract server ID from host and port similar to postgresql.py logic."""
        if not host:
            return str(port)
            
        # Localhost addresses - use port as server ID
        if host in ['127.0.0.1', 'localhost', '::1', '0.0.0.0']:
            return str(port)
            
        # Check for subdomain pattern
        if '.' in host and not host.replace('.', '').replace('-', '').isdigit():
            parts = host.split('.')
            if len(parts) > 2:
                # Extract first part as server ID for subdomains
                return parts[0]
                
        return None

    def _handle_query(self, sql, callback, **kwargs):
        connection_id = id(self)
        try:
            if not self.websocket:
                raise Exception("No active browser session available")
            
            # Handle empty or whitespace-only queries
            if not sql or not sql.strip():
                logger.debug(f"[{connection_id}] Empty query received, returning empty result")
                # Return empty result for empty queries
                empty_batch = self.arrow_batch([pa.array([])], ["result"])
                return self.send_reader(empty_batch, callback)
                
            logger.info(f"[{connection_id}] Executing query: \n{sql}")
            
            result = self.session.do_query(sql)
            return self.send_reader(result, callback)
            
        except Exception as e:
            callback(("ERROR", "42846", f"{e}"), is_error=True)

    def handle_connect(self, ip, port, callback=callable):
        self.server_id = self._extract_server_id(ip, port)
        callback(True)

    def handle_query(self, sql, callback=callable, **kwargs):
        """Handle SQL query from client."""
        connection_id = id(self)        
        if not self.authenticated:
            logger.warning(f"[{connection_id}] Query attempted before authentication")
            error_batch = self.arrow_batch(
                [pa.array(["ERROR: Authentication required"])],
                ["error"]
            )
            return self.send_reader(error_batch, callback)
            
        # Don't create asyncio task in sync thread - just call the sync method directly
        return self._handle_query(sql, callback, **kwargs)

    def handle_auth(self, user, password, host, *, callback, database=None):
        connection_id = id(self)
        logger.info(f"[{connection_id}] Authentication attempt: user={user}, host={host}, database={database}")
        from universql.util import current_context

        credentials = {
            "server_id": self.server_id,
            "user": user,
            "host": host,
        }
        
        if password:
            credentials["password"] = password
        if database:
            credentials["database"] = database
            
        # Extract from current context if available
        url_credentials = current_context.get('url')
        if url_credentials:
            from urllib.parse import urlparse, parse_qs
            
            if isinstance(url_credentials, str):
                parsed = urlparse(url_credentials)
                if parsed.scheme and parsed.scheme.startswith('postgres'):
                    if parsed.hostname and 'host' not in credentials:
                        credentials['host'] = parsed.hostname
                    if parsed.port and 'port' not in credentials:
                        credentials['port'] = parsed.port
                    if parsed.path and len(parsed.path) > 1 and 'database' not in credentials:
                        credentials['database'] = parsed.path.lstrip('/')
                    if parsed.username and 'user' not in credentials:
                        credentials['user'] = parsed.username
                    if parsed.password and 'password' not in credentials:
                        credentials['password'] = parsed.password
                        
                    # Extract query params
                    qparams = parse_qs(parsed.query)
                    for k, v in qparams.items():
                        if v and k not in credentials:
                            credentials[k] = v[0]
            elif isinstance(url_credentials, dict):
                # Handle dict format from main.py
                for key in ['host', 'port', 'database', 'user', 'password', 'sslmode']:
                    if key in url_credentials and url_credentials[key] is not None:
                        if key not in credentials:
                            credentials[key] = url_credentials[key]
        self.websocket = global_sessions.find_websocket_by_credentials(credentials)
        logger.info(f"[{connection_id}] 🔍 Found websocket: {self.websocket}")
  
        if self.websocket is None:
            callback(False, "no", "FATAL", "28P01")
            return

        self.session = UniverSQLSession(
            context=current_context,
            session_id=str(uuid.uuid4()),
            base_engine="postgresql",
            target_engine="websocket",
            credentials=credentials,
            session_parameters={},
        )
        global_sessions.register_session(self.session)  
        logger.info(f"[{connection_id}] 🆕 Created new session: {self.session.session_id}")
        
        self.authenticated = True
        
        logger.info(f"[{connection_id}] 🎉 Authentication successful for user '{user}' - authenticated: {self.authenticated}")
        logger.debug(f"[{connection_id}] Stored in shared state for user: {user}")
        return callback(True)


class PostgresqlServer:
    """PostgreSQL server implementation using riffq with WebSocket integration.
    
    Compatible interface with the original PostgresqlServer for CLI usage.
    """
    
    def __init__(self, host='0.0.0.0', postgresql_port=5436, api_port=55432, ssl_certfile=None, ssl_keyfile=None):
        self.host = host
        self.port = postgresql_port  # PostgreSQL proxy port
        self.api_port = api_port  # FastAPI port
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.server = None
        self.app = FastAPI()
        self.fastapi_server = None
        
    def _setup_ssl(self, server: riffq.RiffqServer) -> bool:
        """Setup SSL certificates for the server."""
        if not (self.ssl_certfile and self.ssl_keyfile):
            logger.info("No SSL configuration provided")
            return False
            
        # Check if we should use embedded certificates
        is_localhost = (
            self.host in ['localhostcomputing.com', 'localhost', '127.0.0.1', '::1', '0.0.0.0'] or
            self.host.endswith('.localhostcomputing.com')
        )
        
        if self.ssl_certfile and self.ssl_keyfile:
            server.set_tls(self.ssl_certfile, self.ssl_keyfile)
            logger.info(f"SSL configured with cert: {self.ssl_certfile}, key: {self.ssl_keyfile}")
            return True
        elif is_localhost and LOCALHOST_UNIVERSQL_COM_BYTES:
            logger.info("Using embedded SSL certificates for localhost")
            try:
                # Decode embedded certificates
                cert_data = base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['cert'])
                key_data = base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['key'])
                
                # Create temporary files
                with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.pem') as cert_file:
                    cert_file.write(cert_data)
                
                with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.key') as key_file:
                    key_file.write(key_data)
                
                logger.info(f"Setting TLS with cert: {cert_file.name}, key: {key_file.name}")
                server.set_tls(cert_file.name, key_file.name)
                logger.info("SSL configured with embedded certificates")
                return True
            except Exception as e:
                logger.error(f"Failed to setup embedded SSL certificates: {e}")
        return False

    async def start(self):
        """Start the async PostgreSQL server with integrated FastAPI."""
        import uvicorn
        
        # Install all apps (including MCP server)
        for app_to_be_installed in APPS:
            app_to_be_installed(self.app)

        # Store connection info in server app state for API endpoints
        self.app.state.host = self.host
        self.app.state.port = self.port
        self.app.state.api_port = self.api_port
        self.app.state.ssl_enabled = bool(self.ssl_certfile and self.ssl_keyfile)
        
        # Configure uvicorn server for FastAPI
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.api_port,
            loop="asyncio",
            log_level="info"
        )
        self.fastapi_server = uvicorn.Server(config)
        
        logger.info(f"FastAPI server (MCP endpoints) starting on {self.host}:{self.api_port}")
        
        # Start FastAPI server in background
        fastapi_task = asyncio.create_task(self.fastapi_server.serve())
        
        # Start riffq PostgreSQL server in a separate thread
        def start_riffq_server():
            logger.info(f"Starting riffq PostgreSQL server on {self.host}:{self.port}")
            
            # Create server with our connection class
            self.server = riffq.RiffqServer(
                f"{self.host}:{self.port}",
                connection_cls=UniversQLConnection
            )
            
            # Setup SSL if configured
            is_tls_active = self._setup_ssl(self.server)
            
            # Start the server
            logger.info(f"🦀 riffq PostgreSQL server listening on {self.host}:{self.port}")
            if self.ssl_certfile and self.ssl_keyfile:
                logger.info("🔒 SSL/TLS enabled")
            
            self.server.start(tls=is_tls_active, catalog_emulation=False) 
        
        # Start riffq server in thread
        riffq_thread = threading.Thread(target=start_riffq_server, daemon=True)
        riffq_thread.start()
        
        try:
            # Wait for FastAPI server to complete
            await fastapi_task
        except asyncio.CancelledError:
            logger.info("Server shutdown requested")
        finally:
            # Cleanup
            if self.fastapi_server:
                await self.fastapi_server.shutdown()

    async def stop(self):
        """Stop the async PostgreSQL server."""
        if self.fastapi_server:
            await self.fastapi_server.shutdown()
        if self.server:
            logger.info("Stopping riffq PostgreSQL server")
            # riffq doesn't seem to have a stop method, server runs until process ends

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()


def create_server(host='127.0.0.1', port=5432, api_port=8084, ssl_certfile=None, ssl_keyfile=None):
    """Create and return a new PostgresqlServer instance."""
    return PostgresqlServer(
        host=host,
        postgresql_port=port,
        api_port=api_port,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile
    )


if __name__ == "__main__":
    # Example usage
    server = create_server()
    try:
        import asyncio
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info("Server interrupted by user")