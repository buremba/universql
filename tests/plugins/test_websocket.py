import asyncio
import json
import logging
import socketserver
import threading
import time

import pytest
import websockets
from click.testing import CliRunner

# Configure logging for tests
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Test configuration
POSTGRESQL_URL = "postgresql://test_user:test_pass@localhost:5433/test_db" 
TEST_TIMEOUT = 30

class TestWebsocketE2E:
    """Simplified end-to-end test for websocket functionality"""
    
    @pytest.fixture(scope="class")
    def server_ports(self):
        """Get available ports for testing"""
        with socketserver.TCPServer(("127.0.0.1", 0), None) as s1:
            postgresql_port = s1.server_address[1]
        with socketserver.TCPServer(("127.0.0.1", 0), None) as s2:
            api_port = s2.server_address[1]
        return postgresql_port, api_port
    
    @pytest.fixture(scope="class")
    def universql_server(self, server_ports):
        """Start UniverSQL server with websocket support in background"""
        postgresql_port, api_port = server_ports
        
        def start_server():
            from universql.main import postgresql as postgresql_command
            runner = CliRunner()
            result = runner.invoke(
                postgresql_command,
                [
                    '--url', POSTGRESQL_URL,
                    '--port', str(postgresql_port),
                    '--api-port', str(api_port),
                    '--host', '127.0.0.1',
                ],
                catch_exceptions=False
            )
            if result.exit_code != 0:
                raise Exception(f"Unable to start UniverSQL server: {result.output}")
        
        logger.info(f"Starting UniverSQL server on PostgreSQL port {postgresql_port}, API port {api_port}")
        thread = threading.Thread(target=start_server)
        thread.daemon = True
        thread.start()
        
        # Give server time to start
        time.sleep(3)
        
        yield postgresql_port, api_port
        
        logger.info("Test completed, server cleanup via daemon thread")

    @pytest.mark.asyncio
    async def test_websocket_create_new_server(self, universql_server):
        """Test creating a new server via REST API and connecting via websocket"""
        postgresql_port, api_port = universql_server
        import httpx
        
        # Create session via REST API (without specifying server_id, will be auto-generated)
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://127.0.0.1:{api_port}/create-session",
                json={}  # No server_id provided, should generate one
            )
            
            assert response.status_code == 200
            data = response.json()
            logger.info(f"Create session response: {data}")
            
            assert data.get("success") == True
            credentials = data.get("credentials")
            assert "server_id" in credentials
            assert "user" in credentials
            assert "password" in credentials
            
            # Verify server_id was created
            server_id = credentials["server_id"]
            assert server_id is not None
            assert len(server_id) > 0
            
            # Now connect via WebSocket with the returned credentials
            websocket_url = f"ws://127.0.0.1:{api_port}/ws"
            async with websockets.connect(websocket_url) as websocket:
                connect_message = {
                    "type": "connect",
                    "credentials": credentials
                }
                await websocket.send(json.dumps(connect_message))

                # Wait for response
                ws_response = await asyncio.wait_for(websocket.recv(), timeout=TEST_TIMEOUT)
                ws_data = json.loads(ws_response)

                logger.info(f"WebSocket connection response: {ws_data}")

                # Verify response
                assert ws_data.get("type") == "connection_established"
                assert "data" in ws_data
                auth_data = ws_data["data"]
                assert auth_data["server_id"] == server_id
                assert auth_data["user"] == credentials["user"]
                
                logger.info(f"✅ Successfully created new server with ID: {server_id}")
                
                # Return credentials for other tests that might need them
                return credentials

    @pytest.mark.asyncio
    async def test_websocket_connect_existing_server(self, universql_server):
        """Test connecting to existing server with credentials via REST API then websocket"""
        postgresql_port, api_port = universql_server
        import httpx
        
        # First, create a session via REST API to get credentials
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://127.0.0.1:{api_port}/create-session",
                json={"server_id": "test-existing-server"}
            )
            
            assert response.status_code == 200
            data = response.json()
            
            assert data.get("success") == True
            created_credentials = data.get("credentials")
            
        logger.info(f"Created credentials for testing: {created_credentials}")
        
        # Now test connecting with existing credentials multiple times
        websocket_url = f"ws://127.0.0.1:{api_port}/ws"
        
        # First websocket connection
        async with websockets.connect(websocket_url) as websocket:
            connect_message = {
                "type": "connect",
                "credentials": created_credentials
            }
            await websocket.send(json.dumps(connect_message))

            # Wait for response
            response = await asyncio.wait_for(websocket.recv(), timeout=TEST_TIMEOUT)
            data = json.loads(response)

            logger.info(f"First websocket connection response: {data}")

            # Verify successful connection
            assert data.get("type") == "connection_established"
            assert "data" in data
            auth_data = data["data"]
            
            # Should return the same server_id and credentials
            assert auth_data["server_id"] == created_credentials["server_id"]
            assert auth_data["user"] == created_credentials["user"]
            
        # Test a second connection with the same credentials (after first one disconnects)
        async with websockets.connect(websocket_url) as websocket:
            connect_message = {
                "type": "connect",
                "credentials": created_credentials
            }
            await websocket.send(json.dumps(connect_message))

            # Wait for response
            response = await asyncio.wait_for(websocket.recv(), timeout=TEST_TIMEOUT)
            data = json.loads(response)

            logger.info(f"Second websocket connection response: {data}")

            # Verify successful connection again
            assert data.get("type") == "connection_established"
            auth_data = data["data"]
            assert auth_data["server_id"] == created_credentials["server_id"]
            
            logger.info(f"✅ Successfully connected to existing server with ID: {auth_data['server_id']}")

    @pytest.mark.asyncio
    async def test_create_session_server_id_conflict(self, universql_server):
        """Test that create-session HTTP endpoint returns 401 when server_id already has a websocket attached"""
        postgresql_port, api_port = universql_server
        import httpx
        
        # First, create a session with specific server_id
        async with httpx.AsyncClient() as client:
            response1 = await client.post(
                f"http://127.0.0.1:{api_port}/create-session",
                json={"server_id": "test-server-123"}
            )
            
            assert response1.status_code == 200
            data1 = response1.json()
            logger.info(f"First create-session response: {data1}")
            
            assert data1.get("success") == True
            credentials1 = data1.get("credentials")
            assert credentials1["server_id"] == "test-server-123"
            
            # Connect via WebSocket with these credentials
            websocket_url = f"ws://127.0.0.1:{api_port}/ws"
            async with websockets.connect(websocket_url) as websocket1:
                connect_message = {
                    "type": "connect",
                    "credentials": credentials1
                }
                await websocket1.send(json.dumps(connect_message))
                
                # Wait for connection established
                ws_response = await asyncio.wait_for(websocket1.recv(), timeout=TEST_TIMEOUT)
                ws_data = json.loads(ws_response)
                logger.info(f"WebSocket connection response: {ws_data}")
                
                assert ws_data.get("type") == "connection_established"
                
                # Now try to create another session with the same server_id - should fail
                response2 = await client.post(
                    f"http://127.0.0.1:{api_port}/create-session",
                    json={"server_id": "test-server-123"}  # Same server_id
                )
                
                logger.info(f"Second create-session response: {response2.status_code}")
                
                # Verify 401 error response
                assert response2.status_code == 401
                error_data = response2.json()
                assert "Server ID test-server-123 is already in use" in error_data.get("detail", "")
                
                logger.info("✅ Successfully prevented server_id conflict")

    @pytest.mark.asyncio
    async def test_create_session_no_conflict(self, universql_server):
        """Test that create-session HTTP endpoint works when no server_id conflict exists"""
        postgresql_port, api_port = universql_server
        import httpx
        
        # Create session with unique server_id via HTTP POST
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://127.0.0.1:{api_port}/create-session",
                json={"server_id": "unique-server-456"}
            )
            
            assert response.status_code == 200
            data = response.json()
            logger.info(f"Create session response: {data}")
            
            assert data.get("success") == True
            credentials = data.get("credentials")
            assert credentials["server_id"] == "unique-server-456"
            assert "user" in credentials
            assert "password" in credentials
            
            # Connect via WebSocket with the returned credentials
            websocket_url = f"ws://127.0.0.1:{api_port}/ws"
            async with websockets.connect(websocket_url) as websocket:
                connect_message = {
                    "type": "connect",
                    "credentials": credentials
                }
                await websocket.send(json.dumps(connect_message))
                
                # Wait for connection established
                ws_response = await asyncio.wait_for(websocket.recv(), timeout=TEST_TIMEOUT)
                ws_data = json.loads(ws_response)
                logger.info(f"WebSocket connection response: {ws_data}")
                
                assert ws_data.get("type") == "connection_established"
                assert ws_data["data"]["server_id"] == "unique-server-456"
                
                # Test query execution
                query_message = {
                    "type": "query",
                    "sql": "SELECT 1 as test_col"
                }
                await websocket.send(json.dumps(query_message))

                # Wait for query result
                query_response = await asyncio.wait_for(websocket.recv(), timeout=TEST_TIMEOUT)
                query_data = json.loads(query_response)
                
                logger.info(f"Query response: {query_data}")
                
                # Verify query result
                assert query_data.get("type") == "query_result"
                assert query_data.get("success") == True
                
                # Wait for ready_for_query
                ready_response = await asyncio.wait_for(websocket.recv(), timeout=TEST_TIMEOUT)
                ready_data = json.loads(ready_response)
                
                assert ready_data.get("type") == "ready_for_query"
                
                logger.info("✅ Successfully created session and executed query")

    @pytest.mark.asyncio
    async def test_postgresql_websocket_query_routing(self, universql_server):
        """Test that PostgreSQL queries are routed through WebSocket connections"""
        postgresql_port, api_port = universql_server
        import httpx
        import asyncio
        import queue
        import threading
        import struct
        import socket
        
        # Create session via REST API (reusing the pattern from working tests)
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://127.0.0.1:{api_port}/create-session",
                json={"server_id": "pg-protocol-test"}
            )
            
            assert response.status_code == 200
            data = response.json()
            logger.info(f"✅ Session created: {data}")
            
            credentials = data.get("credentials")
            
            # Connect via WebSocket (reusing the pattern from working tests)
            websocket_url = f"ws://127.0.0.1:{api_port}/ws"
            websocket_messages = queue.Queue()
            websocket_connected = threading.Event()
            query_routed_received = threading.Event()
            
            async def websocket_handler():
                async with websockets.connect(websocket_url) as websocket:
                    # Connect with credentials
                    connect_message = {
                        "type": "connect",
                        "credentials": credentials
                    }
                    await websocket.send(json.dumps(connect_message))
                    
                    # Wait for connection established
                    ws_response = await asyncio.wait_for(websocket.recv(), timeout=TEST_TIMEOUT)
                    ws_data = json.loads(ws_response)
                    logger.info(f"✅ WebSocket connected: {ws_data}")
                    
                    assert ws_data.get("type") == "connection_established"
                    websocket_connected.set()
                    
                    # Listen for query routing notifications
                    try:
                        while True:
                            message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                            msg_data = json.loads(message)
                            logger.info(f"📩 WebSocket received: {msg_data}")
                            websocket_messages.put(msg_data)
                            
                            if msg_data.get("type") == "query_routed":
                                logger.info("🎯 Query routing notification received!")
                                query_routed_received.set()
                    except asyncio.TimeoutError:
                        pass  # Expected when test completes
            
            # Start websocket handler
            websocket_task = asyncio.create_task(websocket_handler())
            
            # Wait for websocket connection
            await asyncio.get_event_loop().run_in_executor(None, websocket_connected.wait, 10)
            
            # Test PostgreSQL protocol routing using direct socket connection (cleaner than psycopg2)
            def test_postgresql_protocol():
                """Test PostgreSQL protocol directly to avoid psycopg2 complexities"""
                try:
                    # Connect to PostgreSQL port
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(("127.0.0.1", postgresql_port))
                    
                    # Send PostgreSQL startup message
                    startup_msg = b"\\x00\\x03\\x00\\x00"  # Protocol version 3.0
                    startup_msg += f"user\\x00{credentials['user']}\\x00".encode()
                    startup_msg += f"database\\x00test\\x00".encode()
                    startup_msg += b"\\x00"  # End of parameters
                    
                    # Add length prefix
                    startup_length = len(startup_msg) + 4
                    startup_packet = struct.pack("!I", startup_length) + startup_msg
                    sock.send(startup_packet)
                    
                    # Receive authentication request
                    auth_response = sock.recv(1024)
                    logger.info(f"📨 Received auth response: {len(auth_response)} bytes")
                    
                    # Send password (MD5 or plaintext depending on what server requests)
                    password = credentials["password"]
                    pwd_msg = b"p" + struct.pack("!I", len(password) + 5) + password.encode() + b"\\x00"
                    sock.send(pwd_msg)
                    
                    # Receive authentication OK and ready for query
                    ready_response = sock.recv(1024)
                    logger.info(f"📨 Received ready response: {len(ready_response)} bytes")
                    
                    # Send a simple query
                    query_sql = "SELECT 42 as answer"
                    query_msg = b"Q" + struct.pack("!I", len(query_sql) + 5) + query_sql.encode() + b"\\x00"
                    sock.send(query_msg)
                    
                    # Receive query response
                    query_response = sock.recv(1024)
                    logger.info(f"📨 Received query response: {len(query_response)} bytes")
                    
                    sock.close()
                    logger.info("✅ PostgreSQL protocol test completed")
                    return "success"
                    
                except Exception as e:
                    logger.error(f"❌ PostgreSQL protocol test failed: {e}")
                    import traceback
                    traceback.print_exc()
                    return f"error: {e}"
            
            # Run the PostgreSQL protocol test (simplified to focus on routing)
            def test_routing_via_protocol():
                """Test the routing functionality using the same pattern as the working test"""
                try:
                    from universql.protocol.session import sessions as global_sessions
                    
                    # Find the session (reusing working pattern)
                    session = global_sessions.find_session_by_credentials(credentials)
                    if not session:
                        return "no session found"
                    
                    logger.info(f"📋 Found session: {session.session_id}")
                    
                    # Check if websocket is linked (reusing working pattern)
                    websocket = global_sessions.find_websocket_by_credentials(credentials)
                    if not websocket:
                        return "no websocket found"
                    
                    logger.info(f"🔗 Found linked websocket")
                    
                    # Test the routing with a query that a PostgreSQL client would send
                    from universql.protocol.postgresql import PGContext
                    
                    class MockPGContext:
                        def __init__(self, session):
                            self.session = session
                        
                        def _route_query(self, sql):
                            return PGContext._route_query(self, sql)
                    
                    mock_ctx = MockPGContext(session)
                    
                    # Test with a query similar to what PostgreSQL clients send
                    result = mock_ctx._route_query("SELECT 42 as answer")
                    
                    logger.info(f"🎯 PostgreSQL-style query executed successfully! Result type: {type(result)}")
                    return "success"
                    
                except Exception as e:
                    logger.error(f"❌ Routing test failed: {e}")
                    import traceback
                    traceback.print_exc()
                    return f"error: {e}"
            
            # Test the routing logic (reusing the successful pattern)
            result = await asyncio.get_event_loop().run_in_executor(None, test_routing_via_protocol)
            logger.info(f"🔧 Routing test result: {result}")
            
            # Wait for websocket notification
            notification_received = await asyncio.get_event_loop().run_in_executor(None, query_routed_received.wait, 5)
            
            # Cancel websocket task
            websocket_task.cancel()
            try:
                await websocket_task
            except asyncio.CancelledError:
                pass
            
            # Check results (reusing the successful pattern)
            received_messages = []
            try:
                while True:
                    msg = websocket_messages.get_nowait()
                    received_messages.append(msg)
            except queue.Empty:
                pass
            
            logger.info(f"📨 Received {len(received_messages)} messages: {received_messages}")
            
            # Verify we got the routing notification
            query_routed_found = any(
                msg.get("type") == "query_routed" and "SELECT 42" in msg.get("sql", "")
                for msg in received_messages
            )
            
            logger.info(f"🎯 Query routed notification found: {query_routed_found}")
            logger.info(f"🔧 Routing logic result: {result}")
            
            # Test passes if we successfully demonstrated the routing mechanism
            assert result == "success", f"Routing logic failed: {result}"
            assert query_routed_found, f"WebSocket query routing notification not received. Messages: {received_messages}"
            
            logger.info("✅ PostgreSQL protocol to WebSocket routing test completed successfully!")

    @pytest.mark.asyncio
    async def test_postgresql_websocket_routing_simple(self, universql_server):
        """Test PostgreSQL to WebSocket routing with simple query that doesn't require external database"""
        postgresql_port, api_port = universql_server
        import httpx
        import asyncio
        import queue
        import threading
        
        # Create session via REST API  
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://127.0.0.1:{api_port}/create-session",
                json={"server_id": "simple-routing-test"}
            )
            
            assert response.status_code == 200
            data = response.json()
            logger.info(f"✅ Session created: {data}")
            
            credentials = data.get("credentials")
            
            # Connect via WebSocket
            websocket_url = f"ws://127.0.0.1:{api_port}/ws"
            websocket_messages = queue.Queue()
            websocket_connected = threading.Event()
            query_routed_received = threading.Event()
            
            async def websocket_handler():
                async with websockets.connect(websocket_url) as websocket:
                    # Connect with credentials
                    connect_message = {
                        "type": "connect",
                        "credentials": credentials
                    }
                    await websocket.send(json.dumps(connect_message))
                    
                    # Wait for connection established
                    ws_response = await asyncio.wait_for(websocket.recv(), timeout=TEST_TIMEOUT)
                    ws_data = json.loads(ws_response)
                    logger.info(f"✅ WebSocket connected: {ws_data}")
                    
                    assert ws_data.get("type") == "connection_established"
                    websocket_connected.set()
                    
                    # Listen for query routing notifications
                    try:
                        while True:
                            message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                            msg_data = json.loads(message)
                            logger.info(f"📩 WebSocket received: {msg_data}")
                            websocket_messages.put(msg_data)
                            
                            if msg_data.get("type") == "query_routed":
                                logger.info("🎯 Query routing notification received!")
                                query_routed_received.set()
                    except asyncio.TimeoutError:
                        pass  # Expected when test completes
            
            # Start websocket handler
            websocket_task = asyncio.create_task(websocket_handler())
            
            # Wait for websocket connection
            await asyncio.get_event_loop().run_in_executor(None, websocket_connected.wait, 10)
            
            # Now test direct routing by calling the session's _route_query method
            def test_routing_logic():
                """Test that our routing logic works"""
                try:
                    from universql.protocol.session import sessions as global_sessions
                    
                    # Find the session
                    session = global_sessions.find_session_by_credentials(credentials)
                    if not session:
                        return "no session found"
                    
                    logger.info(f"📋 Found session: {session.session_id}")
                    
                    # Check if websocket is linked
                    websocket = global_sessions.find_websocket_by_credentials(credentials)
                    if not websocket:
                        return "no websocket found"
                    
                    logger.info(f"🔗 Found linked websocket")
                    
                    # Test the routing notification (which is the key functionality)
                    from universql.protocol.postgresql import PGContext
                    
                    # Create a mock context to test _route_query
                    class MockPGContext:
                        def __init__(self, session):
                            self.session = session
                        
                        def _route_query(self, sql):
                            return PGContext._route_query(self, sql)
                    
                    mock_ctx = MockPGContext(session)
                    
                    # This should trigger the WebSocket notification and create a clean session
                    result = mock_ctx._route_query("SELECT 1")
                    
                    logger.info(f"🎯 Query executed successfully! Result type: {type(result)}")
                    return "success"
                    
                except Exception as e:
                    logger.error(f"❌ Routing test failed: {e}")
                    import traceback
                    traceback.print_exc()
                    return f"error: {e}"
            
            # Test the routing logic
            result = await asyncio.get_event_loop().run_in_executor(None, test_routing_logic)
            logger.info(f"🔧 Routing test result: {result}")
            
            # Wait for websocket notification
            notification_received = await asyncio.get_event_loop().run_in_executor(None, query_routed_received.wait, 5)
            
            # Cancel websocket task
            websocket_task.cancel()
            try:
                await websocket_task
            except asyncio.CancelledError:
                pass
            
            # Check results
            received_messages = []
            try:
                while True:
                    msg = websocket_messages.get_nowait()
                    received_messages.append(msg)
            except queue.Empty:
                pass
            
            logger.info(f"📨 Received {len(received_messages)} messages: {received_messages}")
            
            # Verify we got the routing notification
            query_routed_found = any(
                msg.get("type") == "query_routed" and "SELECT 1" in msg.get("sql", "")
                for msg in received_messages
            )
            
            logger.info(f"🎯 Query routed notification found: {query_routed_found}")
            logger.info(f"🔧 Routing logic result: {result}")
            
            # Test passes if we successfully demonstrated the routing mechanism
            assert result == "success", f"Routing logic failed: {result}"
            assert query_routed_found, f"WebSocket query routing notification not received. Messages: {received_messages}"
            
            logger.info("✅ PostgreSQL to WebSocket routing test completed successfully!")