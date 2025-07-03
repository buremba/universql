#!/usr/bin/env python3
"""
JDBC integration tests for PostgreSQL proxy using jaydebeapi.
Tests both SSL and non-SSL connections through JDBC, similar to how DBeaver connects.

Note: These tests require Java to be installed on the system.
Java is REQUIRED - tests will FAIL if Java is not available.
"""

import logging
import os
import socketserver
import subprocess
import threading
import time
import tempfile
import base64
from pathlib import Path

import pytest

logger = logging.getLogger(__name__)

# Skip these tests if jaydebeapi is not available
jaydebeapi = pytest.importorskip("jaydebeapi")

def is_java_available():
    """Check if Java is available on the system."""
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, text=True, timeout=5)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False

def get_java_installation_instructions():
    """Get Java installation instructions for the current platform."""
    import platform
    system = platform.system().lower()
    
    if system == "darwin":  # macOS
        return """
Install Java on macOS:
  Option 1: brew install openjdk@17
  Option 2: Download from https://adoptium.net/
  Option 3: brew install --cask temurin17
"""
    elif system == "linux":
        return """
Install Java on Linux:
  Ubuntu/Debian: sudo apt-get install openjdk-17-jdk
  CentOS/RHEL:   sudo yum install java-17-openjdk-devel
  Arch:          sudo pacman -S jdk17-openjdk
"""
    else:
        return """
Install Java:
  Download from https://adoptium.net/ or https://openjdk.java.net/
"""

# Test configuration - Latest PostgreSQL JDBC driver from Maven Central
POSTGRESQL_DRIVER_VERSION = "42.7.7"  # Latest as of June 2025
POSTGRESQL_DRIVER = "org.postgresql.Driver"
MAVEN_CENTRAL_URL = f"https://repo1.maven.org/maven2/org/postgresql/postgresql/{POSTGRESQL_DRIVER_VERSION}/postgresql-{POSTGRESQL_DRIVER_VERSION}.jar"

# Cache directory in project root 
PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / ".cache" / "jdbc_drivers"
JDBC_DRIVER_PATH = CACHE_DIR / f"postgresql-{POSTGRESQL_DRIVER_VERSION}.jar"

class TestPostgreSQLJDBC:
    """Test PostgreSQL proxy via JDBC connections."""
    
    @pytest.fixture(scope="class")
    def java_check(self):
        """Verify Java is available - FAIL if not found."""
        if not is_java_available():
            pytest.fail(f"❌ Java not found. JDBC tests require Java 8+.\n{get_java_installation_instructions()}")
        
        # Get Java version info
        try:
            result = subprocess.run(['java', '-version'], 
                                  capture_output=True, text=True, timeout=5)
            logger.info(f"✅ Java is available: {result.stderr.split()[0:3]}")
        except Exception as e:
            logger.warning(f"Java version check failed: {e}")
    
    @pytest.fixture(scope="class")
    def jdbc_driver(self):
        """Download and cache the latest PostgreSQL JDBC driver from Maven Central."""
        # Create cache directory
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
        if not JDBC_DRIVER_PATH.exists():
            logger.info(f"Downloading PostgreSQL JDBC driver {POSTGRESQL_DRIVER_VERSION} from Maven Central...")
            logger.info(f"URL: {MAVEN_CENTRAL_URL}")
            
            import urllib.request
            import shutil
            
            try:
                with urllib.request.urlopen(MAVEN_CENTRAL_URL) as response:
                    with open(JDBC_DRIVER_PATH, 'wb') as f:
                        shutil.copyfileobj(response, f)
                        
                logger.info(f"✅ PostgreSQL JDBC driver downloaded to {JDBC_DRIVER_PATH}")
                logger.info(f"   Size: {JDBC_DRIVER_PATH.stat().st_size / 1024:.1f} KB")
                
            except Exception as e:
                pytest.fail(f"Failed to download PostgreSQL JDBC driver: {e}")
        else:
            logger.info(f"✅ PostgreSQL JDBC driver already cached: {JDBC_DRIVER_PATH}")
            
        return JDBC_DRIVER_PATH
    
    @pytest.fixture(scope="class")
    def server_ports(self):
        """Get available ports for testing."""
        with socketserver.TCPServer(("127.0.0.1", 0), None) as s1:
            postgresql_port = s1.server_address[1]
        with socketserver.TCPServer(("127.0.0.1", 0), None) as s2:
            api_port = s2.server_address[1]
        return postgresql_port, api_port
    
    @pytest.fixture(scope="class")
    def ssl_certificates(self):
        """Create temporary SSL certificate files for testing."""
        from universql.util import LOCALHOST_UNIVERSQL_COM_BYTES
        
        cert_file = tempfile.NamedTemporaryFile(suffix='.pem', delete=False)
        key_file = tempfile.NamedTemporaryFile(suffix='.pem', delete=False)
        
        try:
            cert_file.write(base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['cert']))
            cert_file.flush()
            
            key_file.write(base64.b64decode(LOCALHOST_UNIVERSQL_COM_BYTES['key']))
            key_file.flush()
            
            yield cert_file.name, key_file.name
            
        finally:
            try:
                os.unlink(cert_file.name)
                os.unlink(key_file.name)
            except:
                pass
    
    @pytest.fixture(scope="class")
    def non_ssl_server(self, server_ports, java_check, jdbc_driver):
        """Start UniverSQL PostgreSQL server without SSL."""
        postgresql_port, api_port = server_ports
        
        def start_server():
            from universql.main import postgresql as postgresql_command
            from click.testing import CliRunner
            
            runner = CliRunner()
            result = runner.invoke(
                postgresql_command,
                [
                    '--host', '127.0.0.1',
                    '--port', str(postgresql_port),
                    '--api-port', str(api_port),
                ],
                catch_exceptions=False
            )
            if result.exit_code != 0:
                raise Exception(f"Unable to start UniverSQL server: {result.output}")
        
        logger.info(f"Starting non-SSL UniverSQL server on port {postgresql_port}")
        thread = threading.Thread(target=start_server)
        thread.daemon = True
        thread.start()
        
        # Give server time to start
        time.sleep(3)
        
        yield postgresql_port, api_port
        
        logger.info("Non-SSL server test completed")
    
    def test_java_availability(self, java_check):
        """Test that Java is available for JDBC tests."""
        # java_check fixture will fail if Java is not available
        assert is_java_available(), "Java should be available after java_check fixture"
        logger.info("✅ Java availability confirmed")
    
    def test_jdbc_driver_download(self, jdbc_driver):
        """Test that PostgreSQL JDBC driver is downloaded and cached."""
        assert jdbc_driver.exists(), f"PostgreSQL JDBC driver not found at {jdbc_driver}"
        assert jdbc_driver.stat().st_size > 1000, "JDBC driver file seems too small"
        
        logger.info(f"✅ PostgreSQL JDBC driver v{POSTGRESQL_DRIVER_VERSION} ready: {jdbc_driver}")
        logger.info(f"   Size: {jdbc_driver.stat().st_size / 1024:.1f} KB")
    
    def test_jdbc_non_ssl_connection(self, non_ssl_server, jdbc_driver):
        """Test JDBC connection without SSL."""
        postgresql_port, api_port = non_ssl_server
        
        # JDBC connection URL for non-SSL
        jdbc_url = f"jdbc:postgresql://127.0.0.1:{postgresql_port}/postgres"
        
        logger.info(f"Testing JDBC non-SSL connection to {jdbc_url}")
        logger.info(f"Using JDBC driver: {jdbc_driver}")
        
        try:
            # Connect via JDBC
            conn = jaydebeapi.connect(
                POSTGRESQL_DRIVER,
                jdbc_url,
                {'user': 'testuser', 'password': 'testpass'},
                str(jdbc_driver)
            )
            
            logger.info("✅ JDBC non-SSL connection established successfully")
            
            # Test a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT 1 as test_value")
            result = cursor.fetchone()
            
            assert result is not None
            assert result[0] == 1
            logger.info(f"✅ Query executed successfully: {result}")
            
            cursor.close()
            conn.close()
            
            logger.info("✅ JDBC non-SSL connection test completed successfully")
            
        except Exception as e:
            logger.error(f"❌ JDBC non-SSL connection failed: {e}")
            raise
    
    def test_jdbc_ssl_connection(self, ssl_server, jdbc_driver):
        """Test JDBC connection with SSL."""
        postgresql_port, api_port = ssl_server
        
        # JDBC connection URL with SSL
        jdbc_url = f"jdbc:postgresql://127.0.0.1:{postgresql_port}/postgres?ssl=true&sslmode=require"
        
        logger.info(f"Testing JDBC SSL connection to {jdbc_url}")
        logger.info(f"Using JDBC driver: {jdbc_driver}")
        
        try:
            # Connect via JDBC with SSL
            conn = jaydebeapi.connect(
                POSTGRESQL_DRIVER,
                jdbc_url,
                {'user': 'testuser', 'password': 'testpass'},
                str(jdbc_driver)
            )
            
            logger.info("✅ JDBC SSL connection established successfully")
            
            # Test a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT 'SSL Connection Test' as message")
            result = cursor.fetchone()
            
            assert result is not None
            assert 'SSL' in result[0]
            logger.info(f"✅ SSL Query executed successfully: {result}")
            
            cursor.close()
            conn.close()
            
            logger.info("✅ JDBC SSL connection test completed successfully")
            
        except Exception as e:
            logger.error(f"❌ JDBC SSL connection failed: {e}")
            raise

if __name__ == "__main__":
    # Quick test when run directly
    import sys
    logging.basicConfig(level=logging.INFO)
    
    print("🔧 JDBC Integration Test")
    print("=" * 50)
    
    if not is_java_available():
        print("❌ Java not found. JDBC tests require Java.")
        print(get_java_installation_instructions())
        sys.exit(1)
    
    if not JDBC_DRIVER_PATH.exists():
        print(f"❌ PostgreSQL JDBC driver not found at {JDBC_DRIVER_PATH}")
        print("Please run: make setup-jdbc-drivers")
        sys.exit(1)
    
    print("✅ Java is available")
    print(f"✅ PostgreSQL JDBC driver v{POSTGRESQL_DRIVER_VERSION} found")
    print("✅ All JDBC integration components are ready!")
    print("Run: pytest tests/integration/test_postgresql_jdbc.py -v") 