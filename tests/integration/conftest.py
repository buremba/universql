import os
import pytest
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def pytest_configure(config):
    """Configure pytest for PostgreSQL integration tests."""
    config.addinivalue_line(
        "markers", "postgresql: marks tests as PostgreSQL integration tests"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running"
    )


def pytest_collection_modifyitems(config, items):
    """Automatically mark PostgreSQL tests."""
    for item in items:
        if "test_postgresql" in item.nodeid:
            item.add_marker(pytest.mark.postgresql)
        if "proxy" in item.nodeid or "universql" in item.nodeid:
            item.add_marker(pytest.mark.slow)


@pytest.fixture(scope="session")
def postgresql_url():
    """Provide PostgreSQL connection URL from environment."""
    url = os.getenv("POSTGRESQL_URL")
    if not url:
        pytest.skip("POSTGRESQL_URL environment variable not set")
    return url


@pytest.fixture(scope="session")
def check_postgresql_available(postgresql_url):
    """Check if PostgreSQL server is available before running tests."""
    import psycopg2
    from urllib.parse import urlparse
    
    try:
        parsed_url = urlparse(postgresql_url)
        conn = psycopg2.connect(
            host=parsed_url.hostname,
            port=parsed_url.port or 5432,
            database=parsed_url.path.lstrip('/') if parsed_url.path else 'postgres',
            user=parsed_url.username,
            password=parsed_url.password,
        )
        conn.close()
        return True
    except Exception as e:
        pytest.skip(f"PostgreSQL server not available: {e}")


@pytest.fixture
def postgresql_connection(check_postgresql_available, postgresql_url):
    """Provide a direct PostgreSQL connection for testing."""
    import psycopg2
    from urllib.parse import urlparse
    
    parsed_url = urlparse(postgresql_url)
    conn = psycopg2.connect(
        host=parsed_url.hostname,
        port=parsed_url.port or 5432,
        database=parsed_url.path.lstrip('/') if parsed_url.path else 'postgres',
        user=parsed_url.username,
        password=parsed_url.password,
    )
    
    try:
        yield conn
    finally:
        conn.close() 