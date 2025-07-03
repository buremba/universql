import logging
import pytest

@pytest.fixture(autouse=True)
def configure_logging(caplog):
    """Configure logging for each test to ensure output is captured"""
    caplog.set_level(logging.INFO)
    return caplog 