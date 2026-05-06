import pytest
import sys
import os

# Add pipeline to path for all tests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

@pytest.fixture(scope="session")
def test_data_dir():
    """Get the test data directory"""
    return os.path.join(os.path.dirname(__file__), "..", "data")

@pytest.fixture(scope="session")
def output_dir():
    """Get the output directory"""
    return os.path.join(os.path.dirname(__file__), "..", "..", "output")
