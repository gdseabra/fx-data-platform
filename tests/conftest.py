# Test configuration and fixtures for FX Data Platform

import pytest
from pyspark.sql import SparkSession
import tempfile
import shutil


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .master("local[4]") \
        .appName("fx-data-platform-tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def temp_dir():
    """Create and cleanup temporary directory."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_transaction_data(spark):
    """Create sample transaction data for tests."""
    data = [
        (1, "user1", "2024-03-15 10:00:00", "BUY", "USD", 1000.00, 5000.00, 5.0, 0.1, 10.0, "completed", "web", "device1"),
        (2, "user2", "2024-03-15 11:00:00", "SELL", "EUR", 500.00, 5500.00, 11.0, 0.05, 5.0, "completed", "mobile", "device2"),
        (3, "user1", "2024-03-15 12:00:00", "BUY", "GBP", 750.00, 9375.00, 12.5, 0.15, 7.50, "completed", "api", "device1"),
    ]
    
    columns = ["id", "user_id", "timestamp", "transaction_type", "currency", "amount_brl", 
               "amount_foreign", "exchange_rate", "spread_pct", "fee_brl", "status", "channel", "device"]
    
    return spark.createDataFrame(data, schema=columns)
