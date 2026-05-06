import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
import tempfile
import os
import sys

# Add pipeline to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.silver.silver_layer import SilverLayer

@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = (SparkSession.builder
            .appName("Test_Silver_Layer")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
            .getOrCreate())
    yield spark
    spark.stop()

@pytest.fixture
def silver_layer(spark_session):
    """Create SilverLayer instance for testing"""
    return SilverLayer(spark_session)

@pytest.fixture
def sample_bronze_data(spark_session):
    """Create sample bronze data for testing"""
    schema = StructType([
        StructField("patient_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("diagnosis", StringType(), True),
        StructField("visit_date", DateType(), True),
        StructField("billing_amount", DoubleType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("source_file", StringType(), True)
    ])
    
    data = [
        (1, "Alice", 30, "Diabetes", "2024-01-01", 200.0, "2024-01-01 10:00:00", "patients.csv"),
        (2, "Bob", 45, "Cardiac", "2024-01-01", 500.0, "2024-01-01 10:00:00", "patients.csv"),
        (3, "Charlie", None, "Diabetes", "2024-01-02", 300.0, "2024-01-01 10:00:00", "patients.csv"),
        (4, "David", 50, "Cardiac", "2024-01-03", None, "2024-01-01 10:00:00", "patients.csv"),
        (5, "Eve", 35, "Hypertension", "2024-01-04", 150.0, "2024-01-01 10:00:00", "patients.csv"),
        (2, "Bob", 45, "Cardiac", "2024-01-01", 500.0, "2024-01-01 11:00:00", "patients.csv"),  # Duplicate
        (6, "Frank", 200, "Diabetes", "2024-01-05", 400.0, "2024-01-01 10:00:00", "patients.csv"),  # Invalid age
        (7, "Grace", 25, "Cardiac", "2024-01-06", -100.0, "2024-01-01 10:00:00", "patients.csv")  # Negative billing
    ]
    
    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def temp_silver_path():
    """Create temporary silver directory for testing"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)

class TestSilverLayer:
    """Test cases for SilverLayer"""
    
    def test_clean_data(self, silver_layer, sample_bronze_data):
        """Test data cleaning functionality"""
        df_clean = silver_layer.clean_data(sample_bronze_data)
        
        # Check null handling
        assert df_clean.filter(col("age").isNull()).count() == 0
        assert df_clean.filter(col("billing_amount").isNull()).count() == 0
        assert df_clean.filter(col("diagnosis").isNull()).count() == 0
        assert df_clean.filter(col("name").isNull()).count() == 0
        
        # Check string cleaning
        first_row = df_clean.filter(col("patient_id") == 3).first()
        assert first_row.age == 0  # Null age replaced with 0
        assert first_row.diagnosis == "DIABETES"  # Upper case
        
        # Check processing timestamp added
        assert "silver_processed_timestamp" in df_clean.columns
    
    def test_remove_duplicates(self, silver_layer, sample_bronze_data):
        """Test duplicate removal"""
        df_clean = silver_layer.clean_data(sample_bronze_data)
        df_deduped = silver_layer.remove_duplicates(df_clean)
        
        # Should remove duplicate patient_id 2
        original_count = df_clean.count()
        deduped_count = df_deduped.count()
        
        assert deduped_count < original_count
        
        # Check no duplicates remain
        patient_ids = [row.patient_id for row in df_deduped.select("patient_id").collect()]
        assert len(patient_ids) == len(set(patient_ids))
    
    def test_validate_data(self, silver_layer, sample_bronze_data):
        """Test data validation"""
        df_clean = silver_layer.clean_data(sample_bronze_data)
        df_validated = silver_layer.validate_data(df_clean)
        
        # Should return the same DataFrame (validation passes for cleaned data)
        assert df_validated.count() == df_clean.count()
    
    def test_process_silver_layer_complete_flow(self, silver_layer, sample_bronze_data, temp_silver_path):
        """Test complete silver layer processing"""
        # Setup temporary paths
        temp_bronze_dir = tempfile.mkdtemp()
        silver_layer.bronze_path = temp_bronze_dir
        silver_layer.silver_path = temp_silver_path
        
        try:
            # Write bronze data
            sample_bronze_data.write.mode("overwrite").parquet(temp_bronze_dir)
            
            # Process silver layer
            df_silver = silver_layer.process_silver_layer()
            
            # Verify results
            assert df_silver.count() > 0
            assert "silver_processed_timestamp" in df_silver.columns
            
            # Verify data was written
            df_read = silver_layer.spark.read.parquet(temp_silver_path)
            assert df_read.count() == df_silver.count()
            
        finally:
            import shutil
            shutil.rmtree(temp_bronze_dir, ignore_errors=True)
    
    def test_data_quality_rules(self, silver_layer, sample_bronze_data):
        """Test data quality rules"""
        df_clean = silver_layer.clean_data(sample_bronze_data)
        
        # Check invalid ages are removed
        invalid_ages = df_clean.filter((col("age") < 0) | (col("age") > 150))
        assert invalid_ages.count() == 0
        
        # Check negative billing amounts are removed
        negative_billing = df_clean.filter(col("billing_amount") < 0)
        assert negative_billing.count() == 0
        
        # Check string fields are trimmed and uppercased
        first_row = df_clean.filter(col("patient_id") == 1).first()
        assert first_row.name == "Alice"  # Trimmed
        assert first_row.diagnosis == "DIABETES"  # Upper case

# Import required functions
from pyspark.sql.functions import col
