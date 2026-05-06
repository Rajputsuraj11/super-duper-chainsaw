import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
import tempfile
import os
import sys

# Add pipeline to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.bronze.bronze_layer import BronzeLayer

@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = (SparkSession.builder
            .appName("Test_Bronze_Layer")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
            .getOrCreate())
    yield spark
    spark.stop()

@pytest.fixture
def bronze_layer(spark_session):
    """Create BronzeLayer instance for testing"""
    return BronzeLayer(spark_session)

@pytest.fixture
def sample_csv_data():
    """Sample CSV data for testing"""
    return """patient_id,name,age,diagnosis,visit_date,billing_amount
1,Alice,30,Diabetes,2024-01-01,200.0
2,Bob,45,Cardiac,2024-01-01,500.0
3,Charlie,,Diabetes,2024-01-02,300.0
4,David,50,Cardiac,2024-01-03,"""

@pytest.fixture
def temp_csv_file(sample_csv_data):
    """Create temporary CSV file for testing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(sample_csv_data)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    os.unlink(temp_path)

@pytest.fixture
def temp_bronze_path():
    """Create temporary bronze directory for testing"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)

class TestBronzeLayer:
    """Test cases for BronzeLayer"""
    
    def test_define_schema(self, bronze_layer):
        """Test schema definition"""
        schema = bronze_layer.define_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 6
        field_names = [field.name for field in schema.fields]
        expected_fields = ["patient_id", "name", "age", "diagnosis", "visit_date", "billing_amount"]
        
        for field in expected_fields:
            assert field in field_names
    
    def test_ingest_csv_success(self, bronze_layer, temp_csv_file):
        """Test successful CSV ingestion"""
        df = bronze_layer.ingest_csv(temp_csv_file)
        
        assert df.count() == 4
        assert "ingestion_timestamp" in df.columns
        assert "source_file" in df.columns
        
        # Check data types
        schema = df.schema
        assert schema["patient_id"].dataType.typeName() == "integer"
        assert schema["name"].dataType.typeName() == "string"
        assert schema["age"].dataType.typeName() == "integer"
        assert schema["billing_amount"].dataType.typeName() == "double"
    
    def test_ingest_csv_with_incremental_filter(self, bronze_layer, temp_csv_file):
        """Test CSV ingestion with incremental filtering"""
        df = bronze_layer.ingest_csv(temp_csv_file, last_run_date="2024-01-01")
        
        # Should only include records after 2024-01-01
        assert df.count() == 2  # Records from 2024-01-02 and 2024-01-03
    
    def test_ingest_csv_file_not_found(self, bronze_layer):
        """Test CSV ingestion with non-existent file"""
        with pytest.raises(Exception):
            bronze_layer.ingest_csv("non_existent_file.csv")
    
    def test_write_to_bronze(self, bronze_layer, temp_csv_file, temp_bronze_path):
        """Test writing to bronze layer"""
        bronze_layer.bronze_path = temp_bronze_path
        
        df = bronze_layer.ingest_csv(temp_csv_file)
        bronze_layer.write_to_bronze(df)
        
        # Verify data was written
        df_read = bronze_layer.spark.read.parquet(temp_bronze_path)
        assert df_read.count() == 4
    
    def test_read_from_bronze(self, bronze_layer, temp_csv_file, temp_bronze_path):
        """Test reading from bronze layer"""
        bronze_layer.bronze_path = temp_bronze_path
        
        # Write data first
        df = bronze_layer.ingest_csv(temp_csv_file)
        bronze_layer.write_to_bronze(df)
        
        # Read data back
        df_read = bronze_layer.read_from_bronze()
        assert df_read.count() == 4
    
    def test_read_from_bronze_with_date_filter(self, bronze_layer, temp_csv_file, temp_bronze_path):
        """Test reading from bronze layer with date filter"""
        bronze_layer.bronze_path = temp_bronze_path
        
        # Write data first
        df = bronze_layer.ingest_csv(temp_csv_file)
        bronze_layer.write_to_bronze(df)
        
        # Read with date filter
        df_read = bronze_layer.read_from_bronze(start_date="2024-01-02")
        assert df_read.count() == 2  # Records from 2024-01-02 and 2024-01-03
    
    def test_run_bronze_ingestion_complete_flow(self, bronze_layer, temp_csv_file, temp_bronze_path):
        """Test complete bronze ingestion flow"""
        bronze_layer.bronze_path = temp_bronze_path
        
        df = bronze_layer.run_bronze_ingestion(temp_csv_file)
        
        assert df.count() == 4
        assert "ingestion_timestamp" in df.columns
        assert "source_file" in df.columns
        
        # Verify data was written to bronze layer
        df_read = bronze_layer.spark.read.parquet(temp_bronze_path)
        assert df_read.count() == 4
    
    def test_schema_evolution_support(self, bronze_layer, temp_csv_file, temp_bronze_path):
        """Test schema evolution support"""
        bronze_layer.bronze_path = temp_bronze_path
        
        # Initial ingestion
        df1 = bronze_layer.run_bronze_ingestion(temp_csv_file)
        
        # Create new CSV with additional column
        new_data = """patient_id,name,age,diagnosis,visit_date,billing_amount,doctor_id
5,Eve,35,Hypertension,2024-01-04,150.0,DOC001"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(new_data)
            new_csv_path = f.name
        
        try:
            # Read with mergeSchema option
            df_combined = (bronze_layer.spark.read
                          .option("mergeSchema", "true")
                          .parquet(temp_bronze_path))
            
            # Should have the new column
            assert "doctor_id" in df_combined.columns
            
        finally:
            os.unlink(new_csv_path)
