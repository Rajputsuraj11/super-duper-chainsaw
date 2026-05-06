import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
import tempfile
import os
import sys

# Add pipeline to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.gold.gold_layer import GoldLayer

@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = (SparkSession.builder
            .appName("Test_Gold_Layer")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
            .getOrCreate())
    yield spark
    spark.stop()

@pytest.fixture
def gold_layer(spark_session):
    """Create GoldLayer instance for testing"""
    return GoldLayer(spark_session)

@pytest.fixture
def sample_silver_data(spark_session):
    """Create sample silver data for testing"""
    schema = StructType([
        StructField("patient_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("diagnosis", StringType(), True),
        StructField("visit_date", DateType(), True),
        StructField("billing_amount", DoubleType(), True),
        StructField("silver_processed_timestamp", StringType(), True)
    ])
    
    data = [
        (1, "Alice", 30, "DIABETES", "2024-01-01", 200.0, "2024-01-01 10:00:00"),
        (2, "Bob", 45, "CARDIAC", "2024-01-01", 500.0, "2024-01-01 10:00:00"),
        (3, "Charlie", 35, "DIABETES", "2024-01-02", 300.0, "2024-01-01 10:00:00"),
        (4, "David", 50, "CARDIAC", "2024-01-03", 600.0, "2024-01-01 10:00:00"),
        (5, "Eve", 35, "HYPERTENSION", "2024-01-04", 150.0, "2024-01-01 10:00:00"),
        (6, "Frank", 60, "DIABETES", "2024-01-05", 400.0, "2024-01-01 10:00:00"),
        (7, "Grace", 25, "CARDIAC", "2024-01-06", 550.0, "2024-01-01 10:00:00"),
        (8, "Henry", 40, "HYPERTENSION", "2024-01-07", 250.0, "2024-01-01 10:00:00"),
        (9, "Iris", 55, "DIABETES", "2024-01-08", 350.0, "2024-01-01 10:00:00"),
        (10, "Jack", 45, "CARDIAC", "2024-01-09", 450.0, "2024-01-01 10:00:00")
    ]
    
    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def temp_gold_path():
    """Create temporary gold directory for testing"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)

class TestGoldLayer:
    """Test cases for GoldLayer"""
    
    def test_aggregate_by_diagnosis(self, gold_layer, sample_silver_data):
        """Test aggregation by diagnosis"""
        df_agg = gold_layer.aggregate_by_diagnosis(sample_silver_data)
        
        # Should have 3 unique diagnoses
        assert df_agg.count() == 3
        
        # Check required columns exist
        required_columns = [
            "diagnosis", "total_billing", "avg_billing", "patient_count",
            "min_age", "max_age", "avg_age", "first_visit", "last_visit",
            "gold_processed_timestamp"
        ]
        
        for col_name in required_columns:
            assert col_name in df_agg.columns
        
        # Verify aggregation calculations
        diabetes_row = df_agg.filter(col("diagnosis") == "DIABETES").first()
        assert diabetes_row.patient_count == 4
        assert diabetes_row.total_billing == 1250.0  # 200 + 300 + 400 + 350
        assert diabetes_row.avg_billing == 312.5  # 1250 / 4
        
        cardiac_row = df_agg.filter(col("diagnosis") == "CARDIAC").first()
        assert cardiac_row.patient_count == 4
        assert cardiac_row.total_billing == 2100.0  # 500 + 600 + 550 + 450
        
        hypertension_row = df_agg.filter(col("diagnosis") == "HYPERTENSION").first()
        assert hypertension_row.patient_count == 2
        assert hypertension_row.total_billing == 400.0  # 150 + 250
    
    def test_create_billing_rankings(self, gold_layer, sample_silver_data):
        """Test billing rankings creation"""
        df_agg = gold_layer.aggregate_by_diagnosis(sample_silver_data)
        df_ranked = gold_layer.create_billing_rankings(df_agg)
        
        # Check ranking column exists
        assert "billing_rank" in df_ranked.columns
        
        # Verify rankings (CARDIAC should be #1 with highest billing)
        cardiac_row = df_ranked.filter(col("diagnosis") == "CARDIAC").first()
        diabetes_row = df_ranked.filter(col("diagnosis") == "DIABETES").first()
        hypertension_row = df_ranked.filter(col("diagnosis") == "HYPERTENSION").first()
        
        assert cardiac_row.billing_rank == 1
        assert diabetes_row.billing_rank == 2
        assert hypertension_row.billing_rank == 3
    
    def test_process_gold_layer_complete_flow(self, gold_layer, sample_silver_data, temp_gold_path):
        """Test complete gold layer processing"""
        # Setup temporary paths
        temp_silver_dir = tempfile.mkdtemp()
        gold_layer.silver_path = temp_silver_dir
        gold_layer.gold_path = temp_gold_path
        
        try:
            # Write silver data
            sample_silver_data.write.mode("overwrite").parquet(temp_silver_dir)
            
            # Process gold layer
            df_gold = gold_layer.process_gold_layer()
            
            # Verify results
            assert df_gold.count() == 3  # 3 diagnoses
            assert "billing_rank" in df_gold.columns
            assert "gold_processed_timestamp" in df_gold.columns
            
            # Verify data was written
            df_read = gold_layer.spark.read.parquet(temp_gold_path)
            assert df_read.count() == df_gold.count()
            
        finally:
            import shutil
            shutil.rmtree(temp_silver_dir, ignore_errors=True)
    
    def test_get_gold_insights(self, gold_layer, sample_silver_data, temp_gold_path):
        """Test getting business insights from gold layer"""
        # Setup temporary paths
        temp_silver_dir = tempfile.mkdtemp()
        gold_layer.silver_path = temp_silver_dir
        gold_layer.gold_path = temp_gold_path
        
        try:
            # Write silver data and process gold
            sample_silver_data.write.mode("overwrite").parquet(temp_silver_dir)
            gold_layer.process_gold_layer()
            
            # Get insights
            insights = gold_layer.get_gold_insights()
            
            # Verify insights structure
            assert "top_diagnosis" in insights
            assert "top_billing" in insights
            assert "total_revenue" in insights
            assert "patient_distribution" in insights
            
            # Verify insights values
            assert insights["top_diagnosis"] == "CARDIAC"
            assert insights["top_billing"] == 2100.0
            assert insights["total_revenue"] == 3750.0  # Sum of all billing
            assert len(insights["patient_distribution"]) == 3
            
            # Verify patient distribution
            dist_dict = dict(insights["patient_distribution"])
            assert dist_dict["DIABETES"] == 4
            assert dist_dict["CARDIAC"] == 4
            assert dist_dict["HYPERTENSION"] == 2
            
        finally:
            import shutil
            shutil.rmtree(temp_silver_dir, ignore_errors=True)
    
    def test_aggregation_data_types(self, gold_layer, sample_silver_data):
        """Test aggregation data types and formatting"""
        df_agg = gold_layer.aggregate_by_diagnosis(sample_silver_data)
        
        # Check data types
        schema = df_agg.schema
        assert schema["total_billing"].dataType.typeName() == "double"
        assert schema["avg_billing"].dataType.typeName() == "double"
        assert schema["patient_count"].dataType.typeName() == "long"
        assert schema["min_age"].dataType.typeName() == "integer"
        assert schema["max_age"].dataType.typeName() == "integer"
        assert schema["avg_age"].dataType.typeName() == "double"
        
        # Check rounding
        first_row = df_agg.first()
        # avg_billing should be rounded to 2 decimal places
        avg_billing_str = str(first_row.avg_billing)
        assert len(avg_billing_str.split('.')[-1]) <= 2
        
        # avg_age should be rounded to 1 decimal place
        avg_age_str = str(first_row.avg_age)
        assert len(avg_age_str.split('.')[-1]) <= 1

# Import required functions
from pyspark.sql.functions import col
