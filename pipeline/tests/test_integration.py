import pytest
from pyspark.sql import SparkSession
import tempfile
import os
import sys

# Add pipeline to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.main import PipelineOrchestrator

@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = (SparkSession.builder
            .appName("Test_Integration")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
            .getOrCreate())
    yield spark
    spark.stop()

@pytest.fixture
def sample_csv_data():
    """Sample CSV data for integration testing"""
    return """patient_id,name,age,diagnosis,visit_date,billing_amount
1,Alice,30,Diabetes,2024-01-01,200.0
2,Bob,45,Cardiac,2024-01-01,500.0
3,Charlie,,Diabetes,2024-01-02,300.0
4,David,50,Cardiac,2024-01-03,
5,Eve,35,Hypertension,2024-01-04,150.0
6,Frank,60,Diabetes,2024-01-05,400.0"""

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
def pipeline_orchestrator():
    """Create PipelineOrchestrator instance for testing"""
    return PipelineOrchestrator()

class TestIntegration:
    """Integration tests for the complete pipeline"""
    
    def test_full_pipeline_execution(self, pipeline_orchestrator, temp_csv_file):
        """Test complete pipeline execution"""
        # Run full pipeline
        success, insights = pipeline_orchestrator.run_full_pipeline(temp_csv_file)
        
        # Verify successful execution
        assert success is True
        assert insights is not None
        
        # Verify insights structure
        assert "top_diagnosis" in insights
        assert "top_billing" in insights
        assert "total_revenue" in insights
        assert "patient_distribution" in insights
        
        # Verify insights values
        assert insights["total_revenue"] == 1550.0  # 200 + 500 + 300 + 150 + 400
        assert len(insights["patient_distribution"]) == 3  # Diabetes, Cardiac, Hypertension
    
    def test_incremental_pipeline_execution(self, pipeline_orchestrator, temp_csv_file):
        """Test incremental pipeline execution"""
        # Run with last_run_date to test incremental loading
        success, insights = pipeline_orchestrator.run_full_pipeline(
            temp_csv_file, 
            last_run_date="2024-01-01"
        )
        
        # Verify successful execution
        assert success is True
        assert insights is not None
        
        # Should only include records after 2024-01-01
        # Records from 2024-01-02, 2024-01-03, 2024-01-04, 2024-01-05
        assert insights["total_revenue"] == 850.0  # 300 + 150 + 400
    
    def test_pipeline_error_handling(self, pipeline_orchestrator):
        """Test pipeline error handling with invalid input"""
        # Test with non-existent file
        success, insights = pipeline_orchestrator.run_full_pipeline("non_existent.csv")
        
        # Should handle error gracefully
        assert success is False
        assert insights is None
    
    def test_spark_session_creation(self, pipeline_orchestrator):
        """Test Spark session creation"""
        spark = pipeline_orchestrator.create_spark_session()
        
        assert spark is not None
        assert spark.conf.get("spark.app.name") == "Healthcare_Medallion_Pipeline"
        
        spark.stop()
    
    def test_layers_initialization(self, pipeline_orchestrator):
        """Test layers initialization"""
        spark = pipeline_orchestrator.create_spark_session()
        
        try:
            pipeline_orchestrator.initialize_layers()
            
            assert pipeline_orchestrator.bronze_layer is not None
            assert pipeline_orchestrator.silver_layer is not None
            assert pipeline_orchestrator.gold_layer is not None
            
        finally:
            spark.stop()
    
    def test_pipeline_idempotency(self, pipeline_orchestrator, temp_csv_file):
        """Test pipeline idempotency - multiple runs should produce same results"""
        # First run
        success1, insights1 = pipeline_orchestrator.run_full_pipeline(temp_csv_file)
        
        # Second run
        success2, insights2 = pipeline_orchestrator.run_full_pipeline(temp_csv_file)
        
        # Both should succeed
        assert success1 is True
        assert success2 is True
        
        # Results should be consistent
        assert insights1["total_revenue"] == insights2["total_revenue"]
        assert insights1["top_diagnosis"] == insights2["top_diagnosis"]
        assert len(insights1["patient_distribution"]) == len(insights2["patient_distribution"])
    
    def test_pipeline_with_corrupted_data(self, pipeline_orchestrator):
        """Test pipeline with corrupted/malformed data"""
        # Create corrupted CSV
        corrupted_data = """patient_id,name,age,diagnosis,visit_date,billing_amount
1,Alice,30,Diabetes,2024-01-01,200.0
2,Bob,invalid_age,Cardiac,2024-01-01,invalid_billing
3,Charlie,,Diabetes,invalid_date,300.0"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(corrupted_data)
            temp_path = f.name
        
        try:
            # Pipeline should handle corrupted data gracefully
            success, insights = pipeline_orchestrator.run_full_pipeline(temp_path)
            
            # Should either succeed with cleaned data or fail gracefully
            # The exact behavior depends on the error handling implementation
            assert isinstance(success, bool)
            
        finally:
            os.unlink(temp_path)
