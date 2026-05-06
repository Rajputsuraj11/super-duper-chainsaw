try:
    from pyspark.sql import SparkSession
    from pipeline.bronze.bronze_layer import BronzeLayer
    from pipeline.silver.silver_layer import SilverLayer
    from pipeline.gold.gold_layer import GoldLayer
    DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Missing dependencies - {e}")
    print("Please install requirements: pip install -r requirements.txt")
    DEPENDENCIES_AVAILABLE = False
    SparkSession = None
    BronzeLayer = None
    SilverLayer = None
    GoldLayer = None

import logging
import sys
import traceback
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self):
        self.spark = None
        self.bronze_layer = None
        self.silver_layer = None
        self.gold_layer = None
        
    def create_spark_session(self):
        """Create Spark session with Delta support and optimizations"""
        try:
            self.spark = (SparkSession.builder
                .appName("Healthcare_Medallion_Pipeline")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate())
            
            logger.info("Spark session created successfully")
            return self.spark
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {str(e)}")
            raise
    
    def initialize_layers(self):
        """Initialize all pipeline layers"""
        try:
            self.bronze_layer = BronzeLayer(self.spark)
            self.silver_layer = SilverLayer(self.spark)
            self.gold_layer = GoldLayer(self.spark)
            logger.info("All layers initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize layers: {str(e)}")
            raise
    
    def run_bronze_layer(self, csv_path, last_run_date=None):
        """Execute bronze layer with error handling"""
        try:
            logger.info("Starting Bronze Layer Processing")
            df_bronze = self.bronze_layer.run_bronze_ingestion(csv_path, last_run_date)
            logger.info(f"Bronze layer completed. Processed {df_bronze.count()} records")
            return df_bronze
            
        except Exception as e:
            logger.error(f"Bronze layer failed: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def run_silver_layer(self):
        """Execute silver layer with error handling"""
        try:
            logger.info("Starting Silver Layer Processing")
            df_silver = self.silver_layer.process_silver_layer()
            logger.info(f"Silver layer completed. Processed {df_silver.count()} records")
            return df_silver
            
        except Exception as e:
            logger.error(f"Silver layer failed: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def run_gold_layer(self):
        """Execute gold layer with error handling"""
        try:
            logger.info("Starting Gold Layer Processing")
            df_gold = self.gold_layer.process_gold_layer()
            insights = self.gold_layer.get_gold_insights()
            logger.info(f"Gold layer completed. Generated insights for {df_gold.count()} diagnoses")
            return df_gold, insights
            
        except Exception as e:
            logger.error(f"Gold layer failed: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def run_full_pipeline(self, csv_path, last_run_date=None):
        """Execute complete pipeline with error handling and recovery"""
        pipeline_start_time = datetime.now()
        
        try:
            logger.info("=" * 50)
            logger.info("STARTING HEALTHCARE MEDALLION PIPELINE")
            logger.info("=" * 50)
            
            # Create Spark session
            self.create_spark_session()
            
            # Initialize layers
            self.initialize_layers()
            
            # Execute pipeline layers
            df_bronze = self.run_bronze_layer(csv_path, last_run_date)
            df_silver = self.run_silver_layer()
            df_gold, insights = self.run_gold_layer()
            
            # Print results
            self.print_pipeline_results(insights)
            
            pipeline_end_time = datetime.now()
            duration = pipeline_end_time - pipeline_start_time
            
            logger.info("=" * 50)
            logger.info(f"PIPELINE COMPLETED SUCCESSFULLY in {duration}")
            logger.info("=" * 50)
            
            return True, insights
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            logger.error(traceback.format_exc())
            return False, None
            
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")
    
    def print_pipeline_results(self, insights):
        """Print pipeline results and insights"""
        if not insights:
            logger.warning("No insights available")
            return
        
        print("\n" + "=" * 50)
        print("PIPELINE RESULTS & INSIGHTS")
        print("=" * 50)
        print(f"Top Diagnosis: {insights['top_diagnosis']}")
        print(f"Top Billing Amount: ${insights['top_billing']:,.2f}")
        print(f"Total Revenue: ${insights['total_revenue']:,.2f}")
        print("\nPatient Distribution by Diagnosis:")
        for diagnosis, count in insights['patient_distribution']:
            print(f"  {diagnosis}: {count} patients")
        print("=" * 50)

def main():
    """Main execution function"""
    try:
        # Check dependencies
        if not DEPENDENCIES_AVAILABLE:
            print("Cannot run pipeline - missing dependencies")
            print("Run: pip install -r requirements.txt")
            return 1
        
        # Initialize pipeline
        pipeline = PipelineOrchestrator()
        
        # Configuration
        csv_path = "data/patients.csv"
        last_run_date = None  # Set to "2024-01-01" for incremental loading
        
        # Run pipeline
        success, insights = pipeline.run_full_pipeline(csv_path, last_run_date)
        
        if success:
            print("Pipeline executed successfully!")
            return 0
        else:
            print("Pipeline execution failed!")
            return 1
            
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        return 1

if __name__ == "__main__":
    main()
