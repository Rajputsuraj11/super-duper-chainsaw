from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeLayer:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.bronze_path = "output/bronze"
        
    def create_spark_session(self):
        """Create and return Spark session with Delta support"""
        return (SparkSession.builder
                .appName("Healthcare_Bronze_Layer")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate())
    
    def define_schema(self):
        """Define the expected schema for patient data"""
        return StructType([
            StructField("patient_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("diagnosis", StringType(), True),
            StructField("visit_date", DateType(), True),
            StructField("billing_amount", DoubleType(), True)
        ])
    
    def ingest_csv(self, csv_path, last_run_date=None):
        """
        Ingest CSV data with optional incremental loading
        
        Args:
            csv_path: Path to the CSV file
            last_run_date: Optional date for incremental loading
        
        Returns:
            DataFrame with ingested data
        """
        try:
            logger.info(f"Starting ingestion from {csv_path}")
            
            # Read CSV with defined schema
            df = (self.spark.read
                  .option("header", "true")
                  .schema(self.define_schema())
                  .csv(csv_path))
            
            # Add metadata columns
            df = df.withColumn("ingestion_timestamp", current_timestamp())
            df = df.withColumn("source_file", lit(os.path.basename(csv_path)))
            
            # Apply incremental filtering if last_run_date is provided
            if last_run_date:
                logger.info(f"Applying incremental filter for dates > {last_run_date}")
                df = df.filter(col("visit_date") > lit(last_run_date))
            
            logger.info(f"Successfully ingested {df.count()} records")
            return df
            
        except Exception as e:
            logger.error(f"Error ingesting CSV data: {str(e)}")
            raise
    
    def write_to_bronze(self, df, mode="append"):
        """
        Write DataFrame to bronze layer with partitioning
        
        Args:
            df: DataFrame to write
            mode: Write mode (append/overwrite)
        """
        try:
            logger.info(f"Writing data to bronze layer at {self.bronze_path}")
            
            # Write with partitioning by visit_date for optimization
            (df.write
             .mode(mode)
             .partitionBy("visit_date")
             .parquet(self.bronze_path))
            
            logger.info("Successfully wrote data to bronze layer")
            
        except Exception as e:
            logger.error(f"Error writing to bronze layer: {str(e)}")
            raise
    
    def read_from_bronze(self, start_date=None, end_date=None):
        """
        Read data from bronze layer with optional date filtering
        
        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter
        
        Returns:
            DataFrame from bronze layer
        """
        try:
            logger.info("Reading data from bronze layer")
            
            # Read with schema evolution support
            df = (self.spark.read
                  .option("mergeSchema", "true")
                  .parquet(self.bronze_path))
            
            # Apply date filters if provided
            if start_date:
                df = df.filter(col("visit_date") >= lit(start_date))
            if end_date:
                df = df.filter(col("visit_date") <= lit(end_date))
            
            logger.info(f"Successfully read {df.count()} records from bronze layer")
            return df
            
        except Exception as e:
            logger.error(f"Error reading from bronze layer: {str(e)}")
            raise
    
    def run_bronze_ingestion(self, csv_path, last_run_date=None):
        """
        Complete bronze layer ingestion process
        
        Args:
            csv_path: Path to source CSV
            last_run_date: Optional date for incremental loading
        """
        try:
            # Ingest data
            df = self.ingest_csv(csv_path, last_run_date)
            
            # Write to bronze layer
            self.write_to_bronze(df)
            
            logger.info("Bronze layer ingestion completed successfully")
            return df
            
        except Exception as e:
            logger.error(f"Bronze layer ingestion failed: {str(e)}")
            raise

if __name__ == "__main__":
    # Example usage
    bronze = BronzeLayer(None)
    spark = bronze.create_spark_session()
    bronze.spark = spark
    
    try:
        # Run ingestion
        df = bronze.run_bronze_ingestion("data/patients.csv")
        df.show()
        
    finally:
        spark.stop()
