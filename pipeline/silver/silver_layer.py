from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, upper, regexp_replace, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
import logging

logger = logging.getLogger(__name__)

class SilverLayer:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.silver_path = "output/silver"
        self.bronze_path = "output/bronze"
    
    def clean_data(self, df):
        """Clean and standardize data"""
        # Handle null values
        df = df.fillna({
            "billing_amount": 0.0,
            "age": 0,
            "diagnosis": "Unknown",
            "name": "Unknown"
        })
        
        # Clean string fields
        df = df.withColumn("name", trim(col("name")))
        df = df.withColumn("diagnosis", upper(trim(col("diagnosis"))))
        
        # Remove invalid ages
        df = df.filter((col("age") >= 0) & (col("age") <= 150))
        
        # Remove negative billing amounts
        df = df.filter(col("billing_amount") >= 0)
        
        # Add processing timestamp
        df = df.withColumn("silver_processed_timestamp", current_timestamp())
        
        return df
    
    def remove_duplicates(self, df):
        """Remove duplicate records based on patient_id"""
        return df.dropDuplicates(["patient_id"])
    
    def validate_data(self, df):
        """Validate data quality"""
        # Check for required fields
        required_fields = ["patient_id", "name", "diagnosis", "visit_date"]
        
        for field in required_fields:
            null_count = df.filter(col(field).isNull()).count()
            if null_count > 0:
                logger.warning(f"Found {null_count} null values in {field}")
        
        return df
    
    def process_silver_layer(self):
        """Complete silver layer processing"""
        try:
            # Read from bronze
            df_bronze = (self.spark.read
                        .option("mergeSchema", "true")
                        .parquet(self.bronze_path))
            
            # Clean data
            df_clean = self.clean_data(df_bronze)
            
            # Remove duplicates
            df_deduped = self.remove_duplicates(df_clean)
            
            # Validate data
            df_validated = self.validate_data(df_deduped)
            
            # Write to silver with partitioning
            (df_validated.write
             .mode("overwrite")
             .partitionBy("diagnosis")
             .parquet(self.silver_path))
            
            logger.info("Silver layer processing completed")
            return df_validated
            
        except Exception as e:
            logger.error(f"Silver layer processing failed: {str(e)}")
            raise
