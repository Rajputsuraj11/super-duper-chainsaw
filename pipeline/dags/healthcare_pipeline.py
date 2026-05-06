from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime
from typing import Optional, Dict, Any
import sys
import os

# Add the pipeline directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.bronze.bronze_layer import BronzeLayer
from pipeline.silver.silver_layer import SilverLayer
from pipeline.gold.gold_layer import GoldLayer
from pyspark.sql import SparkSession

@task(
    name="Create Spark Session",
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    retries=3,
    retry_delay_seconds=30
)
def create_spark_session_task():
    """Create and return Spark session"""
    logger = get_run_logger()
    try:
        spark = (SparkSession.builder
                .appName("Healthcare_Prefect_Pipeline")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate())
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

@task(
    name="Bronze Layer Processing",
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=30),
    retries=2,
    retry_delay_seconds=60
)
def bronze_layer_task(spark: SparkSession, csv_path: str, last_run_date: Optional[str] = None):
    """Execute bronze layer processing"""
    logger = get_run_logger()
    try:
        bronze_layer = BronzeLayer(spark)
        df_bronze = bronze_layer.run_bronze_ingestion(csv_path, last_run_date)
        record_count = df_bronze.count()
        logger.info(f"Bronze layer completed. Processed {record_count} records")
        return {"status": "success", "record_count": record_count}
    except Exception as e:
        logger.error(f"Bronze layer failed: {str(e)}")
        raise

@task(
    name="Silver Layer Processing",
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=30),
    retries=2,
    retry_delay_seconds=60
)
def silver_layer_task(spark: SparkSession):
    """Execute silver layer processing"""
    logger = get_run_logger()
    try:
        silver_layer = SilverLayer(spark)
        df_silver = silver_layer.process_silver_layer()
        record_count = df_silver.count()
        logger.info(f"Silver layer completed. Processed {record_count} records")
        return {"status": "success", "record_count": record_count}
    except Exception as e:
        logger.error(f"Silver layer failed: {str(e)}")
        raise

@task(
    name="Gold Layer Processing",
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=30),
    retries=2,
    retry_delay_seconds=60
)
def gold_layer_task(spark: SparkSession):
    """Execute gold layer processing"""
    logger = get_run_logger()
    try:
        gold_layer = GoldLayer(spark)
        df_gold = gold_layer.process_gold_layer()
        insights = gold_layer.get_gold_insights()
        record_count = df_gold.count()
        logger.info(f"Gold layer completed. Generated insights for {record_count} diagnoses")
        return {
            "status": "success", 
            "record_count": record_count,
            "insights": insights
        }
    except Exception as e:
        logger.error(f"Gold layer failed: {str(e)}")
        raise

@task(
    name="Cleanup Spark Session",
    always_run=True
)
def cleanup_spark_task(spark: SparkSession):
    """Cleanup Spark session"""
    logger = get_run_logger()
    try:
        if spark:
            spark.stop()
            logger.info("Spark session stopped successfully")
    except Exception as e:
        logger.warning(f"Error stopping Spark session: {str(e)}")

@task(
    name="Generate Pipeline Report"
)
def generate_report_task(
    bronze_result: Dict[str, Any],
    silver_result: Dict[str, Any],
    gold_result: Dict[str, Any]
):
    """Generate pipeline execution report"""
    logger = get_run_logger()
    try:
        report = f"""
        PIPELINE EXECUTION REPORT
        =========================
        Bronze Layer: {bronze_result.get('status', 'unknown')} - {bronze_result.get('record_count', 0)} records
        Silver Layer: {silver_result.get('status', 'unknown')} - {silver_result.get('record_count', 0)} records
        Gold Layer: {gold_result.get('status', 'unknown')} - {gold_result.get('record_count', 0)} records
        
        BUSINESS INSIGHTS:
        """
        
        insights = gold_result.get('insights', {})
        if insights:
            report += f"""
        Top Diagnosis: {insights.get('top_diagnosis', 'N/A')}
        Top Billing Amount: ${insights.get('top_billing', 0):,.2f}
        Total Revenue: ${insights.get('total_revenue', 0):,.2f}
        
        Patient Distribution:
        """
            for diagnosis, count in insights.get('patient_distribution', []):
                report += f"  {diagnosis}: {count} patients\n"
        
        logger.info(report)
        return report
        
    except Exception as e:
        logger.error(f"Failed to generate report: {str(e)}")
        raise

@flow(
    name="Healthcare Medallion Pipeline",
    description="Complete healthcare data pipeline with Bronze, Silver, and Gold layers",
    log_prints=True
)
def healthcare_pipeline_flow(
    csv_path: str = "data/patients.csv",
    last_run_date: Optional[str] = None
):
    """
    Main healthcare pipeline flow
    
    Args:
        csv_path: Path to the source CSV file
        last_run_date: Optional date for incremental processing
    """
    logger = get_run_logger()
    
    # Create Spark session
    spark = create_spark_session_task()
    
    try:
        # Execute pipeline layers
        bronze_result = bronze_layer_task.submit(spark, csv_path, last_run_date)
        silver_result = silver_layer_task.submit(spark)
        gold_result = gold_layer_task.submit(spark)
        
        # Wait for all tasks to complete
        bronze_result.wait()
        silver_result.wait()
        gold_result.wait()
        
        # Generate report
        report = generate_report_task(
            bronze_result.result(),
            silver_result.result(),
            gold_result.result()
        )
        
        logger.info("Pipeline execution completed successfully!")
        return {
            "status": "success",
            "bronze": bronze_result.result(),
            "silver": silver_result.result(),
            "gold": gold_result.result(),
            "report": report
        }
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise
    
    finally:
        # Always cleanup Spark session
        cleanup_spark_task.submit(spark)

@flow(
    name="Incremental Healthcare Pipeline",
    description="Incremental healthcare data pipeline for daily processing"
)
def incremental_healthcare_pipeline(
    csv_path: str = "data/patients.csv",
    days_back: int = 1
):
    """
    Incremental pipeline that processes data from the last N days
    
    Args:
        csv_path: Path to the source CSV file
        days_back: Number of days to look back for incremental processing
    """
    from datetime import datetime, timedelta
    
    # Calculate last run date
    last_run_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    
    logger = get_run_logger()
    logger.info(f"Running incremental pipeline for data since {last_run_date}")
    
    return healthcare_pipeline_flow(csv_path=csv_path, last_run_date=last_run_date)

if __name__ == "__main__":
    # Example usage for local testing
    healthcare_pipeline_flow()
