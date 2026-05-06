from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, max, min, current_timestamp, round
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)

class GoldLayer:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.gold_path = "output/gold"
        self.silver_path = "output/silver"
    
    def aggregate_by_diagnosis(self, df):
        """Create aggregated metrics by diagnosis"""
        return (df.groupBy("diagnosis")
                .agg(
                    sum("billing_amount").alias("total_billing"),
                    avg("billing_amount").alias("avg_billing"),
                    count("patient_id").alias("patient_count"),
                    min("age").alias("min_age"),
                    max("age").alias("max_age"),
                    avg("age").alias("avg_age"),
                    min("visit_date").alias("first_visit"),
                    max("visit_date").alias("last_visit")
                )
                .withColumn("gold_processed_timestamp", current_timestamp())
                .withColumn("avg_billing", round("avg_billing", 2))
                .withColumn("avg_age", round("avg_age", 1)))
    
    def create_billing_rankings(self, df):
        """Create rankings by diagnosis"""
        window_spec = Window.orderBy(col("total_billing").desc())
        return df.withColumn("billing_rank", col("total_billing").rank().over(window_spec))
    
    def process_gold_layer(self):
        """Complete gold layer processing"""
        try:
            # Read from silver
            df_silver = (self.spark.read
                        .option("mergeSchema", "true")
                        .parquet(self.silver_path))
            
            # Create aggregations
            df_aggregated = self.aggregate_by_diagnosis(df_silver)
            
            # Add rankings
            df_gold = self.create_billing_rankings(df_aggregated)
            
            # Write to gold
            (df_gold.write
             .mode("overwrite")
             .parquet(self.gold_path))
            
            logger.info("Gold layer processing completed")
            return df_gold
            
        except Exception as e:
            logger.error(f"Gold layer processing failed: {str(e)}")
            raise
    
    def get_gold_insights(self):
        """Get business insights from gold layer"""
        try:
            df_gold = (self.spark.read
                      .parquet(self.gold_path))
            
            # Top billing diagnosis
            top_diagnosis = df_gold.orderBy(col("total_billing").desc()).first()
            
            # Total revenue
            total_revenue = df_gold.agg(sum("total_billing")).collect()[0][0]
            
            # Patient distribution
            patient_dist = df_gold.select("diagnosis", "patient_count").collect()
            
            insights = {
                "top_diagnosis": top_diagnosis["diagnosis"] if top_diagnosis else None,
                "top_billing": top_diagnosis["total_billing"] if top_diagnosis else 0,
                "total_revenue": total_revenue,
                "patient_distribution": [(row["diagnosis"], row["patient_count"]) for row in patient_dist]
            }
            
            return insights
            
        except Exception as e:
            logger.error(f"Error getting gold insights: {str(e)}")
            raise
