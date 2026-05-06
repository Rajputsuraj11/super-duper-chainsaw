from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col
from pyspark.sql.types import IntegerType, StringType
import logging

logger = logging.getLogger(__name__)

class PipelineOptimizer:
    """Optimization utilities for PySpark operations"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        
    def enable_spark_optimizations(self):
        """Enable Spark performance optimizations"""
        optimizations = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.inMemoryColumnarStorage.compressed": "true",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.parquet.filterPushdown": "true",
            "spark.sql.orc.filterPushdown": "true"
        }
        
        for key, value in optimizations.items():
            self.spark.conf.set(key, value)
            logger.info(f"Set optimization: {key} = {value}")
    
    def optimize_dataframe(self, df: DataFrame, cache_hint: str = None) -> DataFrame:
        """Apply DataFrame optimizations"""
        # Repartition if needed
        if df.rdd.getNumPartitions() > 100:
            df = df.coalesce(100)
            logger.info("Coalesced DataFrame to 100 partitions")
        
        # Cache if hint provided
        if cache_hint:
            df.cache()
            df.count()  # Materialize cache
            logger.info(f"Cached DataFrame with hint: {cache_hint}")
        
        return df
    
    def broadcast_join(self, large_df: DataFrame, small_df: DataFrame, join_key: str) -> DataFrame:
        """Perform optimized broadcast join"""
        # Estimate DataFrame sizes
        large_count = large_df.count()
        small_count = small_df.count()
        
        # Use broadcast if small_df is significantly smaller
        if small_count < large_count * 0.1:  # Less than 10% of large DataFrame
            logger.info(f"Using broadcast join (small: {small_count}, large: {large_count})")
            return large_df.join(broadcast(small_df), join_key)
        else:
            logger.info(f"Using regular join (small: {small_count}, large: {large_count})")
            return large_df.join(small_df, join_key)
    
    def partition_by_column(self, df: DataFrame, partition_col: str, output_path: str):
        """Write DataFrame with optimal partitioning"""
        # Determine optimal number of partitions
        row_count = df.count()
        optimal_partitions = max(1, min(200, row_count // 1000000))  # 1M rows per partition
        
        logger.info(f"Partitioning {row_count} rows into {optimal_partitions} partitions by {partition_col}")
        
        (df.repartition(optimal_partitions, col(partition_col))
         .write
         .mode("overwrite")
         .partitionBy(partition_col)
         .parquet(output_path))
    
    def optimize_aggregation(self, df: DataFrame, group_cols: list, agg_cols: list) -> DataFrame:
        """Optimize aggregation operations"""
        # Pre-filter to reduce data size
        for agg_col in agg_cols:
            df = df.filter(col(agg_col).isNotNull())
        
        # Use reduceByKey-like operations for better performance
        from pyspark.sql.functions import sum, avg, count, min, max
        
        agg_exprs = []
        for agg_col in agg_cols:
            agg_exprs.extend([
                sum(agg_col).alias(f"total_{agg_col}"),
                avg(agg_col).alias(f"avg_{agg_col}"),
                count(agg_col).alias(f"count_{agg_col}")
            ])
        
        return df.groupBy(*group_cols).agg(*agg_exprs)
    
    def create_optimized_temp_view(self, df: DataFrame, view_name: str):
        """Create optimized temporary view"""
        # Optimize DataFrame before creating view
        df = self.optimize_dataframe(df, f"temp_view_{view_name}")
        
        df.createOrReplaceTempView(view_name)
        logger.info(f"Created optimized temporary view: {view_name}")
    
    def analyze_query_plan(self, df: DataFrame):
        """Analyze and log query execution plan"""
        explain_output = df.explain(extended=True)
        logger.info("Query execution plan:")
        logger.info(explain_output)
        
        # Check for common optimization opportunities
        plan_str = str(explain_output)
        if "BroadcastHashJoin" not in plan_str and "SortMergeJoin" in plan_str:
            logger.warning("Consider using broadcast join for better performance")
        
        if "FileScan" in plan_str and "PushedFilters" not in plan_str:
            logger.warning("Predicate pushdown not applied - consider filtering earlier")
    
    def monitor_performance(self, operation_name: str, df: DataFrame):
        """Monitor and log performance metrics"""
        import time
        
        start_time = time.time()
        count = df.count()
        end_time = time.time()
        
        duration = end_time - start_time
        throughput = count / duration if duration > 0 else 0
        
        logger.info(f"Performance - {operation_name}: {count} rows in {duration:.2f}s ({throughput:.0f} rows/sec)")
        
        return {
            "operation": operation_name,
            "row_count": count,
            "duration_seconds": duration,
            "throughput_rows_per_sec": throughput
        }
