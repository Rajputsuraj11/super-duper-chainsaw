# AI-Powered Healthcare Data Pipeline (Medallion Architecture)

A production-grade data pipeline built with PySpark implementing the Medallion Architecture (Bronze → Silver → Gold) for healthcare data processing and analytics.

## 🏥 Overview

This pipeline demonstrates best practices in data engineering including:
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated)
- **Incremental Processing**: Only process new data based on timestamps
- **Schema Evolution**: Handle changing data structures over time
- **Error Handling**: Robust error recovery and logging
- **Orchestration**: Prefect DAG for workflow management
- **Testing**: Comprehensive unit and integration tests
- **Optimization**: Partitioning, caching, and performance tuning

## 📊 Use Case

A hospital processes daily patient data to generate insights about:
- Patient demographics by diagnosis
- Billing analytics and revenue tracking
- Visit patterns and trends
- Healthcare service utilization

### Sample Data Flow

```
patients.csv (Raw Data)
    ↓
Bronze Layer (Raw Ingestion)
    ↓
Silver Layer (Cleaning & Validation)
    ↓
Gold Layer (Aggregated Insights)
    ↓
Business Analytics Dashboard
```

## 🏗️ Architecture

### Medallion Layers

#### Bronze Layer (Raw Data)
- **Purpose**: Raw data ingestion with minimal transformation
- **Features**: 
  - CSV ingestion with schema validation
  - Incremental loading based on visit_date
  - Metadata tracking (ingestion timestamps, source files)
  - Partitioning by visit_date for performance

#### Silver Layer (Cleaned Data)
- **Purpose**: Data cleaning, validation, and standardization
- **Features**:
  - Null value handling and imputation
  - Duplicate detection and removal
  - Data type validation and normalization
  - Quality checks and business rules

#### Gold Layer (Aggregated Insights)
- **Purpose**: Business-ready aggregated data
- **Features**:
  - Aggregations by diagnosis (billing, patient counts, demographics)
  - Rankings and KPI calculations
  - Time-based analytics (first/last visits, trends)
  - Business intelligence ready format

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Java 8+ (for PySpark)
- 8GB+ RAM recommended

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd healthcare-pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables (optional)**
   ```bash
   export SPARK_HOME=/path/to/spark
   export PYSPARK_PYTHON=python3
   ```

### Running the Pipeline

#### Option 1: Direct Execution
```bash
# Run the complete pipeline
python pipeline/main.py

# Run with incremental processing
python pipeline/main.py --last-run-date 2024-01-01
```

#### Option 2: Prefect Orchestration
```bash
# Start Prefect server
prefect server start

# Deploy the pipeline
prefect deploy pipeline/dags/healthcare_pipeline.py:healthcare_pipeline_flow

# Run the deployed flow
prefect deployment run 'Healthcare Medallion Pipeline/healthcare-pipeline-flow'
```

#### Option 3: Individual Layer Testing
```bash
# Test Bronze layer
python -c "from pipeline.bronze.bronze_layer import BronzeLayer; from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('test').getOrCreate(); bronze = BronzeLayer(spark); bronze.run_bronze_ingestion('data/patients.csv')"

# Test Silver layer
python -c "from pipeline.silver.silver_layer import SilverLayer; from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('test').getOrCreate(); silver = SilverLayer(spark); silver.process_silver_layer()"

# Test Gold layer
python -c "from pipeline.gold.gold_layer import GoldLayer; from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('test').getOrCreate(); gold = GoldLayer(spark); gold.process_gold_layer()"
```

## 📁 Project Structure

```
healthcare-pipeline/
├── data/
│   └── patients.csv                 # Sample input data
├── output/
│   ├── bronze/                      # Raw data storage
│   ├── silver/                      # Cleaned data storage
│   └── gold/                        # Aggregated data storage
├── pipeline/
│   ├── bronze/
│   │   └── bronze_layer.py          # Bronze layer implementation
│   ├── silver/
│   │   └── silver_layer.py          # Silver layer implementation
│   ├── gold/
│   │   └── gold_layer.py            # Gold layer implementation
│   ├── dags/
│   │   └── healthcare_pipeline.py   # Prefect DAG
│   ├── tests/
│   │   ├── test_bronze_layer.py     # Bronze layer tests
│   │   ├── test_silver_layer.py     # Silver layer tests
│   │   ├── test_gold_layer.py       # Gold layer tests
│   │   └── test_integration.py      # Integration tests
│   └── main.py                      # Main pipeline orchestrator
├── requirements.txt                  # Python dependencies
├── README.md                        # This file
└── .github/
    └── workflows/
        └── ci.yml                   # GitHub Actions CI/CD
```

## 🧪 Testing

### Run All Tests
```bash
# Run unit tests
pytest pipeline/tests/ -v

# Run with coverage
pytest pipeline/tests/ --cov=pipeline --cov-report=html

# Run specific test file
pytest pipeline/tests/test_bronze_layer.py -v

# Run integration tests
pytest pipeline/tests/test_integration.py -v
```

### Test Categories

1. **Unit Tests**: Test individual layer functionality
2. **Integration Tests**: Test end-to-end pipeline flow
3. **Performance Tests**: Validate optimization features

## 📊 Sample Output

### Pipeline Results
```
==================================================
PIPELINE RESULTS & INSIGHTS
==================================================
Top Diagnosis: CARDIAC
Top Billing Amount: $2,100.00
Total Revenue: $3,750.00

Patient Distribution by Diagnosis:
  DIABETES: 4 patients
  CARDIAC: 4 patients
  HYPERTENSION: 2 patients
==================================================
```

### Gold Layer Data Structure
```python
{
    "diagnosis": "DIABETES",
    "total_billing": 1250.0,
    "avg_billing": 312.5,
    "patient_count": 4,
    "min_age": 30,
    "max_age": 60,
    "avg_age": 45.0,
    "first_visit": "2024-01-01",
    "last_visit": "2024-01-08",
    "billing_rank": 2
}
```

## ⚙️ Configuration

### Pipeline Configuration
```python
# In pipeline/main.py
csv_path = "data/patients.csv"
last_run_date = None  # Set to "2024-01-01" for incremental loading
```

### Spark Configuration
```python
# Optimized Spark settings
config = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
}
```

### Prefect Configuration
```python
# Deployment settings
deploy_config = {
    "schedule": "0 2 * * *",  # Daily at 2 AM
    "retries": 3,
    "retry_delay_seconds": 60
}
```

## 🔧 Advanced Features

### Schema Evolution
```python
# Handle new columns automatically
df = spark.read.option("mergeSchema", "true").parquet("output/bronze")
```

### Incremental Processing
```python
# Only process new records
df_incremental = df.filter(col("visit_date") > last_run_date)
```

### Performance Optimization
```python
# Partitioning for performance
df.write.partitionBy("visit_date").parquet("output/silver")

# Broadcast joins for small datasets
df_large.join(broadcast(df_small), "patient_id")
```

### Error Handling
```python
try:
    df = spark.read.csv("data/patients.csv")
except Exception as e:
    logger.error(f"Error loading data: {e}")
    # Implement retry logic or alerting
```

## 🚨 Error Handling & Recovery

### Common Issues and Solutions

1. **Missing Source File**
   - **Symptom**: FileNotFoundError during bronze layer
   - **Solution**: Check file path and permissions
   - **Recovery**: Pipeline will fail gracefully with detailed error message

2. **Schema Mismatch**
   - **Symptom**: AnalysisException during data processing
   - **Solution**: Enable schema evolution with `mergeSchema=true`
   - **Recovery**: Pipeline adapts to new schema automatically

3. **Memory Issues**
   - **Symptom**: OutOfMemoryError
   - **Solution**: Increase executor memory or enable dynamic allocation
   - **Recovery**: Pipeline restarts with optimized settings

4. **Data Quality Issues**
   - **Symptom**: Unexpected null values or invalid data
   - **Solution**: Silver layer cleans and validates data
   - **Recovery**: Invalid records are logged and filtered

### Monitoring and Alerting
```python
# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Prefect monitoring
@task(retries=3, retry_delay_seconds=60)
def monitored_task():
    # Task implementation
    pass
```

## 📈 Performance Optimization

### Best Practices

1. **Partitioning Strategy**
   - Bronze: Partition by `visit_date`
   - Silver: Partition by `diagnosis`
   - Gold: No partitioning (small dataset)

2. **Caching**
   ```python
   # Cache frequently accessed DataFrames
   df.cache()
   ```

3. **Optimizations**
   ```python
   # Enable adaptive query execution
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   
   # Use Kryo serialization
   spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
   ```

## 🔄 CI/CD Pipeline

### GitHub Actions
```yaml
name: Healthcare Pipeline CI/CD
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: pytest pipeline/tests/ --cov=pipeline
```

## 🌐 Deployment Options

### Local Development
- Single-node Spark cluster
- File-based storage
- Manual execution

### Production
- Multi-node Spark cluster
- Cloud storage (S3/ADLS)
- Orchestrated execution (Prefect/Airflow)
- Monitoring and alerting

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review the test cases for usage examples
3. Check the logs for detailed error messages
4. Create an issue with:
   - Error description
   - Steps to reproduce
   - Environment details
   - Log files

## 📚 Additional Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Prefect Documentation](https://docs.prefect.io/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Medallion Architecture](https://www.databricks.com/blog/2020/01/30/the-lakehouse-medallion-architecture.html)

---

**Built with ❤️ for healthcare data engineering**
