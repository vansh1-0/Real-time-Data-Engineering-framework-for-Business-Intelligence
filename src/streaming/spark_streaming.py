"""
Spark Structured Streaming Consumer for Real-Time Market Data
Reads from Kafka topic 'market-prices' and processes stock data
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, avg, sum as sum_col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "market-prices"


def create_spark_session():
    """Create and return a SparkSession"""
    try:
        # Set Java options for compatibility with newer Java versions
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 pyspark-shell'
        
        # Windows workaround: Set HADOOP_HOME and disable native IO
        os.environ['HADOOP_HOME'] = 'C:\\hadoop'
        
        spark = SparkSession.builder \
            .appName("MarketDataProcessor") \
            .master("local[*]") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow -Djava.io.tmpdir=C:/temp") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow -Djava.io.tmpdir=C:/temp") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
            .config("spark.sql.streaming.checkpointLocation", "file:///C:/temp/spark-checkpoints") \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession created successfully: MarketDataProcessor")
        return spark
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {e}")
        raise


def define_schema():
    """Define the schema for market data"""
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True)
    ])
    logger.info("Schema defined for market data")
    return schema


def read_from_kafka(spark):
    """Read streaming data from Kafka topic"""
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        logger.info(f"Connected to Kafka topic: {KAFKA_TOPIC} at {KAFKA_BOOTSTRAP_SERVERS}")
        return df
    except Exception as e:
        logger.error(f"Failed to read from Kafka: {e}")
        raise


def process_stream(df, schema):
    """Parse and transform the streaming data"""
    try:
        # Convert binary value to string
        df = df.selectExpr("CAST(value AS STRING) as json_value")
        
        # Parse JSON data
        parsed_df = df.select(
            from_json(col("json_value"), schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp string to timestamp type
        parsed_df = parsed_df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )

        # Watermark and windowed aggregations for KPI metrics
        kpi_df = parsed_df.withWatermark("timestamp", "2 minutes") \
            .groupBy(
                col("symbol"),
                window(col("timestamp"), "5 minutes")
            ) \
            .agg(
                avg(col("close")).alias("avg_close"),
                sum_col(col("volume")).alias("total_volume")
            ) \
            .select(
                col("symbol"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("avg_close"),
                col("total_volume")
            )

        logger.info("KPI aggregation pipeline configured")
        return kpi_df
    except Exception as e:
        logger.error(f"Failed to process stream: {e}")
        raise


def write_to_console(df):
    """Write the streaming data to console for testing"""
    try:
        query = df.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 50) \
            .trigger(processingTime='10 seconds') \
            .start()

        logger.info("Streaming to console started")
        return query
    except Exception as e:
        logger.error(f"Failed to write stream: {e}")
        raise


def main():
    """Main execution function"""
    logger.info("=" * 70)
    logger.info("Starting Spark Structured Streaming - Market Data Processor")
    logger.info("=" * 70)
    
    try:
        # Initialize Spark Session
        spark = create_spark_session()
        
        # Define schema
        schema = define_schema()
        
        # Read from Kafka
        kafka_stream = read_from_kafka(spark)
        
        # Process the stream
        processed_stream = process_stream(kafka_stream, schema)
        
        # Write to console for testing
        query = write_to_console(processed_stream)
        
        logger.info("Streaming query is running. Press Ctrl+C to stop.")
        logger.info("Waiting for data from Kafka...")
        
        # Await termination
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Streaming job interrupted by user")
    except Exception as e:
        logger.error(f"Streaming job failed: {e}")
        raise
    finally:
        logger.info("Shutting down Spark Streaming job")


if __name__ == "__main__":
    main()
