from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Spark session
spark = (
    SparkSession.builder
    .appName("ITCS6190-Task1-StreamingIngestion")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Schema for the incoming JSON
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

# Read from socket (netcat-like stream)
raw = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# Each line is a JSON object
parsed = raw.select(from_json(col("value"), schema).alias("json")).select("json.*")

# Print to console (required by Task 1)
console_query = (
    parsed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

# Also persist parsed rows to CSV for grading convenience
csv_query = (
    parsed.writeStream
    .format("csv")
    .outputMode("append")
    .option("path", "outputs/task_1")
    .option("checkpointLocation", "outputs/task_1/_checkpoints")
    .option("escape", "\\")
    .start()
)

spark.streams.awaitAnyTermination()