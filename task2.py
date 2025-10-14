from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg as _avg, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Spark session
spark = (
    SparkSession.builder
    .appName("ITCS6190-Task2-DriverAggregations")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

raw = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

df = raw.select(from_json(col("value"), schema).alias("json")).select("json.*")

# Convert event time for watermarking (optional but good practice)
df = df.withColumn("event_time", to_timestamp(col("timestamp")))

agg = (
    df
    .groupBy("driver_id")
    .agg(
        _sum("fare_amount").alias("total_fare"),
        _avg("distance_km").alias("avg_distance"),
    )
)

# Use foreachBatch to write a single CSV with headers per micro-batch
def write_batch(batch_df, batch_id: int):
    (
        batch_df.coalesce(1)
        .write
        .mode("append")
        .option("header", True)
        .csv(f"outputs/task_2/batch_{batch_id:06d}")
    )

query = (
    agg.writeStream
    .outputMode("complete")  # complete mode needed for aggregations without watermark/window
    .foreachBatch(write_batch)
    .option("checkpointLocation", "outputs/task_2/_checkpoints")
    .start()
)

query.awaitTermination()