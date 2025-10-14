from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = (
    SparkSession.builder
    .appName("ITCS6190-Task3-WindowedAnalytics")
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

# Convert to event time and apply watermark (1 minute) to control state size
df = df.withColumn("event_time", to_timestamp(col("timestamp"))).withWatermark("event_time", "1 minute")

# 5-minute window, sliding by 1 minute
win_agg = (
    df.groupBy(window(col("event_time"), "5 minutes", "1 minute"))
      .agg(_sum("fare_amount").alias("sum_fare_amount"))
      .select(
          col("window.start").alias("window_start"),
          col("window.end").alias("window_end"),
          col("sum_fare_amount")
      )
)

def write_batch(batch_df, batch_id: int):
    (
        batch_df
        .orderBy("window_start")              # OK here: batch_df is static
        .coalesce(1)
        .write
        .mode("append")
        .option("header", True)
        .csv(f"outputs/task_3/batch_{batch_id:06d}")
    )

query = (
    win_agg.writeStream
    .outputMode("append")  # append is correct with window + watermark
    .foreachBatch(write_batch)
    .option("checkpointLocation", "outputs/task_3/_checkpoints")
    .start()
)

query.awaitTermination()