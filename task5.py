# /home/data/akhalegh/utils/spark-structured-streaming/task5.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg as _avg,
    hour, minute
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
import os

APP_NAME = "ITCS6190-Task5-RT-FareTrendPrediction"
TRAIN_PATH = "training-dataset.csv"
MODEL_PATH = "models/fare_trend_model_v2"     # LR model trained on time features

# Spark
spark = (
    SparkSession.builder
    .appName(APP_NAME)
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

# ----------------------------
# (A) Train once (if needed)
# ----------------------------
if not os.path.exists(MODEL_PATH):
    print(f"[Task5] Training trend model from {TRAIN_PATH} ...")

    base = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(TRAIN_PATH)
        .na.drop(subset=["fare_amount", "timestamp"])
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    # Tumbling 5-minute windows on static data
    w = (
        base.groupBy(window(col("event_time"), "5 minutes"))
            .agg(_avg("fare_amount").alias("avg_fare"))
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("avg_fare")
            )
    )

    # Feature engineering: hour_of_day, minute_of_hour from window_start
    with_feats = (
        w.withColumn("hour_of_day", hour(col("window_start")))
         .withColumn("minute_of_hour", minute(col("window_start")))
         .na.drop(subset=["avg_fare"])
    )

    assembler = VectorAssembler(
        inputCols=["hour_of_day", "minute_of_hour"],
        outputCol="features"
    )
    train_vec = assembler.transform(with_feats).withColumnRenamed("avg_fare", "label")

    lr = LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction")
    model = lr.fit(train_vec)

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    model.save(MODEL_PATH)
    print(f"[Task5] Trend model saved -> {MODEL_PATH}")
else:
    print(f"[Task5] Using existing model at {MODEL_PATH}")

# ----------------------------
# (B) Streaming Inference
# ----------------------------
model = LinearRegressionModel.load(MODEL_PATH)
assembler = VectorAssembler(
    inputCols=["hour_of_day", "minute_of_hour"],
    outputCol="features"
)

# Socket source
raw = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

df = raw.select(from_json(col("value"), schema).alias("json")).select("json.*")
df = df.withColumn("event_time", to_timestamp(col("timestamp"))).withWatermark("event_time", "1 minute")

# Tumbling 5-minute windows in streaming
win = (
    df.groupBy(window(col("event_time"), "5 minutes"))
      .agg(_avg("fare_amount").alias("avg_fare"))
      .select(
          col("window.start").alias("window_start"),
          col("window.end").alias("window_end"),
          col("avg_fare")
      )
)

# Features for current window
with_feats = (
    win.withColumn("hour_of_day", hour(col("window_start")))
       .withColumn("minute_of_hour", minute(col("window_start")))
)

scored = assembler.transform(with_feats)
scored = (
    model.transform(scored)
         .select(
             "window_start",
             "window_end",
             col("avg_fare").alias("actual_avg_fare"),
             col("prediction").alias("predicted_next_avg_fare")
         )
)

# Print windowed actual vs. predicted to console
query = (
    scored.writeStream
    .format("console")
    .outputMode("append")   # append works with window+watermark when windows close
    .option("truncate", False)
    .start()
)

query.awaitTermination()
