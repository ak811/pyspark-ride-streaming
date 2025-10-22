# /home/data/akhalegh/utils/spark-structured-streaming/task4.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, abs as _abs
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
import os

APP_NAME = "ITCS6190-Task4-RT-FarePrediction"
TRAIN_PATH = "training-dataset.csv"          # CSV with columns matching the schema below
MODEL_PATH = "models/fare_model"             # Saved LinearRegressionModel path

# Spark
spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Shared schema for both training and streaming
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
    print(f"[Task4] Training model from {TRAIN_PATH} ...")
    train_df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(TRAIN_PATH)
        .select("distance_km", "fare_amount")   # keep only needed columns
        .na.drop()
    )

    assembler = VectorAssembler(inputCols=["distance_km"], outputCol="features")
    train_vec = assembler.transform(train_df).withColumnRenamed("fare_amount", "label")

    lr = LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction")
    model = lr.fit(train_vec)

    # Ensure parent folders exist locally
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    model.save(MODEL_PATH)
    print(f"[Task4] Model saved -> {MODEL_PATH}")
else:
    print(f"[Task4] Using existing model at {MODEL_PATH}")

# ----------------------------
# (B) Streaming Inference
# ----------------------------
# Load the saved model
model = LinearRegressionModel.load(MODEL_PATH)
assembler = VectorAssembler(inputCols=["distance_km"], outputCol="features")

# Socket source
raw = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# Parse JSON -> columns
df = raw.select(from_json(col("value"), schema).alias("json")).select("json.*")

# Optional event time casting (useful for future extensions / watermarking)
df = df.withColumn("event_time", to_timestamp(col("timestamp")))

# Build features and predict
feat_df = assembler.transform(df)
pred_df = model.transform(feat_df)

# Compute absolute deviation between actual fare and predicted fare
scored = (
    pred_df
    .withColumn("deviation", _abs(col("fare_amount") - col("prediction")))
    .select(
        "trip_id", "driver_id", "distance_km", "fare_amount",
        col("prediction").alias("predicted_fare"),
        "deviation", "timestamp"
    )
)

# Print to console
query = (
    scored.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
