# Ride Sharing Analytics Using Spark Structured Streaming & Spark SQL

This repository implements a real-time analytics pipeline for a ride‑sharing platform using **Apache Spark Structured Streaming**. It ingests simulated ride events from a socket, parses JSON into structured columns, performs **driver‑level aggregations**, and computes **time‑windowed analytics**. MLlib models are used for per‑ride fare prediction and time‑based fare trend estimation. Selected outputs and logs are saved for inspection and grading.

---

## Repository Structure

```
.
├── data_generator.py            # Streams JSON events over a TCP socket (0.0.0.0:9999)
├── models                       # Saved Spark MLlib models
│   ├── fare_model               # LinearRegressionModel for Task 4 (distance ➜ fare)
│   └── fare_trend_model_v2      # LinearRegressionModel for Task 5 (time-based trend)
├── outputs                      # Selected sample CSVs for grading/inspection
│   ├── task_1_samples           # Parsed rows (sample CSVs from Task 1)
│   ├── task_2_samples           # Driver-level aggregates (sample CSVs from Task 2)
│   ├── task_3_samples           # 5-min windowed sums (sample CSVs from Task 3)
│   ├── task_4_samples           # Per-ride predictions & deviations (sample rows from Task 4)
│   └── task_5_samples           # Time-windowed averages & predicted trend (sample rows from Task 5)
├── README.md                    # This file
├── requirements.txt             # Python dependencies (install with: pip install -r requirements.txt)
├── task1.py                     # Task 1: Ingestion + JSON parsing (prints to console + sample CSVs)
├── task2.py                     # Task 2: Driver-level aggregations (SUM fare, AVG distance)
├── task3.py                     # Task 3: 5-min windowed sums with 1-min slide + 1-min watermark
├── task4.py                     # Task 4: MLlib regression — per-ride fare prediction + deviation
├── task5.py                     # Task 5: MLlib regression — time-based avg fare trend (5-min windows)
└── training-dataset.csv         # Static training data used to fit the MLlib models
```

---

## Quickstart

### 1) Install dependencies
Use the provided **requirements.txt**:
```bash
pip install -r requirements.txt
```

### 2) Start the data generator (Terminal A)
```bash
python data_generator.py
```
This opens a TCP socket on `0.0.0.0:9999` and continually emits JSON events like:
```json
{"trip_id":"...","driver_id":54,"distance_km":25.25,"fare_amount":100.11,"timestamp":"2025-10-14 17:18:05"}
```

### 3) Run each task in its own terminal

#### Task 1 — Ingestion + Parsing
```bash
python task1.py
```
- Reads from the socket with `spark.readStream.format("socket")`.
- Parses JSON into columns: `trip_id, driver_id, distance_km, fare_amount, timestamp`.
- Prints to console and writes selected samples to `outputs/task_1_samples/`.

#### Task 2 — Driver-Level Aggregations
```bash
python task2.py
```
- Groups by `driver_id` and computes:
  - `SUM(fare_amount)` → `total_fare`
  - `AVG(distance_km)` → `avg_distance`
- Writes per-batch sample CSVs under `outputs/task_2_samples/`.

#### Task 3 — 5-Minute Windowed Sums (1-Minute Slide + Watermark)
```bash
python task3.py
```
- Converts `timestamp` → `event_time` (`TimestampType`), applies `withWatermark("event_time", "1 minute")`.
- Uses `window("event_time", "5 minutes", "1 minute")` and aggregates `SUM(fare_amount)`.
- Sorts results **inside** `foreachBatch` (static DF) and writes selected CSVs to `outputs/task_3_samples/`.
- Let it run ~6–7 minutes to produce non-empty window results.

#### Task 4 — Real-Time Fare Prediction (distance ➜ fare using MLlib)
```bash
python task4.py
# (optional) capture logs
python task4.py | tee logs/task4.out
```
- Trains a **LinearRegression** model one-time on `training-dataset.csv` using `distance_km` → `fare_amount`, saved to `models/fare_model/`.
- Streams live rides, assembles features, predicts `predicted_fare`, and computes **`deviation = |fare_amount - predicted_fare|`**.
- Prints to console; selected rows may be saved to `outputs/task_4_samples/`.

#### Task 5 — Time-Based Fare Trend Prediction (5-Minute Windows using MLlib)
```bash
python task5.py
# (optional) capture logs
python task5.py | tee logs/task5.out
```
- Trains a **LinearRegression** model on 5-minute **tumbling** windows of `training-dataset.csv` using time features (`hour_of_day`, `minute_of_hour`). Saved under `models/fare_trend_model_v2/`.
- Streaming: aggregates into the same 5-minute windows with a `1 minute` watermark, computes `actual_avg_fare` and **`predicted_next_avg_fare`** for that window.
- In **`update`** mode the same window may re-emit as late data arrives; **`append`** shows only finalized windows.

---

## Requirements → Implementation Mapping

| Requirement | Implementation |
|---|---|
| Ingest from socket (localhost:9999) | `spark.readStream.format("socket").option("host","localhost").option("port",9999)` |
| Parse JSON into columns | `from_json(col("value"), schema).alias("json").select("json.*")` |
| Print parsed data to console | `writeStream.format("console").outputMode("append")` (Task 1) |
| Driver-level real-time aggregations | `groupBy("driver_id")` + `sum(fare_amount)`, `avg(distance_km)` (Task 2) |
| Write aggregations to CSV | `foreachBatch` writing samples to `outputs/task_2_samples/` |
| Time-windowed analytics | `withWatermark("event_time","1 minute")`, `window("5 minutes","1 minute")` (Task 3) |
| Write windowed results to CSV | `foreachBatch` writing samples to `outputs/task_3_samples/` |
| **Task 4: Train & predict fares** | `VectorAssembler(["distance_km"])` + `LinearRegression` → `models/fare_model` → streaming predictions + `deviation` |
| **Task 5: Train time-based trend** | 5-min tumbling windows + time features (`hour_of_day`, `minute_of_hour`) → `models/fare_trend_model_v2` → `actual_avg_fare` vs. `predicted_next_avg_fare` |

---

## Sample Outputs

### Task 1 — Parsed Rows (examples)
```
d34e5277-8fd6-4067-8eec-5d63cd06535f,23,39.99,62.85,2025-10-14 17:20:57
55a0a604-202b-4ca8-9520-b938833fa867,49,25.43,8.72,2025-10-14 17:20:58
f669a3b1-834c-40cd-97ac-bcf82333ac8c,19,44.6,118.66,2025-10-14 17:20:56
```

### Task 2 — Driver Aggregations (snapshots)
_Columns: `driver_id,total_fare,avg_distance`_
```
65,77.11,26.55
78,164.29,23.65
81,215.8,27.56
...
```

### Task 3 — 5-Minute Windows (1-minute slide; watermark 1m)
```
window_start,window_end,sum_fare_amount
2025-10-14T17:22:00.000Z,2025-10-14T17:27:00.000Z,1626.91
2025-10-14T17:23:00.000Z,2025-10-14T17:28:00.000Z,5892.83
2025-10-14T17:29:00.000Z,2025-10-14T17:34:00.000Z,23518.80
```

### Task 4 — Per-Ride Fare Prediction + Deviation
_Columns: `trip_id,driver_id,distance_km,fare_amount,predicted_fare,deviation,timestamp`_
```
16c873cb-5e73-4adf-896e-a02702552673,15,27.0,72.5,51.06825011249654,21.431749887503457,2025-10-22 22:04:02
74d7b792-3460-48a9-936a-da6d64255127,91,14.59,49.05,28.746938929409502,20.303061070590495,2025-10-22 22:04:03
5d6ec177-230a-4434-b5f2-cf72fa766985,81,11.37,86.55,22.955269146207222,63.594730853792775,2025-10-22 22:04:04
506ce0a4-989e-41c9-83f1-5899a93b8f81,62,4.71,48.88,10.976225433124254,37.90377456687575,2025-10-22 22:04:05
9f9340da-49a4-4ed3-b404-639815a035b8,19,36.27,111.98,67.74178392935528,44.238216070644725,2025-10-22 22:04:06
```

### Task 5 — Time-Based Fare Trend (5-minute windows)
_Columns: `window_start,window_end,actual_avg_fare,predicted_next_avg_fare`_
```
2025-10-22 22:15:00,2025-10-22 22:20:00,79.0018181818182,48.88266302780869
2025-10-22 22:20:00,2025-10-22 22:25:00,80.50434782608696,48.70300203146776
2025-10-22 22:25:00,2025-10-22 22:30:00,76.59015384615385,48.523341035126826
2025-10-22 22:30:00,2025-10-22 22:35:00,83.46603448275863,48.3436800387859
```

---

## Troubleshooting

- **Empty Task 3 samples**: Many micro-batches won’t contain a *completed* 5-min window yet. Let Task 3 run ~6–7 minutes. Inside `foreachBatch`, you can skip writing empty batches by checking `batch_df.rdd.isEmpty()`.
- **Task 5 repeats the same window**: Expected in **`update`** mode as late data updates the aggregate. Switch to **`append`** to emit only after watermark finalization.
- **“Sorting not supported”**: Sort streaming results **inside `foreachBatch`** (the micro-batch is static) or drop the sort on the streaming DF.
- **Port forwarding (Codespaces)**: Ensure port **9999** is forwarded; the generator prints “New client connected” when a task attaches.
- **Spark UI port in use**: Spark auto-increments the UI port (4040 → 4041 → 4042). Informational only.
- **Rounding**: Spark’s floating-point prints can be long. If desired, wrap with `round(sum(...), 2)` and `round(avg(...), 2)`.

