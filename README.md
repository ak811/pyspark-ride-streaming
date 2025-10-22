# Ride Sharing Analytics Using Spark Streaming and Spark SQL

## Overview
This repository implements a real-time analytics pipeline for a ride‑sharing platform using **Apache Spark Structured Streaming**. It ingests simulated ride events from a socket, parses JSON into structured columns, performs **driver‑level aggregations**, and computes **time‑windowed analytics**. Outputs are written to CSV for inspection and grading.

---

## Repository Structure

```
.
├── data_generator.py            # Streams JSON events over a TCP socket (localhost:9999)
├── task1.py                     # Task 1: Ingestion + parsing (prints to console + CSV)
├── task2.py                     # Task 2: Driver-level aggregations (SUM fare, AVG distance)
├── task3.py                     # Task 3: 5-min windowed sums with 1-min slide + watermark
├── outputs/
│   ├── task_1/                  # Raw parsed rows (CSV, many small part files)
│   ├── task_2/                  # Aggregates written per micro-batch (CSV per batch_NNNNNN/)
│   ├── task_3/                  # Windowed results per micro-batch (CSV per batch_NNNNNN/)
│   ├── task_1_samples/          # Selected sample CSVs for Task 1 (3 files)
│   ├── task_2_samples/          # Selected sample CSVs for Task 2 (3 files)
│   └── task_3_samples/          # Selected sample CSVs for Task 3 (3 files)
└── README.md                    # (This file)
```

---

## How to Run (Codespaces or Local)

1) **Install dependencies**
```bash
pip install pyspark faker
```

2) **Start the data generator** (Terminal A)
```bash
python data_generator.py
```
This opens `0.0.0.0:9999` and continually emits JSON events like:
```json
{"trip_id":"...","driver_id":54,"distance_km":25.25,"fare_amount":100.11,"timestamp":"2025-10-14 17:18:05"}
```

3) **Run the tasks** (each in its own terminal)

**Task 1**
```bash
python task1.py
```
- Reads from the socket with `spark.readStream.format("socket")`.
- Parses JSON into columns: `trip_id, driver_id, distance_km, fare_amount, timestamp`.
- Prints to console and writes CSVs to `outputs/task_1/`.

**Task 2**
```bash
python task2.py
```
- Reuses parsed fields, groups by `driver_id`.
- Computes:
  - `SUM(fare_amount)` → `total_fare`
  - `AVG(distance_km)` → `avg_distance`
- Writes one CSV per micro-batch under `outputs/task_2/batch_*/`.

**Task 3**
```bash
python task3.py
```
- Converts `timestamp` → `event_time` (TimestampType), applies `withWatermark("event_time","1 minute")`.
- 5‑minute **window** with **1‑minute slide**; aggregates `SUM(fare_amount)`.
- Sorts results **inside** `foreachBatch` (static DF) and writes to `outputs/task_3/batch_*/`.
- Let it run ~6–7 minutes (≈35+ micro-batches) to produce non‑empty window results.

---

## Requirements → Implementation Mapping

| Requirement | Implementation |
|---|---|
| Ingest from socket (localhost:9999) | `spark.readStream.format("socket").option("host","localhost").option("port",9999)` |
| Parse JSON into columns | `from_json(col("value"), schema).alias("json").select("json.*")` |
| Print parsed data to console | `writeStream.format("console").outputMode("append")` (Task 1) |
| Driver-level real-time aggregations | `groupBy("driver_id")` + `sum(fare_amount)`, `avg(distance_km)` (Task 2) |
| Write aggregations to CSV | `foreachBatch` writing `outputs/task_2/batch_*/` |
| Time-windowed analytics | `withWatermark("event_time","1 minute")`, `window("5 minutes","1 minute")` (Task 3) |
| Write windowed results to CSV | `foreachBatch` to `outputs/task_3/batch_*/` |

---

## Sample Results (from actual run)

### Task 1 — Parsed Rows (3 examples)
```
d34e5277-8fd6-4067-8eec-5d63cd06535f,23,39.99,62.85,2025-10-14 17:20:57
55a0a604-202b-4ca8-9520-b938833fa867,49,25.43,8.72,2025-10-14 17:20:58
f669a3b1-834c-40cd-97ac-bcf82333ac8c,19,44.6,118.66,2025-10-14 17:20:56
```

### Task 2 — Driver Aggregations (3 snapshots)
_Example excerpt (columns: `driver_id,total_fare,avg_distance`):_
```
65,77.11,26.55
78,164.29,23.65
81,215.8,27.56
...
```
```
65,16.48,4.31
78,164.29,23.65
81,192.07,21.83
...
```
```
65,77.11,26.55
78,164.29,23.65
81,192.07,21.83
...
```

> _Note:_ Long floats are normal from Spark. If desired, round in code with `round(sum(...), 2)` and `round(avg(...), 2)`.

### Task 3 — 5‑Minute Windows (1‑minute slide; watermark 1m)
```
window_start,window_end,sum_fare_amount
2025-10-14T17:22:00.000Z,2025-10-14T17:27:00.000Z,1626.91
2025-10-14T17:23:00.000Z,2025-10-14T17:28:00.000Z,5892.83
2025-10-14T17:29:00.000Z,2025-10-14T17:34:00.000Z,23518.80
```

---

## Troubleshooting

- **Empty Task 3 files:** Many micro-batches won’t contain a *completed* 5‑min window yet. Let Task 3 run ~6–7 minutes. You can also skip writing empty batches in `foreachBatch` by checking `batch_df.rdd.isEmpty()`.  
- **“Sorting not supported” error:** Sort streaming results **inside `foreachBatch`** (the micro‑batch is static) or drop the sort.  
- **Port forwarding in Codespaces:** Ensure port **9999** is forwarded; the generator prints a “New client connected” message when Task 1/2/3 attaches.  
- **Spark UI port in use:** Spark will auto-increment the UI port (4040 → 4041 → 4042). This is informational only.

