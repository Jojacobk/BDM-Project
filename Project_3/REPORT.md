# Project 3 Report — CDC + Orchestrated Lakehouse Pipeline

---

## 1. CDC Correctness

### Silver mirrors PostgreSQL

After running the DAG, the validation task compares `lakehouse.cdc.silver_customers` and
`lakehouse.cdc.silver_drivers` row counts against the live PostgreSQL source:

```
[validation] customers: PostgreSQL=<N>, Silver=<N> → ✓ PASS
[validation] drivers:   PostgreSQL=<N>, Silver=<N> → ✓ PASS
```

**TODO: paste validation task log output here before submission.**

### Spot-check

```
[validation] ✓ PASS — customer id=3 found in silver
[validation] ✓ PASS — customer id=7 found in silver
[validation] ✓ PASS — customer id=1 found in silver
```

**TODO: paste actual spot-check output here.**

### Deletes propagated to silver

```
[validation] ✓ PASS — no ghost rows in silver (deletes propagated correctly)
```

When `simulate.py` deletes a row from PostgreSQL, Debezium emits a `op='d'` event,
bronze captures it, and the silver MERGE executes a DELETE on the matching `id`.
Confirmed by querying silver immediately after a manual `DELETE FROM customers WHERE id=X`:
the row is absent from silver within the next DAG run.

**TODO: paste before/after row count screenshot here.**

### Idempotency

Running the DAG twice without any new PostgreSQL changes:
- `bronze_cdc`: `trigger(availableNow=True)` with the Kafka checkpoint replays nothing
  (the checkpoint pointer is already past all committed offsets).
- `silver_cdc`: `read_bronze_incremental()` reads from the saved snapshot ID; if no new
  bronze snapshots exist, the function returns immediately with no MERGE executed.
- Result: silver row count is identical before and after the second run.

```
[silver_cdc] No new bronze snapshots since last run. Nothing to do.
```

**TODO: paste second-run log output showing identical counts.**

---

## 2. Lakehouse Design

### Table schemas

**`lakehouse.cdc.bronze`** — raw CDC events, append-only

| Column | Type | Description |
|--------|------|-------------|
| kafka_topic | STRING | Debezium topic (`dbserver1.public.customers`) |
| kafka_partition | INT | Kafka partition |
| kafka_offset | LONG | Kafka offset |
| kafka_timestamp | TIMESTAMP | Kafka broker timestamp |
| op | STRING | Debezium operation: r=snapshot, c=insert, u=update, d=delete |
| ts_ms | LONG | Event timestamp (milliseconds since epoch) |
| source_table | STRING | PostgreSQL table name from `$.payload.source.table` |
| before_json | STRING | Row state before change (JSON string, null for c/r) |
| after_json | STRING | Row state after change (JSON string, null for deletes) |
| ingested_at | TIMESTAMP | When the script wrote this row |

**`lakehouse.cdc.silver_customers`** — current state mirror of `public.customers`

| Column | Type | Notes |
|--------|------|-------|
| id | INT (PK) | |
| name | STRING | |
| email | STRING | |
| country | STRING | |
| created_at | STRING | Debezium epoch micros serialised as string |

**`lakehouse.cdc.silver_drivers`** — current state mirror of `public.drivers`

| Column | Type | Notes |
|--------|------|-------|
| id | INT (PK) | |
| name | STRING | |
| license_number | STRING | |
| rating | STRING | DECIMAL(3,2) serialised as string by Debezium |
| city | STRING | |
| active | BOOLEAN | |
| created_at | STRING | Debezium epoch micros serialised as string |

**`lakehouse.taxi.bronze`** — raw Kafka taxi events (see Section 4)

**`lakehouse.taxi.silver`** — cleaned and enriched taxi trips (see Section 4)

**`lakehouse.taxi.gold`** — hourly aggregations per zone (see Section 4)

**`lakehouse.taxi.gold_congestion_impact`** — per-zone, per-hour congestion metrics (see Section 5)

**`lakehouse.taxi.gold_congestion_zones`** — daily congestion zone summary (see Section 5)

### Iceberg snapshot history — silver_customers

```
TODO: paste output of:
spark.sql("SELECT snapshot_id, committed_at, operation, summary
           FROM lakehouse.cdc.silver_customers.snapshots
           ORDER BY committed_at DESC LIMIT 10").show(truncate=False)
```

### Rolling back a bad MERGE with Iceberg time travel

If a MERGE introduces incorrect data (e.g. a bug in `silver_cdc.py` corrupts rows),
rollback is a two-step operation using Iceberg's time travel:

```python
# 1. Find the last good snapshot ID from the history above
good_snapshot_id = <snapshot_id from before the bad MERGE>

# 2. Roll the table back to that snapshot — metadata-only, no data rewrite
spark.sql(f"""
    CALL lakehouse.system.rollback_to_snapshot(
        'cdc.silver_customers',
        {good_snapshot_id}
    )
""")
```

This works because Iceberg's snapshot model never overwrites Parquet files —
every MERGE writes new files and records the previous state in the snapshot log.
Rolling back simply moves the `current-snapshot-id` pointer in the metadata.

---

## 3. Orchestration Design

### DAG graph

**TODO: paste screenshot of the Airflow graph view here (Admin → DAGs → project3_pipeline → Graph).**

### Task dependency chain

```
health_check
     ├── bronze_cdc  → silver_cdc  ─────────────────────────┐
     └── bronze_taxi → silver_taxi → gold_taxi               ├─ validation
                                   → gold_congestion ────────┘
```

Rationale for this order:
- `health_check` is first because there is no point ingesting CDC data if the
  Debezium connector is not running. A failed health check stops both paths immediately.
- `bronze_cdc` and `bronze_taxi` are independent and run in parallel — neither depends on the other's data.
- `silver_cdc` must wait for `bronze_cdc` because it reads from the bronze table.
- `silver_taxi` must wait for `bronze_taxi` for the same reason.
- `gold_taxi` and `gold_congestion` both read from `silver_taxi`, so they run in parallel
  after silver_taxi completes.
- `validation` is last — it only makes sense once all silver tables have been updated.
  It gates on `silver_cdc`, `gold_taxi`, and `gold_congestion` to confirm both paths completed.

### Scheduling strategy

Schedule: `*/15 * * * *` (every 15 minutes).

This schedule supports a **15-minute data freshness SLA**: the worst case is that a
PostgreSQL change happens immediately after a DAG run, and the next run picks it up
15 minutes later. For a city transportation authority monitoring congestion in
near-real-time, 15-minute lag is acceptable — traffic conditions are reported by NYC DOT
at 5-minute granularity, so 15 minutes is within operational tolerance.

`catchup=False` prevents Airflow from backfilling historical runs when the DAG is
first deployed or re-enabled after downtime, which would cause multiple concurrent
MERGE operations that could produce duplicate processing.

`max_active_runs=1` ensures that if a run takes longer than 15 minutes, the next
scheduled run waits rather than overlapping, preventing checkpoint conflicts.

### Retry and failure handling

Each task is configured with `retries=2, retry_delay=timedelta(minutes=1)`.

- **health_check failure**: retries once (30s delay), then fails the entire DAG.
  All downstream tasks are skipped via Airflow's default `ALL_SUCCESS` trigger rule.
- **bronze_cdc failure**: silver_cdc and validation are skipped (downstream dependency).
  bronze_taxi and its downstream continue independently.
- **MERGE (silver_cdc) failure**: validation is skipped. The silver table remains at its
  previous snapshot — no partial state is committed.
- **validation failure**: `retries=0` because a validation failure indicates a data
  quality issue, not a transient error. Auto-retry would just confirm the same failure.

**TODO: paste screenshot of one failed task and the Airflow task log showing the retry.**

### DAG run history

**TODO: paste screenshot of at least 3 consecutive successful DAG runs from the Airflow UI (Browse → DAG Runs).**

### Backfill

Because `catchup=False` is set, Airflow does not automatically backfill.
To manually backfill a specific date range (e.g. after fixing a bug):

```bash
airflow dags backfill project3_pipeline \
  --start-date 2026-01-01 \
  --end-date   2026-01-02
```

Each backfill run is idempotent: `trigger(availableNow=True)` with the checkpoint
pointer means no new Kafka data is re-read for already-processed intervals, and the
MERGE INTO operation produces the same silver state regardless of how many times
it runs for a given interval.

---

## 4. Streaming Pipeline (Taxi)

**TODO: implement in `notebooks/02_taxi_pipeline.ipynb` and fill in this section.**

Improvements over Project 2:
- Now triggered by Airflow instead of running as a standalone streaming job.
- TODO: list specific improvements based on Project 2 feedback.

Bronze schema: TODO
Silver schema: TODO
Gold schema: TODO

Row counts at each layer: TODO
Idempotency test: TODO

---

## 5. Custom Scenario — Congestion Impact Analysis

**TODO: implement `gold_congestion.py` and fill in this section.**

### gold_congestion_impact schema

| Column | Type | Description |
|--------|------|-------------|
| pickup_zone_id | INT | PULocationID |
| pickup_zone_name | STRING | Zone name from lookup |
| hour_of_day | INT | 0–23 |
| trip_date | DATE | Partition column |
| avg_speed_mph | DOUBLE | distance / duration in hours |
| avg_congestion_surcharge | DOUBLE | avg congestion_surcharge |
| trips_with_surcharge | LONG | count where congestion_surcharge > 0 |
| trips_without_surcharge | LONG | count where congestion_surcharge = 0 |
| total_congestion_revenue | DOUBLE | sum of congestion_surcharge |
| avg_fare_per_mile | DOUBLE | fare_amount / trip_distance |

### gold_congestion_zones schema

| Column | Type | Description |
|--------|------|-------------|
| report_date | DATE | Daily summary date |
| most_congested_zones | STRING | JSON array: top 5 zones by lowest avg speed |
| top_revenue_zones | STRING | JSON array: top 5 zones by total congestion revenue |
| speed_profile_top3 | STRING | JSON: hour-by-hour speed for top 3 congested zones |

### Query results

**Rush hour (8–9 AM) slowest zone:**
```
TODO: paste query output
```

**Total congestion surcharge revenue per day:**
```
TODO: paste query output
```

---

## 6. Bonus — Schema Evolution

Not implemented.

---

## 7. Challenges and Design Decisions

### Why `F.col("offset")` instead of `raw_stream.offset`

`offset` is a Python built-in function. Accessing `raw_stream.offset` resolves to
the Python built-in rather than the DataFrame column, causing:
`AttributeError: 'function' object has no attribute 'alias'`
Fix: always use `F.col("offset").alias("kafka_offset")`.

### Why local checkpoints instead of `s3a://`

Spark's streaming checkpoint mechanism uses the Hadoop FileSystem API.
The `s3a://` scheme requires the `hadoop-aws` JAR to be present in the Spark classpath.
While the Iceberg S3FileIO handles `s3://` for table data, checkpoint writes go through
a different code path that does not use Iceberg's S3 client. Using a local path
(`/home/jovyan/checkpoints/`) avoids this dependency entirely.

### Why `trigger(availableNow=True)` for Airflow integration

Streaming jobs run indefinitely by default, which is incompatible with Airflow's task
model (which expects tasks to terminate). `trigger(availableNow=True)` processes all
Kafka data that has arrived since the last checkpoint, then terminates cleanly.
This makes each Airflow task finite and retriable.

### Why snapshot mode `initial`

`seed.py` inserts rows into PostgreSQL before the Debezium connector is registered.
Using `initial` mode ensures these existing rows are captured as `op='r'` snapshot
events before Debezium switches to live WAL streaming. Using `never` would miss
all pre-existing data.

### Why plain DATE partitioning instead of `days()` transform

Spark 4.x streaming in append mode does not support Iceberg partition transforms
(`days()`, `hours()`, etc.) — they cause a runtime error. Using an explicit DATE
column and `PARTITIONED BY (trip_date)` achieves the same partition pruning without
the incompatibility.

---

## 8. Credentials (.env values)

```
POSTGRES_USER=cdc_user
POSTGRES_PASSWORD=<your value>
POSTGRES_DB=sourcedb
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=<your value>
JUPYTER_TOKEN=<your value>
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=<your value>
```

**Note:** actual secret values are provided separately to the grader as instructed.
